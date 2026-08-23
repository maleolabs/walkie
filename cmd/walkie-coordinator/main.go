// Command walkie-coordinator is walkie's coordination server.
//
// Owning work item:
//
//	eka get walkie/ts:coordinator-skeleton
//
// It joins the tailnet as its own node via tsnet (criterion 1), accepts one
// WebSocket per client, authenticates each connection by resolving the
// caller's tailnet identity from the connection's remote address (criterion 2,
// adr:004-security-model), and persists to a pure-Go SQLite store (criteria 4
// and 5). It holds no credentials and no user table — authentication is a
// lookup, not a subsystem. See internal/coordinator and
// fnd:tailnet-capability-baseline.
//
// # Configuration
//
// Everything comes from the environment so the same binary runs unmodified in
// a container, under systemd, or by hand:
//
//	WALKIE_TAILNET_AUTHKEY  tsnet auth key joining the tailnet (empty is fine
//	                        when the state directory already holds a joined
//	                        node)
//	WALKIE_HOSTNAME         this node's hostname inside the tailnet
//	                        (default "walkie-coordinator")
//	WALKIE_STATE_DIR        tsnet state directory (required; holds node keys
//	                        and must persist across restarts)
//	WALKIE_STORE_PATH       SQLite database path
//	                        (default "<WALKIE_STATE_DIR>/coordinator.db")
//	WALKIE_CONTROL_PORT     control-plane port (default "443")
//	WALKIE_PRESENCE_TTL     liveness TTL for presence (default "45s")
//	WALKIE_QUEUE_TTL        offline-queue retention per message (default "72h")
//	WALKIE_QUEUE_MAX_SIZE   offline-queue cap, messages per recipient
//	                        (default "256")
//
// The auth key arrives via environment rather than flag deliberately: argv is
// world-readable through ps and shell history, and this process has no other
// secret-shaped input. There is no credential anywhere else — adding a second
// one would be a design regression (adr:004).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	tsnet "tailscale.com/tsnet"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator"
	"github.com/maleolabs/walkie/internal/coordinator/presence"
	"github.com/maleolabs/walkie/internal/coordinator/queue"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	"github.com/maleolabs/walkie/internal/crypto"
	"github.com/maleolabs/walkie/internal/store"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

// defaultPresenceTTL backs WALKIE_PRESENCE_TTL; see loadConfig for the
// reasoning and why it is provisional.
const defaultPresenceTTL = 45 * time.Second

// Offline-queue retention defaults (sto:offline-queue), both provisional and
// env-overridable like the presence TTL. 72h covers a long weekend offline;
// 256 messages per recipient bounds one chatty peer without making the cap a
// daily event at human messaging rates. Both are bounded-retention knobs, not
// performance tuning — req:offline-delivery requires that they EXIST.
const (
	defaultQueueTTL     = 72 * time.Hour
	defaultQueueMaxSize = 256
)

func main() {
	showVersion := flag.Bool("version", false, "print version, then exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("walkie-coordinator %s %s/%s\n", version, runtime.GOOS, runtime.GOARCH)
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("walkie-coordinator: fatal", slog.String("reason", err.Error()))
		os.Exit(1)
	}
}

// run wires the whole coordinator and blocks until shutdown is signalled.
//
// Startup order matters and is not interchangeable: the store opens BEFORE
// the tailnet join, so a misconfigured database fails fast and locally
// instead of after a network round trip; the listener comes from tsnet
// itself, which is what makes the tailnet-only bind (criterion 3) structural
// rather than a matter of getting an address string right.
func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Criterion 5: first start creates and migrates the schema with no
	// manual step; later starts apply only new migrations. All timestamps
	// the store writes come from the injected clock.
	st, err := store.Open(cfg.storePath, clock.Real())
	if err != nil {
		return fmt.Errorf("open store %s: %w", cfg.storePath, err)
	}
	defer st.Close()

	// Presence (sto:device-presence): the tracker derives liveness from what
	// the coordinator observes — connections, heartbeats, their endings, and
	// the TTL. It is backed by the same store, so last-seen facts and status
	// labels survive a restart while nothing online does: after startup every
	// device is offline until observed alive again. Registered AFTER the
	// store's defer so Close order runs tracker-then-store.
	tracker, err := presence.NewTracker(st, clock.Real(), cfg.presenceTTL, logger)
	if err != nil {
		return fmt.Errorf("start presence tracker: %w", err)
	}
	defer tracker.Close()

	// The TOFU keystore (ts:queue-sealed-box): pins each device's X25519
	// public key on first announce, serves the table as PublicKeyDirectory
	// snapshots, and REFUSES a changed key for a known peer — nil confirmer,
	// because this process is headless and assumed consent is not consent.
	// Recovery from a refused change is manual by design: verify fingerprints
	// out of band, then correct the pin file by hand with the process
	// stopped. Opened BEFORE the offline queue, because the queue's sealing
	// seam looks recipient pins up at enqueue time.
	keys, err := crypto.OpenKeystore(filepath.Join(cfg.stateDir, "peer-keys.json"), clock.Real(), logger)
	if err != nil {
		return fmt.Errorf("open peer key keystore: %w", err)
	}
	defer keys.Close()

	// The offline queue (sto:offline-queue), SEALED (ts:queue-sealed-box):
	// bounded retention over the same store, wired as the server's
	// OfflineSink through queue.NewSealed. From here on, a message for an
	// offline device is HELD ENCRYPTED whenever the recipient's key is
	// pinned — sealed to that pinned public key with X25519 +
	// XChaCha20-Poly1305 (adr:004), stored as ciphertext this process cannot
	// read, and replayed as opaque SealedDelivery frames the recipient opens
	// with its own identity key. A recipient that has not announced a key yet
	// has no pin to seal to: its messages rest plaintext under the documented
	// bootstrap rule (queue.AtRest), logged per hold, never dropped. Close
	// order stays tracker-then-queue-then-store.
	inbox, err := queue.NewSealed(st, clock.Real(), cfg.queueTTL, cfg.queueMaxSize, logger, queue.NewKeystoreSealer(keys, logger))
	if err != nil {
		return fmt.Errorf("start offline queue: %w", err)
	}
	defer inbox.Close()

	// Criterion 1, production path: this process IS a tailnet node. tsnet
	// brings its own WireGuard, DERP fallback and node identity inside the
	// container — no host networking, no Tailscale sidecar (adr:001). The
	// auth key is passed to tsnet directly and is never logged.
	srv := &tsnet.Server{
		Hostname: cfg.hostname,
		Dir:      cfg.stateDir,
		AuthKey:  cfg.authKey,
	}
	defer srv.Close()

	logger.Info("joining tailnet",
		slog.String("hostname", cfg.hostname),
		slog.String("state_dir", cfg.stateDir),
	)

	// Criterion 2, production path: WhoIs-backed resolution against this
	// node's local daemon. Constructing the resolver starts the join, so a
	// bad auth key or unreachable coordination server surfaces HERE, with
	// the knobs that fix it named in the wrapper below.
	resolver, err := tsauth.NewTSNetResolver(srv)
	if err != nil {
		return fmt.Errorf("join tailnet (check WALKIE_TAILNET_AUTHKEY and WALKIE_STATE_DIR): %w", err)
	}

	// Criterion 3, production path: tsnet's listener binds ONLY the node's
	// tailnet IP — there is no code path here that could bind a wildcard,
	// because no address string is ever parsed or constructed locally. The
	// property is proven by test in internal/coordinator (server_test.go);
	// this wiring is where the tested shape meets production.
	ln, err := srv.Listen("tcp", ":"+cfg.controlPort)
	if err != nil {
		return fmt.Errorf("listen on tailnet interface :%s: %w", cfg.controlPort, err)
	}
	logger.Info("control plane listening", slog.String("addr", ln.Addr().String()))

	// Shutdown: SIGINT/SIGTERM cancel ctx, which stops the accept loop and
	// winds live connections down inside the server's grace window. SIGTERM
	// matters as much as SIGINT: container runtimes stop processes with
	// TERM, and missing it would turn every deploy into a hard kill.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The queue is the OfflineSink: holds for offline devices, drains on
	// Hello with the client's last_acked_position, refuses at the size cap
	// (see OfflineSink and QueueDrain in internal/coordinator/routing.go).
	// WireKeys attaches the TOFU pin store before Serve — key distribution
	// and its refusal gate live from this process's first connection.
	coord := coordinator.NewServer(resolver, clock.Real(), logger, tracker, inbox)
	coord.WireKeys(keys, nil)
	if err := coord.Serve(ctx, ln); err != nil {
		// The drain can genuinely fail: a peer that ignores close frames
		// outlives the grace window. Saying "shutdown complete" then would
		// be false — tsnet and the store are about to close under live
		// connections — so report what actually happened and still exit
		// clean: SIGTERM was handled, and a supervisor should restart us
		// normally rather than treat this as a crash.
		if errors.Is(err, coordinator.ErrDrainIncomplete) {
			logger.Warn("shutdown incomplete: connections outlived grace window",
				slog.String("reason", err.Error()))
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}

// config is the process's entire configuration surface; see the package
// comment for the environment variables and their defaults.
type config struct {
	authKey      string
	hostname     string
	stateDir     string
	storePath    string
	controlPort  string
	presenceTTL  time.Duration
	queueTTL     time.Duration
	queueMaxSize int
}

// loadConfig reads the environment into [config].
//
// WALKIE_STATE_DIR is required rather than defaulted: it holds the node's
// long-lived tailnet keys, and a silently-chosen location for stateful data
// is how backups get missed and containers lose identity across restarts.
func loadConfig() (*config, error) {
	stateDir := os.Getenv("WALKIE_STATE_DIR")
	if stateDir == "" {
		return nil, errors.New("WALKIE_STATE_DIR is required (tsnet state directory; must persist across restarts)")
	}

	storePath := os.Getenv("WALKIE_STORE_PATH")
	if storePath == "" {
		storePath = filepath.Join(stateDir, "coordinator.db")
	}

	port := os.Getenv("WALKIE_CONTROL_PORT")
	if port == "" {
		// 443 is the conventional single-service port and needs no
		// privilege on a tailscale TUN interface, which the coordinator
		// owns exclusively. Override per deployment via the environment.
		port = "443"
	}

	// The liveness TTL: how long a silent device stays online after its last
	// observed heartbeat. 45s is a provisional default — generous against
	// jittery tailnet links, tight enough that criterion 3's "offline within
	// the TTL" stays honest for humans watching a roster. The heartbeat
	// cadence it must relate to lands with the client work items; revisit
	// then rather than tuning blind.
	ttl := defaultPresenceTTL
	if v := os.Getenv("WALKIE_PRESENCE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse WALKIE_PRESENCE_TTL %q: %w", v, err)
		}
		ttl = d
	}

	// Offline-queue retention knobs (sto:offline-queue). Same provisional
	// posture as the presence TTL: bounded by construction, tuned by
	// operators, never unbounded by omission.
	queueTTL := defaultQueueTTL
	if v := os.Getenv("WALKIE_QUEUE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse WALKIE_QUEUE_TTL %q: %w", v, err)
		}
		queueTTL = d
	}
	queueMaxSize := defaultQueueMaxSize
	if v := os.Getenv("WALKIE_QUEUE_MAX_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("parse WALKIE_QUEUE_MAX_SIZE %q: want a positive integer", v)
		}
		queueMaxSize = n
	}

	return &config{
		authKey:      os.Getenv("WALKIE_TAILNET_AUTHKEY"),
		hostname:     envOr("WALKIE_HOSTNAME", "walkie-coordinator"),
		stateDir:     stateDir,
		storePath:    storePath,
		controlPort:  port,
		presenceTTL:  ttl,
		queueTTL:     queueTTL,
		queueMaxSize: queueMaxSize,
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
