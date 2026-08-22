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
	"syscall"

	tsnet "tailscale.com/tsnet"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	"github.com/maleolabs/walkie/internal/store"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

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

	coord := coordinator.NewServer(resolver, clock.Real(), logger)
	if err := coord.Serve(ctx, ln); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}

// config is the process's entire configuration surface; see the package
// comment for the environment variables and their defaults.
type config struct {
	authKey     string
	hostname    string
	stateDir    string
	storePath   string
	controlPort string
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

	return &config{
		authKey:     os.Getenv("WALKIE_TAILNET_AUTHKEY"),
		hostname:    envOr("WALKIE_HOSTNAME", "walkie-coordinator"),
		stateDir:    stateDir,
		storePath:   storePath,
		controlPort: port,
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
