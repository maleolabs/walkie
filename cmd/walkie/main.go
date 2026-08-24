// Command walkie is the terminal client.
//
// Owning work item:
//
//	eka get walkie/sto:terminal-ui
//
// Running `walkie` with no arguments starts the interactive client: it opens
// local state under ~/.walkie (identity key, message history, offline outbox —
// all created on first run, nothing to hand-edit), connects to the coordinator
// over the host's Tailscale, and brings up the terminal interface. vis:
// terminal-mesh-comms principle 4 makes that path a design constraint here: a
// working session in no more than three commands, for a reader who is not
// technical.
//
// # Why the client dials through the HOST's tailscale, not tsnet
//
// The coordinator joins the tailnet via tsnet because it is a container with
// no host daemon. A user device is ALREADY a tailnet member — that is the
// population this binary serves — so the client dials the coordinator's
// MagicDNS name through the ordinary host stack and lets tailscaled route it
// over the tunnel. The coordinator's WhoIs then resolves the connection to the
// HOST's identity: no auth key, no node pairing, no secret anywhere on the
// client (adr:004 puts no credentials in walkie; fnd:tailnet-capability-
// baseline records what the tailnet already provides). A tsnet join would
// demand a per-device auth key — an interactive prompt or environment variable
// a non-technical reader must discover — which is precisely the failure mode
// criterion 1 forbids.
//
// # Configuration surface (deliberately tiny)
//
// Every flag has a sane default; none is required; there is no configuration
// file. -coordinator exists for deployments whose coordinator does not run
// under its default hostname.
//
//	-coordinator  control-plane WebSocket URL
//	              (default "ws://walkie-coordinator:443" — MagicDNS resolves
//	              the coordinator's default hostname; port 443 is its default;
//	              plain ws because WireGuard encrypts the tunnel and adr:004
//	              forbids stacking TLS inside it)
//	-db           history database (default ~/.walkie/history.db — the SAME
//	              path `walkie history` reads)
//	-identity     X25519 identity key (default ~/.walkie/identity.key,
//	              generated owner-only on first run; losing it forfeits the
//	              messages queued for this device — see README)
//	-socket       local control socket (default ~/.walkie/control.sock — the
//	              event stream and four-command surface of ts:control-socket;
//	              pass -socket="" to disable). Local only, owner-only; see
//	              internal/ctlsocket's package comment for the protocol.
//
// What works today:
//
//   - `walkie` — text, presence and connection state in the terminal
//     (sto:terminal-ui), plus the local control socket for scripts
//     (ts:control-socket).
//   - `walkie history` — scriptable query over the local message store
//     (sto:message-history criterion 2): by conversation, by time range, one
//     JSON object per line. See its -help for the security statement that
//     ships with it.
//   - `walkie help` — the whole command surface, derived from the structures
//     that implement it so it cannot drift (ts:docs-quickstart-runbook
//     criterion 2).
//   - `walkie -version` — which build variant this is, which matters because a
//     fleet running two variants needs a way to tell them apart on the device
//     (adr:002-runtime-stack).
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/maleolabs/walkie/internal/audio"
	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/crypto"
	"github.com/maleolabs/walkie/internal/history"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/obs"
	"github.com/maleolabs/walkie/internal/outbox"
	"github.com/maleolabs/walkie/internal/presenceview"
	"github.com/maleolabs/walkie/internal/store"
	"github.com/maleolabs/walkie/internal/tui"
)

// version is overridden at build time with -ldflags "-X main.version=...".
// Wiring that into the release pipeline belongs to ts:build-release-matrix.
var version = "dev"

// defaultCoordinatorURL is where the client looks for the coordinator unless
// -coordinator says otherwise: the coordinator binary's default hostname,
// resolvable by MagicDNS on any tailnet with defaults, on its default control
// port. See the package comment for why the scheme is plain ws.
const defaultCoordinatorURL = "ws://walkie-coordinator:443"

func main() {
	// Subcommand dispatch happens before global flag parsing: flag.Parse stops
	// at the first non-flag argument, so "walkie history ..." would otherwise
	// fall through to the interactive client below. dispatchSubcommand's
	// runner table and help.go's subcommandSummaries are pinned together by
	// help_test.go — a subcommand may exist in one place only if it exists in
	// both.
	if len(os.Args) > 1 {
		if code, handled := dispatchSubcommand(os.Args[1], os.Args[2:], os.Stdout, os.Stderr); handled {
			os.Exit(code)
		}
	}

	cf := registerClientFlags(flag.CommandLine)
	flag.Parse()

	if *cf.version {
		printVersion(os.Stdout)
		return
	}
	if *cf.db == "" {
		fmt.Fprintln(os.Stderr, "walkie: cannot resolve a home directory for the history database; pass -db")
		os.Exit(1)
	}
	if *cf.identity == "" {
		fmt.Fprintln(os.Stderr, "walkie: cannot resolve a home directory for the identity key; pass -identity")
		os.Exit(1)
	}

	logger := obs.NewLogger(os.Stderr, obs.ParseLevel(os.Getenv("WALKIE_LOG_LEVEL")))
	if err := runClient(logger, clientConfig{
		coordinatorURL: *cf.coordinator,
		dbPath:         *cf.db,
		identityPath:   *cf.identity,
		socketPath:     *cf.socket,
	}); err != nil {
		logger.Error("walkie: fatal", slog.String("reason", err.Error()))
		os.Exit(1)
	}
}

// clientFlags holds the parsed client flag values. It exists so main() and
// runHelp register the IDENTICAL flag set from one function: help enumerates
// what main parses, so the two cannot drift.
type clientFlags struct {
	version     *bool
	coordinator *string
	db          *string
	identity    *string
	socket      *string
}

// registerClientFlags registers the client's flags on fs. Every flag has a
// sane default; none is required; there is no configuration file (see the
// package comment for why the surface stays this small).
func registerClientFlags(fs *flag.FlagSet) clientFlags {
	return clientFlags{
		version:     fs.Bool("version", false, "print version and build variant, then exit"),
		coordinator: fs.String("coordinator", defaultCoordinatorURL, "control-plane WebSocket URL of the coordinator"),
		db:          fs.String("db", defaultDBPath(), "path to the history database"),
		identity:    fs.String("identity", defaultIdentityPath(), "path to this device's X25519 identity key"),
		socket:      fs.String("socket", defaultSocketPath(), "local control socket path (empty disables the control socket)"),
	}
}

// clientConfig is everything runClient needs from the flag surface.
type clientConfig struct {
	coordinatorURL string
	dbPath         string
	identityPath   string
	socketPath     string
}

// runClient assembles and runs the interactive client. Startup order is not
// interchangeable: local state opens BEFORE any network attempt, so a bad
// database path or unwritable key file fails fast and locally with a message
// that names the fix, instead of after a connection has been established.
func runClient(logger *slog.Logger, cfg clientConfig) error {
	clk := clock.Real()

	// Message history (sto:message-history): the same file `walkie history`
	// queries, at the same default path, so the live view and the scriptable
	// query read one store. Retention bounds are the package defaults —
	// bounded retention is an invariant, not a knob to skip.
	hist, err := history.Open(cfg.dbPath, clk, history.Options{
		TTL:         history.DefaultTTL,
		MaxMessages: history.DefaultMaxMessages,
		Logger:      logger,
	})
	if err != nil {
		return fmt.Errorf("open history %s: %w", cfg.dbPath, err)
	}
	defer hist.Close()

	// This device's identity key (ts:queue-sealed-box): generated owner-only
	// on first run, loaded on every later one. Everything queued for this
	// device while it was offline is sealed to THIS key; losing the file
	// forfeits exactly those messages (adr:004 consequence, stated in README).
	key, err := crypto.LoadOrCreateIdentity(cfg.identityPath)
	if err != nil {
		return fmt.Errorf("load identity %s: %w", cfg.identityPath, err)
	}
	defer key.Zero()

	// The offline outbox (sto:offline-queue) shares the history file: the
	// embedded schema carries both tables, and one directory keeps backups and
	// permissions uniform. It opens through its own store handle —
	// history.Store does not expose its internals, and reaching around a seam
	// to share one pool would buy a file descriptor at the cost of coupling;
	// sequential opens cannot race each other's migrations, and busy_timeout
	// absorbs the rare cross-handle write overlap at human messaging rates.
	boxStore, err := store.Open(cfg.dbPath, clk)
	if err != nil {
		return fmt.Errorf("open outbox store %s: %w", cfg.dbPath, err)
	}
	defer boxStore.Close()
	box, err := outbox.New(boxStore, clk, logger)
	if err != nil {
		return fmt.Errorf("start outbox: %w", err)
	}

	// The roster seam (sto:device-presence): fed by OnEnvelope below, rendered
	// by the UI. Eagerly constructed — unlike the hub it needs no identity,
	// and roster frames can arrive from the first moment online.
	presence := presenceview.New(logger)

	// Connection supervision (ts:reconnect-resume): machine first, client on
	// top, production WebSocket dialer through the host's tailscale (see the
	// package comment for why not tsnet).
	mach, err := control.NewMachine(clk, logger)
	if err != nil {
		return fmt.Errorf("connection machine: %w", err)
	}
	dialer := control.NewWebSocketDialer(cfg.coordinatorURL, nil)
	seedA, seedB := backoffSeeds()
	client, err := control.NewClient(control.DefaultConfig(), mach, dialer.Dial, seedA, seedB, clk, logger)
	if err != nil {
		return fmt.Errorf("connection client: %w", err)
	}

	a := newApp(clk, logger, client, box, hist, key, presence)
	client.OnEnvelope(a.OnEnvelope)

	// The local control socket (ts:control-socket): auxiliary by design, so
	// ANY startup failure degrades to one warning and a socket-less run —
	// including windows (no Unix sockets in the form this design recognises)
	// and another walkie already holding the path (ErrInUse; that instance
	// owns the surface). Text and presence never depended on it.
	if cfg.socketPath != "" {
		srv, l, err := startControlSocket(cfg.socketPath, ctlDeps{
			app:      a,
			client:   client,
			mach:     mach,
			presence: presence,
		}, logger)
		if err != nil {
			logger.Warn("control socket disabled",
				slog.String("path", cfg.socketPath),
				slog.String("reason", err.Error()),
			)
		} else {
			// Both halves close, server first: Close drops the connections
			// and subscriptions, then the listener's Close unlinks the socket
			// file (Go only unlinks on listener close) and ends the accept
			// loop Serve would otherwise block in forever.
			defer func() { srv.Close(); _ = l.Close() }()
			// Message events carry metadata only — no body ever crosses the
			// control socket (ctlsocket package comment explains the line).
			a.SetOnFiled(func(msg message.Message) {
				srv.PublishMessage(msg.ID, msg.Sender, msg.Recipient,
					message.ConversationKeyFor(client.ConnectedAs(), msg))
			})
		}
	}

	// Subscribe-first (Machine.Subscribe's rule), then seed the UI from the
	// direct read: a change landing between the two appears in both — a
	// harmless duplicate, never a miss. Two subscriptions because each is
	// single-consumer: one drives the per-online duties, one drives the UI.
	connSub := mach.Subscribe()
	dutySub := mach.Subscribe()
	go a.WatchConn(dutySub)

	model := tui.New(tui.Params{
		HubFunc:         a.hubSource,
		Presence:        presence,
		ConnChanges:     connSub.C(),
		PresenceChanges: presence.Subscribe().C(),
		Inbound:         a.inbound,
		ConnectedAs:     client.ConnectedAs,
		Send:            a.SendFromUI,
		AudioAvailable:  audio.Available(),
		ConnState:       mach.State(),
	})

	client.Start()
	defer client.Stop()
	defer mach.Close() // ends the subscription pumps Stop leaves running

	program := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := program.Run(); err != nil {
		return fmt.Errorf("terminal interface: %w", err)
	}
	return nil
}

// printVersion reports the build variant (adr:002-runtime-stack): a fleet
// running two variants needs a way to tell them apart on the device.
func printVersion(w io.Writer) {
	fmt.Fprintf(w, "walkie %s %s/%s\n", version, runtime.GOOS, runtime.GOARCH)

	// The interesting half. A device that cannot install libopus still runs a
	// fully useful text and presence client, so "no audio" is a normal state to
	// report plainly rather than a defect to hide.
	if audio.Available() {
		fmt.Fprintln(w, `audio  available (built with the "voice" tag)`)
	} else {
		fmt.Fprintln(w, `audio  unavailable (built without the "voice" tag)`)
	}
}

// defaultIdentityPath mirrors defaultDBPath: one directory per user, named
// after the runtime state .gitignore already excludes.
func defaultIdentityPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "" // caller reports it; there is nothing sensible to guess
	}
	return filepath.Join(home, ".walkie", "identity.key")
}
