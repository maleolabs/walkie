package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator/presence"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// maxEnvelopeBytes bounds one control-plane frame.
//
// Why this number: the largest legitimate payload today is an AttachmentChunk
// carrying a 1 MiB content-addressed chunk (adr:003-wire-protocol; voice notes
// ride the CONTROL plane as attachments in the MVP — real-time audio is the
// UDP data plane and never appears here). 4 MiB leaves room for envelope
// overhead and fleet growth without letting a hostile peer stream unbounded
// bytes into coordinator memory per frame.
const maxEnvelopeBytes = 4 << 20

// shutdownGrace is how long Serve waits for in-flight connections to wind down
// after a shutdown signal before walking away. Human-scale messaging means no
// legitimate connection needs longer to notice a closed read; anything still
// alive after this is stuck and better off killed than waited on.
const shutdownGrace = 5 * time.Second

// closeHandshakeBudget bounds how long one connection's polite GoingAway
// handshake may hold up its handler before shutdown aborts the transport.
//
// Why not the whole window: Close waits for the peer to echo the close frame,
// and a peer with no active reader never echoes — its internal timeout is 5s,
// exactly the grace window, so an ordinary idle client would burn the entire
// budget and every deploy would report an incomplete drain. Half the window
// leaves the polite path to responsive peers and still reserves time for the
// aborted handler to exit before the deadline.
const closeHandshakeBudget = shutdownGrace / 2

// ErrDrainIncomplete is returned by Serve when live connections were still
// running when the grace window expired.
//
// Why a sentinel rather than logging alone: main.go must not print "shutdown
// complete" when connections survived the drain — that line is a promise to
// the operator that defers closing tsnet and the store ran under zero live
// traffic. Callers distinguish it with errors.Is; it is still an abnormal
// end, just one the process handles by reporting honestly instead of failing.
var ErrDrainIncomplete = errors.New("coordinator: graceful drain incomplete")

// Server is the coordinator's accept loop: one WebSocket per client, each
// authenticated by tailnet identity before any protocol traffic flows.
//
// Owning work item:
//
//	eka get walkie/ts:coordinator-skeleton
//
// # The identity gate is the security boundary
//
// adr:004-security-model authenticates a caller with a WhoIs lookup on the
// connection's remote address. This type enforces that lookup as the FIRST act
// on every accepted connection — before the WebSocket upgrade completes, so
// zero walkie traffic can precede it. A connection whose identity cannot be
// resolved is refused AND logged (criterion 2). There is deliberately no
// anonymous fallback, no token check and no session anywhere downstream of
// this gate: if you find yourself adding one, re-read adr:004.
//
// # The listener is injected, not opened here
//
// Serve takes a ready [net.Listener]. Production wiring passes tsnet's
// listener, which binds ONLY the node's tailnet IP — never 0.0.0.0 (criterion
// 3). Injection is what makes that property testable rather than asserted:
// server_test.go binds loopback explicitly AS A SIMULATED TAILNET INTERFACE
// and proves a dial addressed to another interface cannot reach the server.
// Nothing in this package ever calls net.Listen itself.
//
// # Shutdown drains through a registry, not through net/http
//
// websocket.Accept hijacks every connection, and http.Server.Shutdown neither
// closes nor waits for hijacked connections — so without help, Serve would
// return while every read loop was still running and the grace window would
// be dead code. The conns registry plus the wg counter below are that help:
// shutdown force-closes each live connection to unblock its Reader, then
// waits for the handlers to actually exit before returning.
type Server struct {
	resolver tsauth.Resolver
	logger   *slog.Logger
	clk      clock.Clock

	// presence is the server-authoritative liveness tracker (sto:device-presence).
	// It is wired, not bypassed: every input to it is an event THIS process
	// witnesses — a connection completing the identity gate below, an
	// application-level Heartbeat arriving on dispatch, or a connection
	// handler exiting. There is deliberately no envelope in the schema and no
	// branch in dispatch through which a client could assert its own online
	// state (req:device-presence criterion 5); if you are about to add one,
	// stop and re-read the Tracker's type comment first.
	//
	// A nil tracker disables presence entirely: the skeleton's original
	// behaviour, still used by tests that exercise non-presence machinery.
	// Production wiring (cmd/walkie-coordinator) always passes a real one.
	presence *presence.Tracker

	// conns tracks every live WebSocket with its per-connection cancel func
	// so shutdown can close them all and, only as a last resort, abort the
	// stragglers. Guarded by connsMu; entries are added after Accept and
	// removed when the connection's handler exits.
	connsMu sync.Mutex
	conns   map[*websocket.Conn]context.CancelFunc

	// draining flips to true the moment shutdown starts snapshotting.
	// Why it must be checked atomically WITH registration: a client dial
	// completes while the handler is between Accept and track, so shutdown
	// can legitimately run in that gap — without the flag its snapshot
	// would miss the connection and neither close it nor count it as
	// closed, hanging the drain wait for the whole grace window.
	draining bool

	// wg counts running connection handlers. Shutdown waits on it (bounded
	// by the grace window) so Serve returning means the read loops really
	// exited — not merely that net/http stopped counting them.
	wg sync.WaitGroup
}

// NewServer returns a Server resolving identities through resolver and reading
// its clock from clk. A nil logger falls back to slog's default: the skeleton
// must never be silently unlogged, because criterion 2's refusal evidence IS a
// log line.
//
// tracker wires server-authoritative presence (sto:device-presence). Nil
// disables it — the pre-presence skeleton shape, kept for tests that exercise
// other machinery — but every production binary passes a real Tracker backed
// by the store, so liveness facts survive a coordinator restart.
func NewServer(resolver tsauth.Resolver, clk clock.Clock, logger *slog.Logger, tracker *presence.Tracker) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		resolver: resolver,
		logger:   logger,
		clk:      clk,
		presence: tracker,
		conns:    make(map[*websocket.Conn]context.CancelFunc),
	}
}

// Serve accepts connections on ln until ctx is cancelled or ln fails,
// handling each connection concurrently.
//
// The listener comes from the caller (see the type comment for why). Serve
// returns the accept error when it was not caused by shutdown; a clean ctx
// cancellation returns nil once every live connection has actually drained,
// or [ErrDrainIncomplete] if connections outlived the grace window.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	hs := &http.Server{
		Handler: s,
		// The identity gate runs inside the handler, which only fires once
		// headers are parsed; this timeout keeps a peer that opens a socket
		// and sends nothing from holding a slot forever. It is transport
		// hygiene, not authentication.
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- hs.Serve(ln) }()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		// hs.Serve failed out from under us, so no shutdown will drain the
		// live connections; release their contexts so nothing per-connection
		// outlives this call.
		for _, cancelConn := range s.liveCancels() {
			cancelConn()
		}
		return fmt.Errorf("coordinator: serve %s: %w", ln.Addr(), err)
	case <-ctx.Done():
		return s.shutdown(hs)
	}
}

// shutdown stops the HTTP server and drains live WebSocket connections
// within [shutdownGrace].
//
// Why an explicit registry close is needed at all: hs.Shutdown closes the
// listener and waits only for handlers still holding non-hijacked
// connections. Every WebSocket here is hijacked, so Shutdown returns while
// all read loops are still running — closing each registered connection is
// what actually reaches the blocked Readers, and the WaitGroup wait is what
// makes "Serve returned" mean "no handler is still running" rather than
// "net/http stopped counting".
//
// Why shutdown does NOT cancel the per-connection contexts up front: a
// cancelled Reader ctx makes coder/websocket's timeout loop cut the TCP
// connection immediately, without a close frame — the peer would see a bare
// EOF and the "graceful" in graceful shutdown would be a lie. The GoingAway
// close below is therefore what unblocks Readers; per-connection contexts
// are force-cancelled only against stragglers that outlived the window.
func (s *Server) shutdown(hs *http.Server) error {
	deadline := time.Now().Add(shutdownGrace)
	shutdownCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Refuse new registrations before snapshotting: a handler still between
	// Accept and track when this flag flips sees it under the same lock and
	// closes its own connection instead of joining a drain that already
	// counted it absent.
	s.connsMu.Lock()
	s.draining = true
	s.connsMu.Unlock()

	if err := hs.Shutdown(shutdownCtx); err != nil {
		// Only non-hijacked work (an in-flight identity lookup, say) can
		// cause this; the WebSocket drain below is bounded separately.
		s.logger.Warn("coordinator: http shutdown timed out", slog.Duration("grace", shutdownGrace))
	}

	// Close every live connection. Close performs the WebSocket close
	// handshake — sending GoingAway first, so well-behaved peers learn the
	// server is stopping — and then closes the transport, which is what
	// unblocks this side's Readers. Each close runs on its own goroutine so
	// one stuck peer cannot serially eat the whole grace window.
	for _, ws := range s.liveConns() {
		go func() {
			handshakeDone := make(chan struct{})
			go func() {
				defer close(handshakeDone)
				_ = ws.Close(websocket.StatusGoingAway, "server shutting down")
			}()
			select {
			case <-handshakeDone:
			case <-time.After(closeHandshakeBudget):
				s.abortConn(ws)
			}
		}()
	}

	waitCtx, cancelWait := context.WithDeadline(context.Background(), deadline)
	defer cancelWait()
	drained := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		return nil
	case <-waitCtx.Done():
		// Last resort: abort whatever outlived the window at the transport
		// level so no handler can outlive Serve itself.
		for _, cancelConn := range s.liveCancels() {
			cancelConn()
		}
		err := fmt.Errorf("%w: %d connection(s) still live after %s",
			ErrDrainIncomplete, len(s.liveConns()), shutdownGrace)
		s.logger.Warn("coordinator: shutdown incomplete", slog.String("reason", err.Error()))
		return err
	}
}

// liveConns snapshots the currently registered connections.
func (s *Server) liveConns() []*websocket.Conn {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	live := make([]*websocket.Conn, 0, len(s.conns))
	for ws := range s.conns {
		live = append(live, ws)
	}
	return live
}

// liveCancels snapshots the cancel funcs of currently registered
// connections, for the post-grace-window force-abort path.
func (s *Server) liveCancels() []context.CancelFunc {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	cancels := make([]context.CancelFunc, 0, len(s.conns))
	for _, cancelConn := range s.conns {
		cancels = append(cancels, cancelConn)
	}
	return cancels
}

// track registers ws with its cancel func in the shutdown registry. It
// reports false when shutdown has already begun: the caller must close the
// connection itself and release, because the drain's snapshot will never
// know about it. See the ServeHTTP call site for the ordering rules.
func (s *Server) track(ws *websocket.Conn, cancelConn context.CancelFunc) bool {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if s.draining {
		return false
	}
	s.conns[ws] = cancelConn
	return true
}

// release removes ws from the shutdown registry and marks its handler done.
// Deferred by serveConn so both happen exactly once per accepted connection,
// on every exit path including panics.
func (s *Server) release(ws *websocket.Conn) {
	s.connsMu.Lock()
	delete(s.conns, ws)
	s.connsMu.Unlock()
	s.wg.Done()
}

// abortConn cancels one connection's context: the transport-level breaker
// shutdown uses when a connection's polite close handshake stalls. Cancelling
// makes coder/websocket's timeout loop close the underlying connection,
// which both ends the stalled handshake and unblocks the handler's Reader.
func (s *Server) abortConn(ws *websocket.Conn) {
	s.connsMu.Lock()
	cancelConn := s.conns[ws]
	s.connsMu.Unlock()
	if cancelConn != nil {
		cancelConn()
	}
}

// ServeHTTP upgrades one request to the control-plane WebSocket.
//
// It implements http.Handler so the accept path is exactly the production
// path under test: tests drive it through the same injected-listener Serve
// call main.go uses, not through a parallel code path.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Identity BEFORE any protocol traffic (criterion 2). This runs before
	// websocket.Accept, so not even the upgrade response reaches a peer we
	// cannot attribute. r.RemoteAddr is the socket's peer address; on the
	// production tsnet listener that is the caller's tailnet IP, which is
	// precisely what WhoIs resolves.
	remote, err := remoteAddrOf(r.RemoteAddr)
	if err != nil {
		s.logger.Warn("connection refused: malformed remote address",
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("reason", err.Error()),
		)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	id, err := s.resolver.Resolve(r.Context(), remote)
	if err != nil {
		// Refusal log carries the remote ADDRESS and error class only —
		// never message content, never key material (the no-content rule
		// arrives formally with ts:observability-baseline but holds from
		// the first line).
		s.logger.Warn("connection refused: identity unresolved",
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("reason", err.Error()),
		)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	s.logger.Info("connection accepted",
		slog.String("login_name", id.LoginName),
		slog.String("node_name", id.NodeName),
	)

	// The WaitGroup count goes up BEFORE Accept: once Accept hijacks the
	// connection, net/http stops accounting for this handler entirely, so a
	// count added afterwards could race Serve's drain wait past a connection
	// accepted in exactly that instant.
	s.wg.Add(1)
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.wg.Done()
		// Accept already wrote the HTTP error for us; log for the operator.
		s.logger.Warn("websocket upgrade failed",
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("reason", err.Error()),
		)
		return
	}
	// Per-connection lifetime. Deliberately NOT a context-tree child of
	// Serve's ctx: cancelling a parent cancels its children instantly, and a
	// cancelled Reader ctx makes coder/websocket's timeout loop cut the TCP
	// connection on the spot — the peer would see a bare EOF instead of the
	// graceful GoingAway, racing and usually beating shutdown's close. The
	// ctx is detached instead and cancelled exactly where its lifetime must
	// end: handler exit here, and shutdown's force-abort of stragglers.
	// Either way no connection ctx outlives Serve, which is the property
	// deriving from Serve's ctx exists to guarantee.
	ctx, cancel := context.WithCancel(context.Background())

	// Registered after a successful Accept so shutdown's snapshot sees every
	// hijacked-but-still-running connection; serveConn removes it. If
	// shutdown began in the instant between Accept and this registration,
	// the snapshot already ran without us — close our own connection and
	// leave, or the drain wait would hang on a connection nobody will
	// ever close.
	if !s.track(ws, cancel) {
		s.release(ws)
		ws.CloseNow()
		return
	}

	defer func() {
		// Read-loop exit closes with whatever status the loop chose; this
		// defer catches panics and plain returns. A cancelled connection
		// (shutdown abort path) tells the peer GoingAway — a deliberate
		// server stop, not an internal error.
		//
		// Async on purpose: Close blocks until the close handshake resolves,
		// and if shutdown's own Close is mid-handshake on this connection
		// the two would serialize into its full internal timeout — gating
		// handler exit, and therefore the drain wait, on nothing the peer
		// controls. Nothing here needs the handshake's result.
		code := websocket.StatusInternalError
		if ctx.Err() != nil {
			code = websocket.StatusGoingAway
		}
		go ws.Close(code, "")
	}()

	s.serveConn(ctx, ws, &connWriter{ws: ws, logger: s.logger}, id)
}

// serveConn reads protobuf Envelopes from one authenticated connection until
// the peer disconnects or the server shuts down.
//
// Unknown fields are ignored, not rejected: proto3 decode drops them by
// construction, and this package must do nothing to opt out — the fleet runs
// mixed versions and adr:003-wire-protocol forbids lockstep upgrades. A test
// pins this at the server level (server_test.go), not just at the codec.
//
// # Presence lifecycle (sto:device-presence)
//
// When a tracker is wired, this function brackets the connection's whole
// lifetime — every exit path, including drain and shutdown aborts — with
// exactly one ConnectionEstablished / ConnectionLost pair:
//
//   - Established runs BEFORE any protocol traffic. The identity gate above
//     already proved who the peer is; a live WebSocket to a proven identity is
//     first-hand evidence of life, so criterion 1 ("a connecting device
//     appears online") applies at accept time, not at Hello time. A client
//     that connects and dies before Hello therefore still flickers online→
//     offline honestly rather than being invisible for its whole short life.
//   - Lost runs from a defer, so clean closes, read errors, version-gate
//     refusals, size-cap refusals, panics and shutdown aborts all record the
//     loss exactly once. The tracker's refcount absorbs reconnect overlap,
//     where the new handler's Establish lands before the old handler's Loss.
func (s *Server) serveConn(ctx context.Context, ws *websocket.Conn, out *connWriter, id tsauth.Identity) {
	// Removed on exit so the shutdown registry only ever lists connections
	// whose read loop is actually still running; the paired WaitGroup Done
	// is what lets Serve's drain wait finish.
	defer s.release(ws)

	device := deviceOf(id)

	if s.presence != nil {
		s.presence.ConnectionEstablished(device)

		// Subscribe BEFORE snapshotting the roster (view.go documents why
		// this order cannot miss an event; duplicates it can produce are
		// harmless because PresenceUpdates are idempotent statements of
		// current state). The pump goroutine forwards changes to THIS
		// connection for as long as it lives; sub.Close below both stops it
		// and makes its exit prompt on every path out of this function.
		sub := s.presence.Subscribe()
		defer sub.Close()
		go s.pumpPresence(ctx, out, sub, device)

		// The new connection gets the full roster once, itself included —
		// a roster needs its own line, and a point-in-time read model is
		// not the event stream the subject-exclusion rule governs.
		for _, e := range s.presence.Snapshot() {
			out.write(ctx, presenceUpdateEnvelope(e))
		}

		defer s.presence.ConnectionLost(device)
	}

	for {
		typ, frame, err := ws.Reader(ctx)
		if err != nil {
			// Normal lifecycle: peer close and server shutdown both land
			// here. Only unexpected failures are worth a warn line.
			if !isNormalClose(err) && ctx.Err() == nil {
				s.logger.Warn("read loop ended",
					slog.String("node_name", id.NodeName),
					slog.String("reason", err.Error()),
				)
			}
			return
		}
		if typ != websocket.MessageBinary {
			// Every control-plane envelope is a binary protobuf frame
			// (adr:003). A text frame means the peer is not speaking the
			// protocol; refuse rather than guess.
			s.logger.Warn("non-binary frame on control connection",
				slog.String("node_name", id.NodeName),
			)
			ws.Close(websocket.StatusUnsupportedData, "control plane speaks binary protobuf")
			return
		}

		data, err := io.ReadAll(io.LimitReader(frame, maxEnvelopeBytes+1))
		if err != nil {
			s.logger.Warn("frame read failed",
				slog.String("node_name", id.NodeName),
				slog.String("reason", err.Error()),
			)
			return
		}
		if len(data) > maxEnvelopeBytes {
			s.logger.Warn("envelope exceeds size cap",
				slog.String("node_name", id.NodeName),
				slog.Int("cap_bytes", maxEnvelopeBytes),
			)
			ws.Close(websocket.StatusMessageTooBig, "envelope too large")
			return
		}

		var env walkiev1.Envelope
		if err := proto.Unmarshal(data, &env); err != nil {
			// Undecodable bytes are a broken peer, not a hostile one — the
			// identity is known and the frame was size-capped. Answer with
			// the structured malformed error and drop the frame; the
			// connection stays up so the client can resync.
			s.logger.Warn("undecodable envelope",
				slog.String("node_name", id.NodeName),
				slog.String("reason", err.Error()),
			)
			out.write(ctx, &walkiev1.Envelope{
				Payload: &walkiev1.Envelope_ProtocolError{
					ProtocolError: &walkiev1.ProtocolError{
						Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
						Detail: "envelope did not decode",
					},
				},
			})
			continue
		}

		if !s.dispatch(ctx, out, id, &env) {
			return
		}
	}
}

// dispatch handles one decoded envelope. It reports false when the connection
// must close (version-gate refusal), true otherwise.
//
// This switch is the skeleton's deliberate minimum. Queue drain belongs to
// sto:offline-queue, key distribution to ts:queue-sealed-box: each lands here
// as its own case with its owning item's tests. Everything unrecognized is
// logged-and-ignored — NEVER rejected — because ignoring unknown payloads is
// exactly how an older coordinator survives a newer client (adr:003
// mixed-fleet rule).
//
// # Criterion 5, verified at the only place new client input enters
//
// The presence cases below are the complete set of client-originated inputs to
// liveness, and neither asserts anything:
//
//   - Heartbeat carries no fields at all. Its arrival is OBSERVED evidence of
//     life — the fact is that bytes came from the peer's socket, not anything
//     the peer claimed. ObserveHeartbeat records the observation; it accepts
//     no state.
//   - PresenceStatusChange carries a status LABEL and nothing else (the proto
//     field doc forbids folding an online flag into it). SetStatus stores the
//     label; it cannot move liveness in either direction.
//
// There is no "I am online" envelope in walkie.v1 and none may be added here:
// the tracker's API makes such an assertion unrepresentable, and this switch
// must never grow a case that routes one. req:device-presence criterion 5.
func (s *Server) dispatch(ctx context.Context, out *connWriter, id tsauth.Identity, env *walkiev1.Envelope) bool {
	switch payload := env.GetPayload().(type) {
	case *walkiev1.Envelope_Hello:
		return s.handleHello(ctx, out, id, payload.Hello)

	case *walkiev1.Envelope_Heartbeat:
		if s.presence != nil {
			s.presence.ObserveHeartbeat(deviceOf(id))
		}
		return true

	case *walkiev1.Envelope_PresenceStatusChange:
		return s.handleStatusChange(ctx, out, id, payload.PresenceStatusChange)

	default:
		// Structured, content-free: the payload TYPE name is protocol
		// metadata, safe to log; message_id identifies without disclosing.
		s.logger.Debug("payload ignored: no handler",
			slog.String("node_name", id.NodeName),
			slog.String("payload_type", fmt.Sprintf("%T", env.GetPayload())),
			slog.String("message_id", env.GetMessageId()),
		)
		return true
	}
}

// handleStatusChange applies a client's own custom status label.
//
// The tracker enforces the receiver-side bound from ts:protocol-schema-v1
// (256 bytes of UTF-8) and REJECTS rather than truncates — truncation would
// silently publish something the user did not write. The rejection travels
// back as a ProtocolError so the client can push back on its user instead of
// believing the status took. MALFORMED is the closest code the schema offers
// for "your frame violated a receiver-enforced rule"; the detail names the
// actual rule. The connection stays open either way: a rejected status is a
// correctable mistake, not a broken peer.
//
// Success answers with nothing on THIS connection — the setter applied its
// own status locally, and the change reaches everyone else as a PresenceUpdate
// broadcast. An ack would be a third copy of information both ends already
// hold.
func (s *Server) handleStatusChange(ctx context.Context, out *connWriter, id tsauth.Identity, ch *walkiev1.PresenceStatusChange) bool {
	if s.presence == nil {
		return true // presence disabled: nothing to apply, nothing to reject
	}
	if err := s.presence.SetStatus(deviceOf(id), ch.GetStatus()); err != nil {
		s.logger.Warn("status change rejected",
			slog.String("node_name", id.NodeName),
			slog.Int("len_bytes", len(ch.GetStatus())),
			slog.String("reason", err.Error()),
		)
		out.write(ctx, &walkiev1.Envelope{
			Payload: &walkiev1.Envelope_ProtocolError{
				ProtocolError: &walkiev1.ProtocolError{
					Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
					Detail: "presence_status_change.status rejected: " + err.Error(),
				},
			},
		})
	}
	return true
}

// handleHello answers the handshake.
//
// The version gate is real but minimal: walkiev1 owns the range and the
// refusal shape (ts:protocol-schema-v1 placed them beside the generated code
// so both ends cannot disagree about compatibility). An out-of-range peer gets
// the structured ProtocolError naming both versions, then the connection
// closes — there is nothing further two incompatible endpoints can say.
func (s *Server) handleHello(ctx context.Context, out *connWriter, id tsauth.Identity, hello *walkiev1.Hello) bool {
	if !walkiev1.ProtocolVersionSupported(hello.GetProtocolVersion()) {
		out.write(ctx, &walkiev1.Envelope{
			Payload: &walkiev1.Envelope_ProtocolError{
				ProtocolError: walkiev1.NewVersionUnsupportedError(hello.GetProtocolVersion()),
			},
		})
		s.logger.Info("peer refused: unsupported protocol version",
			slog.String("node_name", id.NodeName),
			slog.Uint64("peer_version", uint64(hello.GetProtocolVersion())),
		)
		out.ws.Close(websocket.StatusUnsupportedData, "unsupported protocol version")
		return false
	}

	// Device echoes the RESOLVED identity back: the client learns who the
	// tailnet says it is, it does not assert one (Hello.device field doc).
	// NodeName is the device identifier; LoginName is the fallback for
	// daemons that answered WhoIs without a node record. The same rule names
	// the device everywhere else presence is concerned — see deviceOf.
	device := id.NodeName
	if device == "" {
		device = id.LoginName
	}

	// pending_count is honestly zero: queue drain belongs to
	// sto:offline-queue, which will replace this constant when it lands.
	// received_at comes from the injected clock — every timestamp this
	// process writes must be fakeable under test (ts:test-harness).
	out.write(ctx, &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_HelloAck{
			HelloAck: &walkiev1.HelloAck{
				Device:          device,
				ReceivedAt:      timestamppb.New(s.clk.Now()),
				PendingCount:    0,
				ProtocolVersion: walkiev1.MaxProtocolVersion,
			},
		},
	})
	return true
}

// isNormalClose reports whether err is just the connection ending: the peer's
// close frame, our own shutdown cancelling the read, or the transport closing
// under us — which is exactly what shutdown's registry Close does to unblock
// Readers. Anything else from the read loop is worth an operator's attention.
func isNormalClose(err error) bool {
	return errors.Is(err, io.EOF) ||
		websocket.CloseStatus(err) != -1 ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, net.ErrClosed)
}

// remoteAddrOf parses an http.Request.RemoteAddr ("ip:port") into the
// *net.TCPAddr the Resolver interface takes.
//
// Split-and-parse rather than net.ResolveTCPAddr: RemoteAddr is always a
// literal IP with port, and the resolver form must never depend on DNS —
// authentication that can block on name resolution is authentication that can
// be taken down by it.
func remoteAddrOf(remote string) (*net.TCPAddr, error) {
	host, portStr, err := net.SplitHostPort(remote)
	if err != nil {
		return nil, fmt.Errorf("coordinator: remote addr %q: %w", remote, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("coordinator: remote addr %q: %w", remote, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("coordinator: remote addr %q: not an IP", remote)
	}
	return &net.TCPAddr{IP: ip, Port: port}, nil
}
