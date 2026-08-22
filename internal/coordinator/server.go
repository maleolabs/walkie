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
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
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
type Server struct {
	resolver tsauth.Resolver
	logger   *slog.Logger
	clk      clock.Clock
}

// NewServer returns a Server resolving identities through resolver and reading
// its clock from clk. A nil logger falls back to slog's default: the skeleton
// must never be silently unlogged, because criterion 2's refusal evidence IS a
// log line.
func NewServer(resolver tsauth.Resolver, clk clock.Clock, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{resolver: resolver, logger: logger, clk: clk}
}

// Serve accepts connections on ln until ctx is cancelled or ln fails,
// handling each connection concurrently.
//
// The listener comes from the caller (see the type comment for why). Serve
// returns the accept error when it was not caused by shutdown; a clean
// ctx cancellation returns nil.
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
		return fmt.Errorf("coordinator: serve %s: %w", ln.Addr(), err)
	case <-ctx.Done():
		// Graceful path: stop accepting, give live connections the grace
		// window to finish their in-flight exchange, then return. Per-
		// connection reads use ctx-derived contexts, so cancellation reaches
		// them without a connection registry.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := hs.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("coordinator: shutdown timed out", slog.Duration("grace", shutdownGrace))
			return nil
		}
		return nil
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

	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		// Accept already wrote the HTTP error for us; log for the operator.
		s.logger.Warn("websocket upgrade failed",
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("reason", err.Error()),
		)
		return
	}
	// Read-loop exit closes with whatever status the loop chose; this defer
	// only catches panics and early returns. CloseNow semantics are fine
	// there — nothing half-written matters on an abnormal exit.
	defer ws.Close(websocket.StatusInternalError, "")

	s.serveConn(r.Context(), ws, id)
}

// serveConn reads protobuf Envelopes from one authenticated connection until
// the peer disconnects or the server shuts down.
//
// Unknown fields are ignored, not rejected: proto3 decode drops them by
// construction, and this package must do nothing to opt out — the fleet runs
// mixed versions and adr:003-wire-protocol forbids lockstep upgrades. A test
// pins this at the server level (server_test.go), not just at the codec.
func (s *Server) serveConn(ctx context.Context, ws *websocket.Conn, id tsauth.Identity) {
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
			s.reply(ws, &walkiev1.Envelope{
				Payload: &walkiev1.Envelope_ProtocolError{
					ProtocolError: &walkiev1.ProtocolError{
						Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
						Detail: "envelope did not decode",
					},
				},
			})
			continue
		}

		if !s.dispatch(ctx, ws, id, &env) {
			return
		}
	}
}

// dispatch handles one decoded envelope. It reports false when the connection
// must close (version-gate refusal), true otherwise.
//
// This switch is the skeleton's deliberate minimum. Presence updates belong to
// sto:device-presence, queue drain to sto:offline-queue, key distribution to
// ts:queue-sealed-box: each lands here as its own case with its owning item's
// tests. Everything unrecognized is logged-and-ignored — NEVER rejected —
// because ignoring unknown payloads is exactly how an older coordinator
// survives a newer client (adr:003 mixed-fleet rule).
func (s *Server) dispatch(ctx context.Context, ws *websocket.Conn, id tsauth.Identity, env *walkiev1.Envelope) bool {
	switch payload := env.GetPayload().(type) {
	case *walkiev1.Envelope_Hello:
		return s.handleHello(ws, id, payload.Hello)
	default:
		// Structured, content-free: the payload TYPE name is protocol
		// metadata, safe to log; message_id identifies without disclosing.
		s.logger.Debug("payload ignored: no handler in skeleton",
			slog.String("node_name", id.NodeName),
			slog.String("payload_type", fmt.Sprintf("%T", env.GetPayload())),
			slog.String("message_id", env.GetMessageId()),
		)
		return true
	}
}

// handleHello answers the handshake.
//
// The version gate is real but minimal: walkiev1 owns the range and the
// refusal shape (ts:protocol-schema-v1 placed them beside the generated code
// so both ends cannot disagree about compatibility). An out-of-range peer gets
// the structured ProtocolError naming both versions, then the connection
// closes — there is nothing further two incompatible endpoints can say.
func (s *Server) handleHello(ws *websocket.Conn, id tsauth.Identity, hello *walkiev1.Hello) bool {
	if !walkiev1.ProtocolVersionSupported(hello.GetProtocolVersion()) {
		s.reply(ws, &walkiev1.Envelope{
			Payload: &walkiev1.Envelope_ProtocolError{
				ProtocolError: walkiev1.NewVersionUnsupportedError(hello.GetProtocolVersion()),
			},
		})
		s.logger.Info("peer refused: unsupported protocol version",
			slog.String("node_name", id.NodeName),
			slog.Uint64("peer_version", uint64(hello.GetProtocolVersion())),
		)
		ws.Close(websocket.StatusUnsupportedData, "unsupported protocol version")
		return false
	}

	// Device echoes the RESOLVED identity back: the client learns who the
	// tailnet says it is, it does not assert one (Hello.device field doc).
	// NodeName is the device identifier; LoginName is the fallback for
	// daemons that answered WhoIs without a node record.
	device := id.NodeName
	if device == "" {
		device = id.LoginName
	}

	// pending_count is honestly zero: queue drain belongs to
	// sto:offline-queue, which will replace this constant when it lands.
	// received_at comes from the injected clock — every timestamp this
	// process writes must be fakeable under test (ts:test-harness).
	s.reply(ws, &walkiev1.Envelope{
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

// reply writes one envelope as a binary frame.
//
// Coordinator-originated envelopes leave message_id empty on purpose: dedup
// by ULID exists for REDELIVERED traffic (req:offline-delivery), whose policy
// belongs to sto:offline-queue. Inventing an ID scheme here would pre-empt
// that design; request-reply correlation on this connection needs none.
func (s *Server) reply(ws *websocket.Conn, env *walkiev1.Envelope) {
	data, err := proto.Marshal(env)
	if err != nil {
		// Marshal failure on a structurally valid reply is a programmer
		// error; log and let the read loop's next iteration surface the
		// dead connection.
		s.logger.Error("reply marshal failed", slog.String("reason", err.Error()))
		return
	}
	if err := ws.Write(context.Background(), websocket.MessageBinary, data); err != nil {
		s.logger.Warn("reply write failed", slog.String("reason", err.Error()))
	}
}

// isNormalClose reports whether err is just the connection ending: the peer's
// close frame or our own shutdown cancelling the read. Anything else from the
// read loop is worth an operator's attention.
func isNormalClose(err error) bool {
	return errors.Is(err, io.EOF) ||
		websocket.CloseStatus(err) != -1 ||
		errors.Is(err, context.Canceled)
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
