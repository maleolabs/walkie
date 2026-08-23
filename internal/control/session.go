package control

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// Session is one live control-plane transport connection: exactly what the
// reconnect loop needs from a wire, and nothing else.
//
// Owning work item:
//
//	eka get walkie/ts:reconnect-resume
//
// # Why a local seam rather than *websocket.Conn
//
// The loop's job — dial, handshake, watch, retry — is identical whatever the
// bytes ride on, and ts:test-harness requires that every reconnect behaviour
// be verified through the injected network and clock. Depending on the
// WebSocket type directly would make the loop untestable without an HTTP
// stack per simulated client (twenty of them in the herd test). The seam is
// three methods on purpose; [WebSocketDialer] is the production
// implementation, and tests substitute framed sessions over testnet links.
// This mirrors [Sender] in heartbeat.go: the policy owns the logic, the wire
// adapts to it.
//
// # Contract
//
//   - Send writes one envelope; an error means the transport FAILED (the
//     frame was not handed off). Silence proves nothing — see Sender.
//   - Recv blocks until the next envelope arrives or the transport fails;
//     after an error the session is dead and Close is optional but allowed.
//   - Close tears the transport down and unblocks a parked Recv with an
//     error. Idempotent.
//
// Exactly ONE goroutine may call Recv at a time (the client's read loop);
// Send is safe for concurrent use.
type Session interface {
	Send(env *walkiev1.Envelope) error
	Recv() (*walkiev1.Envelope, error)
	Close() error
}

// DialFunc opens one fresh Session. It must return a NEW transport each
// call — a reconnect dials anew, never reuses a corpse — and it must honour
// ctx cancellation (a Stop during a dial must end the dial).
//
// An error means the coordinator was unreachable at this instant; the loop
// records dial-failed and re-arms backoff. Errors are ordinary here, not
// exceptional: during an outage this function is most of what the client
// does.
type DialFunc func(ctx context.Context) (Session, error)

// WebSocketDialer is the production [DialFunc]: the control-plane WebSocket
// of adr:003-wire-protocol, one per client, over whatever HTTP transport the
// caller supplies.
//
// # The injected http.Client is the tailnet seam
//
// Production wiring passes an *http.Client whose Transport dials through
// tsnet so the connection lands on the coordinator's tailnet-only listener
// (adr:001: the tailnet is the substrate; there is no public endpoint). A nil
// client uses http.DefaultClient — right for loopback testing, wrong for a
// real deployment, which is why the field doc says so plainly. Tests pass a
// Transport whose DialContext returns a testnet link endpoint, which is how
// the end-to-end suite runs the REAL server and the REAL adapter over the
// injected network with zero production-code special cases.
type WebSocketDialer struct {
	url        string
	httpClient *http.Client
}

// NewWebSocketDialer returns a DialFunc for the control-plane endpoint url.
// httpClient may be nil, which selects http.DefaultClient; production wiring
// MUST pass one whose transport dials via tsnet (see the type comment).
func NewWebSocketDialer(url string, httpClient *http.Client) *WebSocketDialer {
	return &WebSocketDialer{url: url, httpClient: httpClient}
}

// Dial performs the WebSocket handshake and returns the session.
func (d *WebSocketDialer) Dial(ctx context.Context) (Session, error) {
	hc := d.httpClient
	if hc == nil {
		hc = http.DefaultClient
	}
	ws, _, err := websocket.Dial(ctx, d.url, &websocket.DialOptions{HTTPClient: hc})
	if err != nil {
		return nil, fmt.Errorf("control: dial %s: %w", d.url, err)
	}
	return &wsSession{ws: ws}, nil
}

// wsSession adapts *websocket.Conn onto [Session].
//
// coder/websocket permits one concurrent reader and one concurrent writer
// per connection; the client's shape honours that by construction — exactly
// one read-loop goroutine calls Recv, and every writer funnels through the
// client's send mutex before reaching Send.
type wsSession struct {
	ws *websocket.Conn
}

// Send marshals and writes one binary frame (adr:003: the control plane
// speaks binary protobuf envelopes).
func (s *wsSession) Send(env *walkiev1.Envelope) error {
	if env == nil {
		return nil
	}
	data, err := marshalEnvelope(env)
	if err != nil {
		return fmt.Errorf("control: ws send: %w", err)
	}
	ctx := context.Background() // lifetime governed by Close, not a request scope
	return s.ws.Write(ctx, websocket.MessageBinary, data)
}

// Recv reads the next binary frame and decodes it. A text frame is a broken
// peer speaking the wrong protocol — refused, same rule as the server's read
// loop, rather than guessed at.
func (s *wsSession) Recv() (*walkiev1.Envelope, error) {
	ctx := context.Background()
	typ, r, err := s.ws.Reader(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("control: ws recv: non-binary frame on control connection")
	}
	data, err := io.ReadAll(io.LimitReader(r, maxClientEnvelopeBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxClientEnvelopeBytes {
		return nil, fmt.Errorf("control: ws recv: envelope exceeds size cap (%d bytes)", len(data))
	}
	return unmarshalEnvelope(data)
}

// Close tears the transport down IMMEDIATELY (CloseNow): no polite close
// handshake, no waiting for the peer's echo. Asymmetry with the server is
// deliberate — the coordinator performs the graceful GoingAway direction on
// ITS shutdown, while a client teardown typically happens BECAUSE the peer
// is dead or unreachable; blocking a teardown on a dead peer's close-frame
// echo stalls the supervisor for the WebSocket layer's full internal
// timeout exactly when promptness matters most. Idempotent at the
// transport level (a second Close on a closed conn errors informationally).
func (s *wsSession) Close() error {
	s.ws.CloseNow()
	return nil
}

// maxClientEnvelopeBytes bounds one inbound control-plane frame, mirroring
// the coordinator's own cap: the largest legitimate payload today is a 1 MiB
// attachment chunk plus envelope overhead. A peer sending more is broken or
// hostile; either way the frame is refused, not absorbed.
const maxClientEnvelopeBytes = 4 << 20

// marshalEnvelope / unmarshalEnvelope are the one place this package touches
// protobuf codec errors, so every call site gets the same wrapping and no
// error message says only "proto:".
func marshalEnvelope(env *walkiev1.Envelope) ([]byte, error) {
	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("envelope did not encode: %w", err)
	}
	return data, nil
}

func unmarshalEnvelope(data []byte) (*walkiev1.Envelope, error) {
	var env walkiev1.Envelope
	if err := proto.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("envelope did not decode: %w", err)
	}
	return &env, nil
}
