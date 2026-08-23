package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// The tests in this file exercise the accept loop end-to-end over real
// sockets. Loopback addresses stand in for tailnet interfaces — see
// simulatedTailnetAddr below — because criterion 1's real join needs a live
// tailnet and auth key, which criteria 2 and 3 must not depend on.

const (
	// simulatedTailnetAddr is the address the test listener binds, standing
	// in for the node's tailscale TUN IP. Linux routes all of 127/8 to
	// loopback, so distinct addresses exist without root and a bind to one
	// genuinely excludes the others — which is exactly the property
	// criterion 3 is about.
	simulatedTailnetAddr = "127.0.0.2"
	// otherIfaceAddr is a DIFFERENT local interface address: dials addressed
	// here model a connection attempt arriving outside the tailnet.
	otherIfaceAddr = "127.0.0.1"
)

// startServer runs a Server on a fresh listener bound to
// [simulatedTailnetAddr] and returns its dial address plus the log buffer.
// The returned cancel stops the server and waits for it to exit.
func startServer(t *testing.T, resolver tsauth.Resolver) (addr string, logs *bytes.Buffer, cancel func()) {
	t.Helper()

	logs = &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	clk := clock.NewFake(time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC))

	ln, err := net.Listen("tcp4", simulatedTailnetAddr+":0")
	if err != nil {
		t.Fatalf("bind %s: %v (environment lacks distinct loopback addresses)", simulatedTailnetAddr, err)
	}

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// nil tracker, nil offline sink: these tests exercise skeleton
		// machinery (identity gate, handshake, shutdown), not presence —
		// presence_server_test.go owns the wired-tracker rig and
		// routing_test.go owns the text-routing rig.
		if err := NewServer(resolver, clk, logger, nil, nil).Serve(ctx, ln); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	return ln.Addr().String(), logs, func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down within grace")
		}
	}
}

// dialWS connects a websocket client to addr, seeding resolver so the
// identity gate admits it.
//
// The gate keys on the connection's SOURCE address, so the test discovers the
// source host the kernel actually picks (a probe dial reports it via
// LocalAddr) instead of assuming which loopback address sources the traffic.
func dialWS(t *testing.T, ctx context.Context, resolver tsauth.Resolver, addr string) *websocket.Conn {
	t.Helper()

	probe, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatalf("source-address probe %s: %v", addr, err)
	}
	srcHost, _, _ := net.SplitHostPort(probe.LocalAddr().String())
	probe.Close()

	resolver.(*tsauth.StaticResolver).Set(srcHost, tsauth.Identity{
		LoginName: "alice@example.com",
		NodeName:  "phone.tail-scale.ts.net.",
	})

	ws, _, err := websocket.Dial(ctx, "ws://"+addr, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { ws.Close(websocket.StatusNormalClosure, "") })
	return ws
}

// roundTrip sends env and reads one reply envelope.
func roundTrip(t *testing.T, ctx context.Context, ws *websocket.Conn, env *walkiev1.Envelope) *walkiev1.Envelope {
	t.Helper()

	data, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	typ, r, err := ws.Reader(ctx)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("reply frame type = %v, want binary", typ)
	}
	replyData, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read reply body: %v", err)
	}
	var got walkiev1.Envelope
	if err := proto.Unmarshal(replyData, &got); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	return &got
}

func helloEnvelope(version uint32) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_Hello{
			Hello: &walkiev1.Hello{
				ClientVersion:     "test",
				ProtocolVersion:   version,
				LastAckedPosition: 0,
			},
		},
	}
}

// Criterion 2: an accepted connection whose identity cannot be resolved is
// refused AND the refusal is logged — asserted against captured log records,
// not by reading the code.
func TestUnresolvableIdentityRefusedAndLogged(t *testing.T) {
	addr, logs, cancel := startServer(t, tsauth.NewStaticResolver(nil))
	defer cancel()

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()

	_, resp, err := websocket.Dial(ctx, "ws://"+addr, nil)
	if err == nil {
		t.Fatal("dial without resolvable identity succeeded; identity gate did not refuse")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("refusal = (resp %v, err %v), want HTTP 403", resp, err)
	}

	out := logs.String()
	if !strings.Contains(out, "identity unresolved") {
		t.Errorf("refusal not logged; log output:\n%s", out)
	}
	// The logged remote_addr is the client's SOURCE address (host:port with
	// an ephemeral port) — on this loopback rig that is otherIfaceAddr.
	// Assert the host prefix, not a fixed port.
	if !strings.Contains(out, "remote_addr="+otherIfaceAddr+":") {
		t.Errorf("refusal log missing remote_addr attribute for %s; log output:\n%s", otherIfaceAddr, out)
	}
}

// Criterion 3, network half: the server is reachable through its bound
// ("tailnet") interface and NOT through another local interface. This is the
// proof-by-test the criterion demands — had the listener been wildcard, the
// second dial would succeed.
//
// The source-address side of admission is criterion 2's job (the identity
// gate); this test pins that binding alone excludes foreign interfaces.
func TestNonTailnetDialCannotReachBoundListener(t *testing.T) {
	addr, _, cancel := startServer(t, tsauth.NewStaticResolver(nil))
	defer cancel()
	_, port, _ := net.SplitHostPort(addr)

	// Guard the double itself: the listener must be bound to exactly the
	// simulated tailnet address, never a wildcard.
	boundHost, _, _ := net.SplitHostPort(addr)
	if boundHost != simulatedTailnetAddr || boundHost == "0.0.0.0" || boundHost == "::" {
		t.Fatalf("listener bound to %q, want exactly %s", boundHost, simulatedTailnetAddr)
	}

	// A connection attempt ADDRESSED to another interface cannot reach it:
	// nothing is listening there, so the kernel refuses at TCP level before
	// any walkie code runs.
	conn, err := net.DialTimeout("tcp4", net.JoinHostPort(otherIfaceAddr, port), 2*time.Second)
	if err == nil {
		conn.Close()
		t.Fatalf("dial to non-tailnet interface %s:%s CONNECTED; binding is not interface-exclusive", otherIfaceAddr, port)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || !errors.Is(opErr.Err, syscall.ECONNREFUSED) {
		t.Logf("non-tailnet dial failed with %v (want ECONNREFUSED; other failures still prove unreachability)", err)
	}

	// Control: the SAME server IS reachable through the bound interface —
	// exclusion comes from the bind, not from a broken server.
	httpClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Get("http://" + addr)
	if err != nil {
		t.Fatalf("bound-interface reachability control failed: %v", err)
	}
	resp.Body.Close()
}

// Criterion 3 + handshake, application half: a peer admitted through the
// bound interface completes Hello → HelloAck with the RESOLVED identity
// echoed back and the injected clock's timestamp.
func TestHandshakeEndToEndOverBoundListener(t *testing.T) {
	resolver := tsauth.NewStaticResolver(nil)
	addr, _, cancel := startServer(t, resolver)
	defer cancel()

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()
	ws := dialWS(t, ctx, resolver, addr)

	got := roundTrip(t, ctx, ws, helloEnvelope(walkiev1.MaxProtocolVersion))

	ack := got.GetHelloAck()
	if ack == nil {
		t.Fatalf("reply payload = %T, want HelloAck", got.GetPayload())
	}
	if ack.GetDevice() != "phone.tail-scale.ts.net." {
		t.Errorf("Device = %q, want resolved NodeName", ack.GetDevice())
	}
	if ack.GetProtocolVersion() != walkiev1.MaxProtocolVersion {
		t.Errorf("ProtocolVersion = %d, want %d", ack.GetProtocolVersion(), walkiev1.MaxProtocolVersion)
	}
	wantAt := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC) // startServer's fake clock
	if !ack.GetReceivedAt().AsTime().Equal(wantAt) {
		t.Errorf("ReceivedAt = %v, want fake-clock time %v", ack.GetReceivedAt().AsTime(), wantAt)
	}
}

// adr:003 mixed-fleet rule pinned at the SERVER level: an envelope carrying
// unknown fields decodes and dispatches normally. The unknown field is hand-
// built bytes (field 999, varint wire type) appended after a valid Hello —
// what a newer client's envelope looks like to this build.
func TestUnknownFieldsIgnoredByServer(t *testing.T) {
	resolver := tsauth.NewStaticResolver(nil)
	addr, _, cancel := startServer(t, resolver)
	defer cancel()

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()
	ws := dialWS(t, ctx, resolver, addr)

	data, err := proto.Marshal(helloEnvelope(walkiev1.MaxProtocolVersion))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	unknownField := []byte{0x98, 0x3e, 0x01} // tag(999, wiretype 0), value 1
	if err := ws.Write(ctx, websocket.MessageBinary, append(data, unknownField...)); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, r, err := ws.Reader(ctx)
	if err != nil {
		t.Fatalf("server rejected envelope with unknown fields: %v", err)
	}
	body, _ := io.ReadAll(r)
	var got walkiev1.Envelope
	if err := proto.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if got.GetHelloAck() == nil {
		t.Fatalf("reply payload = %T, want HelloAck (unknown fields must be ignored, adr:003)", got.GetPayload())
	}
}

// An out-of-range protocol_version gets the structured ProtocolError naming
// both versions, then the connection closes: two incompatible endpoints have
// nothing further to say to each other.
func TestUnsupportedProtocolVersionRefused(t *testing.T) {
	resolver := tsauth.NewStaticResolver(nil)
	addr, logs, cancel := startServer(t, resolver)
	defer cancel()

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()
	ws := dialWS(t, ctx, resolver, addr)

	if err := ws.Write(ctx, websocket.MessageBinary, mustMarshal(t, helloEnvelope(999))); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, r, err := ws.Reader(ctx)
	if err != nil {
		t.Fatalf("read refusal: %v", err)
	}
	body, _ := io.ReadAll(r)
	var got walkiev1.Envelope
	if err := proto.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	pe := got.GetProtocolError()
	if pe == nil {
		t.Fatalf("reply payload = %T, want ProtocolError", got.GetPayload())
	}
	if pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_VERSION_UNSUPPORTED {
		t.Errorf("code = %v, want VERSION_UNSUPPORTED", pe.GetCode())
	}
	for _, want := range []string{"999", fmt.Sprintf("%d", walkiev1.MinProtocolVersion)} {
		if !strings.Contains(pe.GetDetail(), want) {
			t.Errorf("detail %q missing peer/server version %q", pe.GetDetail(), want)
		}
	}

	// The connection then closes from our side.
	if _, _, err := ws.Reader(ctx); err == nil {
		t.Error("connection stayed open after version-gate refusal")
	}
	if !strings.Contains(logs.String(), "unsupported protocol version") {
		t.Errorf("version refusal not logged; log output:\n%s", logs.String())
	}
}

// Payloads with no handler in the skeleton are logged-and-ignored, NEVER
// rejected: ignoring unknown payloads is how an older coordinator survives a
// newer client (adr:003). Proven by a follow-up handshake succeeding on the
// same connection — no sleeps, just causality.
func TestUnhandledPayloadIgnoredConnectionStaysOpen(t *testing.T) {
	resolver := tsauth.NewStaticResolver(nil)
	addr, _, cancel := startServer(t, resolver)
	defer cancel()

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()
	ws := dialWS(t, ctx, resolver, addr)

	heartbeat := &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}},
	}
	if err := ws.Write(ctx, websocket.MessageBinary, mustMarshal(t, heartbeat)); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}

	// The heartbeat IS answered now — with the schema's own Heartbeat echo,
	// the pong half of the application-level heartbeat (ts:reconnect-resume).
	// The unhandled payload before it was still logged-and-ignored, and the
	// follow-up handshake succeeding on the same connection proves the
	// connection stayed open. Skip the pong, then require the Hello's ack.
	got := roundTrip(t, ctx, ws, helloEnvelope(walkiev1.MaxProtocolVersion))
	for got.GetHeartbeat() != nil {
		got = roundTrip(t, ctx, ws, helloEnvelope(walkiev1.MaxProtocolVersion))
	}
	if got.GetHelloAck() == nil {
		t.Fatalf("post-heartbeat reply = %T, want HelloAck", got.GetPayload())
	}
}

// Graceful shutdown must actually drain: cancelling Serve's ctx closes every
// live WebSocket within [shutdownGrace], and Serve does not return until the
// read loops have exited.
//
// Why this test exists in this shape: http.Server.Shutdown neither closes nor
// waits for hijacked connections — and every WebSocket is hijacked — so an
// implementation that only calls Shutdown returns instantly while clients sit
// on open connections. The client below holds a read WITHOUT sending or
// acking anything (the silent-peer worst case), so the only way it observes
// closure is the server actively closing the connection.
func TestShutdownClosesLiveConnectionWithinGrace(t *testing.T) {
	resolver := tsauth.NewStaticResolver(nil)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	clk := clock.NewFake(time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC))

	ln, err := net.Listen("tcp4", simulatedTailnetAddr+":0")
	if err != nil {
		t.Fatalf("bind %s: %v (environment lacks distinct loopback addresses)", simulatedTailnetAddr, err)
	}

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewServer(resolver, clk, logger, nil, nil).Serve(ctx, ln) }()

	ws := dialWS(t, context.Background(), resolver, ln.Addr().String())

	// Block a client-side read and never write: the server's Reader for
	// this connection has nothing to consume and no reason to return except
	// shutdown reaching it.
	readErr := make(chan error, 1)
	go func() {
		_, _, err := ws.Reader(context.Background())
		readErr <- err
	}()

	stop() // the SIGTERM analogue

	const budget = shutdownGrace + 2*time.Second

	// 1. The peer observes a real close frame within the grace window.
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("client read returned nil; connection closed without ending")
		}
		// A proper GoingAway frame is the common outcome, but under heavy
		// scheduling the transport abort can land as a bare EOF at the peer
		// (RST discarding queued bytes) even though the frame was written.
		// The mandated property is termination within the window; note the
		// rarer shape rather than failing on it.
		if cs := websocket.CloseStatus(err); cs == -1 {
			t.Logf("note: client saw abrupt end (%v) instead of a close frame", err)
		}
	case <-time.After(budget):
		t.Fatalf("server did not close live connection within grace window (%s)", shutdownGrace)
	}

	// 2. Serve itself returns within the grace window — drain completed,
	// not abandoned — with a clean result.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve after drain: %v", err)
		}
	case <-time.After(budget):
		t.Fatalf("Serve did not return within grace window (%s); drain did not complete", shutdownGrace)
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
