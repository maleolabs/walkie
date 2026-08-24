package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator/presence"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/store"
	"github.com/maleolabs/walkie/internal/testnet"
)

// The tests in this file drive presence END-TO-END through the real server:
// real WebSocket handshakes, real envelope bytes, the real dispatch switch —
// over testnet Links on a *clock.Fake. No test sleeps in real time; every
// timing claim is made true by advancing the clock and made OBSERVED by a
// blocking receive whose failure mode is a loud timeout, not a flaky pass.
//
// # The kill signature (criterion 3, the defining test)
//
// A SIGKILLed process announces nothing: no close frame, no goodbye write,
// nothing. The kill is modeled by ABANDONING the client side of a live link —
// the goroutines driving it simply stop, the transport is never closed, not
// one more byte is written. From the coordinator's side that is exactly a
// half-open socket: its Reader parks forever, ConnectionLost never fires, and
// the TTL expiry is the only thing that can end the device's presence.
//
// The complementary shape — a transport that dies ABRUPTLY but OBSERVABLY
// (the RST analogue) — has its own test below: the server's read wakes with
// an error, ConnectionLost records the loss at once, and other devices see
// offline immediately with last-seen at detection time. Observed severance
// and silent death are different facts with different timestamps; both
// behaviours are deliberate wiring.

// e2eTTL is this suite's liveness TTL; only its relationship to Advance
// calls matters.
const e2eTTL = 45 * time.Second

// rigEpoch anchors the rig's fake clock.
var rigEpoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// syncBuffer is a log capture that is safe to share between the server's
// goroutines and the test goroutine. The rig hands one buffer to every
// component's slog handler — server dispatch, tracker, queue, keystore — and
// tests read it back with String while the server is still running; a plain
// bytes.Buffer is not safe for that concurrent use, and -race flags exactly
// that pair (a handleHello Info on a server goroutine vs. a test-side read).
// Every write and every read takes the same mutex, so every test built on the
// rig inherits race-safe capture.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// pipeListener hands the server pre-connected link endpoints from Accept, so
// tests control exactly which simulated tailnet peers exist without real
// sockets. Serve's injected-listener contract (see Server's type comment)
// makes this a first-class production shape, not a parallel path.
type pipeListener struct {
	addr      net.Addr
	conns     chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		// The coordinator's own synthetic tailnet address: a specific IP,
		// never a wildcard — mirroring the tailnet-only bind rule even here.
		addr:  &net.TCPAddr{IP: net.IPv4(100, 64, 0, 254), Port: 443},
		conns: make(chan net.Conn),
		done:  make(chan struct{}),
	}
}

func (l *pipeListener) enqueue(c net.Conn) error {
	select {
	case l.conns <- c:
		return nil
	case <-l.done:
		return net.ErrClosed
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return l.addr }

// addrConn stamps a pipe endpoint with explicit tailnet addresses. net.Pipe
// reports "pipe" for both ends, which remoteAddrOf refuses — and rightly so:
// the identity gate keys on the remote address, so the simulation must carry
// a plausible one rather than special-case the gate.
type addrConn struct {
	net.Conn
	local, remote net.Addr
}

func (c *addrConn) LocalAddr() net.Addr  { return c.local }
func (c *addrConn) RemoteAddr() net.Addr { return c.remote }

func tailnetAddr(lastByte byte) *net.TCPAddr {
	return &net.TCPAddr{IP: net.IPv4(100, 64, 0, lastByte), Port: 40000 + int(lastByte)}
}

// presenceRig is one coordinator under test: store, tracker, server and
// listener on a single fake clock.
type presenceRig struct {
	resolver *tsauth.StaticResolver
	clk      *clock.Fake
	tracker  *presence.Tracker
	// logs is the shared slog sink; syncBuffer because server goroutines
	// write it while tests read it (see the type comment).
	logs *syncBuffer
	ln   *pipeListener

	// st and srv are exposed for suites that extend the rig — the offline
	// queue tests wire a real queue over THIS store (one store per process,
	// as in production) by assigning srv.offline before any client connects,
	// when no connection handler exists to race the write.
	st  *store.Store
	srv *Server

	nextIP byte
}

// startPresenceRig runs one coordinator under test. The optional variadic
// sink wires sto:text-messaging's OfflineSink extension point (routing_test.go
// exercises it); absent, the server runs with nil — today's production shape.
func startPresenceRig(t *testing.T, sink ...OfflineSink) *presenceRig {
	t.Helper()

	clk := clock.NewFake(rigEpoch)
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))

	st, err := store.Open(t.TempDir()+"/coord.db", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tr, err := presence.NewTracker(st, clk, e2eTTL, logger)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}

	ln := newPipeListener()
	resolver := tsauth.NewStaticResolver(nil)

	var offline OfflineSink
	if len(sink) > 0 {
		offline = sink[0]
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvDone := make(chan struct{})
	srv := NewServer(resolver, clk, logger, tr, offline)
	go func() {
		defer close(srvDone)
		if err := srv.Serve(ctx, ln); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	r := &presenceRig{resolver: resolver, clk: clk, tracker: tr, logs: logs, ln: ln, st: st, srv: srv, nextIP: 1}
	t.Cleanup(func() {
		cancel()
		select {
		case <-srvDone:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop within grace")
		}
		tr.Close()
		st.Close()
	})
	return r
}

// testClient is one connected device: a real WebSocket client speaking the
// protocol over a controllable link, with a reader goroutine collecting every
// envelope the server sends.
type testClient struct {
	name string // resolved NodeName == deviceOf(identity)
	ws   *websocket.Conn
	link *testnet.Link
	raw  net.Conn // the raw client-side pipe end, for teardown of killed conns

	envs chan *walkiev1.Envelope
	done chan struct{}

	// roster holds the one-shot snapshot received before the connect-time
	// HelloAck. It is QUERY-ONLY (see snapshotEntry): the snapshot is a read
	// model, deliberately kept out of the ordered event stream below.
	roster []*walkiev1.Envelope

	// helloAck is THIS connection's handshake ack, kept query-only: the
	// offline-queue tests assert on pending_count without re-plumbing the
	// handshake loop.
	helloAck *walkiev1.HelloAck

	// pending holds live events consumed off the wire while a helper was
	// waiting for something else (a heartbeat's sync ack); nextEnvelope
	// serves them before reading further, preserving wire order.
	pending []*walkiev1.Envelope

	// wireLog records EVERY envelope this connection received, regardless of
	// which helper consumed it. This is sto:offline-queue criterion 4's
	// instrument: assertions about "what crossed the wire" must be made
	// against a complete record of the wire, not against whatever a
	// nextEnvelope-driven helper happened to pull — a full-replay server bug
	// hides precisely in the frames nobody asked for. Guarded by wireMu
	// because the reader goroutine appends while test assertions read.
	wireMu  sync.Mutex
	wireLog []*walkiev1.Envelope
}

// connectDevice dials a fresh device named name over a fresh link, stages its
// tailnet identity, completes Hello/HelloAck, and leaves its reader running.
//
// The optional lastAcked becomes Hello.last_acked_position — the offline
// queue's resume input (sto:offline-queue). Absent, it is zero: "I have
// nothing yet".
func (r *presenceRig) connectDevice(t *testing.T, name string, lastAcked ...uint64) *testClient {
	t.Helper()

	ip := r.nextIP
	r.nextIP++
	host := fmt.Sprintf("100.64.0.%d", ip)
	r.resolver.Set(host, tsauth.Identity{
		LoginName: fmt.Sprintf("%s@example.com", strings.Split(name, ".")[0]),
		NodeName:  name,
	})

	c := &testClient{
		name: name,
		link: testnet.NewLink(r.clk, testnet.Conditions{}),
		envs: make(chan *walkiev1.Envelope, 128),
		done: make(chan struct{}),
	}
	serverEnd, clientEnd := c.link.Pipe()
	c.raw = clientEnd

	if err := r.ln.enqueue(&addrConn{Conn: serverEnd, local: r.ln.Addr(), remote: tailnetAddr(ip)}); err != nil {
		t.Fatalf("enqueue %s: %v", name, err)
	}

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	ws, _, err := websocket.Dial(dialCtx, "ws://coordinator/v1", &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return &addrConn{Conn: clientEnd, local: tailnetAddr(ip), remote: r.ln.Addr()}, nil
			},
		}},
	})
	if err != nil {
		t.Fatalf("dial %s: %v", name, err)
	}
	c.ws = ws
	t.Cleanup(func() { c.teardown(t) })

	// Reader: every envelope the server sends lands in envs, in order.
	go func() {
		defer close(c.done)
		readCtx, cancelRead := context.WithCancel(context.Background())
		defer cancelRead()
		for {
			typ, rd, err := ws.Reader(readCtx)
			if err != nil {
				return
			}
			if typ != websocket.MessageBinary {
				return
			}
			data, err := io.ReadAll(io.LimitReader(rd, maxEnvelopeBytes+1))
			if err != nil {
				return
			}
			var env walkiev1.Envelope
			if proto.Unmarshal(data, &env) != nil {
				return
			}
			c.wireMu.Lock()
			c.wireLog = append(c.wireLog, &env)
			c.wireMu.Unlock()
			c.envs <- &env
		}
	}()

	// Handshake. Everything before the ack is the roster snapshot.
	hello := helloEnvelope(walkiev1.MaxProtocolVersion)
	if len(lastAcked) > 0 {
		hello.GetHello().LastAckedPosition = lastAcked[0]
	}
	c.send(t, hello)
	for {
		env := c.recvRaw(t)
		if env.GetHelloAck() != nil {
			c.helloAck = env.GetHelloAck()
			break
		}
		c.roster = append(c.roster, env)
	}
	return c
}

// recvRaw reads the next envelope straight off the wire, bypassing pending —
// for helpers that must classify frames themselves.
func (c *testClient) recvRaw(t *testing.T) *walkiev1.Envelope {
	t.Helper()
	select {
	case env := <-c.envs:
		return env
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: timed out waiting for envelope", c.name)
	case <-c.done:
		t.Fatalf("%s: connection closed while waiting for envelope", c.name)
	}
	return nil
}

// awaitHelloAck consumes envelopes until the next HelloAck, keeping anything
// else in arrival order via pending. Reading RAW here is what makes this
// terminate: nextEnvelope serves pending first, so feeding pending from a
// nextEnvelope-driven loop would re-deliver the same envelope forever.
func (c *testClient) awaitHelloAck(t *testing.T) {
	t.Helper()
	for {
		env := c.recvRaw(t)
		if env.GetHelloAck() != nil {
			return
		}
		c.pending = append(c.pending, env)
	}
}

// send writes one envelope to the device's connection.
func (c *testClient) send(t *testing.T, env *walkiev1.Envelope) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ws.Write(ctx, websocket.MessageBinary, mustMarshal(t, env)); err != nil {
		t.Fatalf("%s write: %v", c.name, err)
	}
}

// heartbeat sends a Heartbeat and then a second Hello, waiting for THAT ack.
// (Since ts:reconnect-resume the coordinator answers a Heartbeat with a
// Heartbeat echo — the pong half of the application-level heartbeat — so
// awaitHelloAck parks that echo in pending; the follow-up Hello remains the
// synchronization: dispatch is sequential in the server's read loop, so the
// second ack proves the heartbeat was already dispatched — without this, a
// clock advance could race an unprocessed heartbeat back to life.)
func (c *testClient) heartbeat(t *testing.T) {
	t.Helper()
	c.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}}})
	c.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	c.awaitHelloAck(t)
}

// syncProbe sends a Heartbeat and waits for its echo — the neutral causal
// barrier. Dispatch is sequential per connection, so receiving the echo
// proves the server dispatched the Heartbeat, which proves every envelope it
// wrote BEFORE the probe has landed in the wire record.
//
// Why not a follow-up Hello: on a queue-wired rig a Hello is NOT
// observationally neutral. Resume returns the whole unacked suffix every
// time it is asked (at-least-once; rows leave retention only via QueueAck),
// so a probe Hello carrying a stale last_acked_position is itself a second
// resume request whose duplicate frames follow its own ack — a wire-count
// assertion sampling right after that ack then races them (sighted as
// intermittent 4/5/6-vs-3 counts under load). A Heartbeat asserts nothing,
// drains nothing, and its echo carries the same sequential-dispatch proof.
func (c *testClient) syncProbe(t *testing.T) {
	t.Helper()
	c.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}}})
	for {
		if env := c.nextEnvelope(t); env.GetHeartbeat() != nil {
			return
		}
	}
}

// nextEnvelope returns the next envelope of any kind, failing loudly on
// timeout or connection death instead of hanging. Frames consumed while a
// helper waited for something else are served first, in wire order.
func (c *testClient) nextEnvelope(t *testing.T) *walkiev1.Envelope {
	t.Helper()
	if len(c.pending) > 0 {
		env := c.pending[0]
		c.pending = c.pending[1:]
		return env
	}
	return c.recvRaw(t)
}

// nextPresence returns the next PresenceUpdate, skipping protocol chatter.
func (c *testClient) nextPresence(t *testing.T) *walkiev1.PresenceUpdate {
	t.Helper()
	for {
		env := c.nextEnvelope(t)
		if pu := env.GetPresenceUpdate(); pu != nil {
			return pu
		}
	}
}

// assertSilent fails if any envelope is already queued — the deterministic
// way to say "nothing was emitted": events only enter the stream when the
// server emits them, so an empty buffer now plus causality later is proof.
func (c *testClient) assertSilent(t *testing.T) {
	t.Helper()
	select {
	case env := <-c.envs:
		t.Fatalf("%s: unexpected envelope %T", c.name, env.GetPayload())
	default:
	}
}

// wireDirectMessages snapshots every DirectMessage envelope this connection
// has received so far, in wire order — the criterion-4 measurement surface.
//
// Callers MUST establish causality first (the Hello-probe/awaitHelloAck idiom:
// dispatch is sequential in the server's read loop, so the probe's ack proves
// every earlier server-side write has landed). Without that, a snapshot taken
// mid-drain would undercount frames still in flight and could pass a
// full-replay implementation that simply had not finished replaying yet.
func (c *testClient) wireDirectMessages() []*walkiev1.Envelope {
	c.wireMu.Lock()
	defer c.wireMu.Unlock()
	var out []*walkiev1.Envelope
	for _, env := range c.wireLog {
		if env.GetDirectMessage() != nil {
			out = append(out, env)
		}
	}
	return out
}

// teardown closes the connection politely. Killed connections override this
// with a bare transport close — a polite close would WRITE a frame, which is
// precisely what a SIGKILL never does.
func (c *testClient) teardown(t *testing.T) {
	t.Helper()
	_ = c.ws.Close(websocket.StatusNormalClosure, "")
}

// snapshotEntry finds device in the connect-time roster snapshot.
func snapshotEntry(t *testing.T, c *testClient, device string) *walkiev1.PresenceUpdate {
	t.Helper()
	for _, env := range c.roster {
		pu := env.GetPresenceUpdate()
		if pu != nil && pu.GetDevice() == device {
			return pu
		}
	}
	t.Fatalf("device %q absent from %s's connect-time roster (%d entries)", device, c.name, len(c.roster))
	return nil
}

// TestConnectAppearsOnlineToOtherDevices is criterion 1 end-to-end: laptop is
// already connected when phone joins; laptop receives phone's ONLINE update
// unprompted, and phone's connect-time roster shows both devices.
func TestConnectAppearsOnlineToOtherDevices(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	laptop.assertSilent(t) // alone in the world: nothing streamed

	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")

	got := laptop.nextPresence(t)
	if got.GetDevice() != "phone.tail-scale.ts.net." || got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_ONLINE {
		t.Fatalf("laptop saw %+v, want phone ONLINE", got)
	}

	// Phone's connect-time roster includes itself AND laptop, online.
	self := snapshotEntry(t, phone, "phone.tail-scale.ts.net.")
	if self.GetState() != walkiev1.PresenceState_PRESENCE_STATE_ONLINE {
		t.Fatalf("phone's own roster line = %+v, want ONLINE", self)
	}
	other := snapshotEntry(t, phone, "laptop.tail-scale.ts.net.")
	if other.GetState() != walkiev1.PresenceState_PRESENCE_STATE_ONLINE {
		t.Fatalf("roster shows laptop = %+v, want ONLINE", other)
	}
}

// TestCleanDisconnectBroadcastsOfflineWithLastSeen is criterion 2 end-to-end:
// a polite close produces an immediate OFFLINE broadcast whose last-seen is
// the disconnect moment — proven by advancing the clock between connect and
// close, so the timestamp distinguishes "when it left" from "when it joined".
func TestCleanDisconnectBroadcastsOfflineWithLastSeen(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	rig.clk.Advance(7 * time.Second)
	leftAt := rig.clk.Now()

	phone.teardown(t) // polite close frame — the goodbye a SIGKILL never sends

	got := laptop.nextPresence(t)
	if got.GetDevice() != "phone.tail-scale.ts.net." || got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("laptop saw %+v, want phone OFFLINE", got)
	}
	if !got.GetLastSeen().AsTime().Equal(leftAt) {
		t.Fatalf("last-seen = %v, want disconnect moment %v", got.GetLastSeen().AsTime(), leftAt)
	}
}

// TestKilledDeviceExpiresWithinTTLAnnouncingNothing is THE DEFINING TEST
// (criterion 3), driven through the real server: phone connects, heartbeats,
// then is KILLED — abandoned mid-link with no close frame and no final write.
// The coordinator's Reader parks on the half-open pipe; only the TTL ends the
// device. Laptop must receive OFFLINE within the TTL, with last-seen equal to
// the LAST HEARTBEAT — not the expiry moment, which says when we noticed, not
// when the device was last real.
func TestKilledDeviceExpiresWithinTTLAnnouncingNothing(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")

	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	// A heartbeat after connect, synchronized as processed (see heartbeat).
	rig.clk.Advance(5 * time.Second)
	beatenAt := rig.clk.Now()
	phone.heartbeat(t)

	// THE KILL: abandon the connection. No Close, no CloseNow, no final
	// write — the goroutines driving phone simply cease, which is what
	// SIGKILL does to a process. Nothing observable reaches the server.
	_ = phone

	// Well inside the lease: still online. Proven by causality, not sleep —
	// tablet's ONLINE event can only be queued behind whatever came earlier,
	// so receiving it proves no phone-offline preceded it.
	rig.clk.Advance(e2eTTL - time.Second)
	tablet := rig.connectDevice(t, "tablet.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "tablet.tail-scale.ts.net." {
		t.Fatalf("probe saw %+v, want tablet", got)
	}
	_ = tablet // the probe above is tablet's whole job

	// Cross the deadline — one full TTL after the last heartbeat, having
	// announced nothing.
	rig.clk.Advance(time.Second)

	got := laptop.nextPresence(t)
	if got.GetDevice() != "phone.tail-scale.ts.net." || got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("after TTL: laptop saw %+v, want phone OFFLINE", got)
	}
	if !got.GetLastSeen().AsTime().Equal(beatenAt) {
		t.Fatalf("last-seen = %v, want last heartbeat %v (not the expiry moment)",
			got.GetLastSeen().AsTime(), beatenAt)
	}
}

// TestStatusSurvivesReconnectEndToEnd is criterion 4 through the full stack:
// set over the wire, broadcast to others, kept across a clean disconnect, and
// served back in the roster after the SAME identity reconnects — persistence
// by the store, not client memory.
func TestStatusSurvivesReconnectEndToEnd(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	phone.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_PresenceStatusChange{
		PresenceStatusChange: &walkiev1.PresenceStatusChange{Status: "out climbing"},
	}})
	got := laptop.nextPresence(t)
	if got.GetDevice() != "phone.tail-scale.ts.net." || got.GetStatus() != "out climbing" {
		t.Fatalf("status broadcast = %+v, want phone with status", got)
	}

	// Clean disconnect; the label rides the offline event too.
	phone.teardown(t)
	got = laptop.nextPresence(t)
	if got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE || got.GetStatus() != "out climbing" {
		t.Fatalf("offline broadcast = %+v, want status preserved", got)
	}

	// Same identity reconnects: the roster says who it was and what it wrote.
	phone2 := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." ||
		got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_ONLINE {
		t.Fatalf("reconnect broadcast = %+v, want phone ONLINE", got)
	}
	self := snapshotEntry(t, phone2, "phone.tail-scale.ts.net.")
	if self.GetStatus() != "out climbing" {
		t.Fatalf("roster status after reconnect = %q, want preserved", self.GetStatus())
	}
}

// TestDeviceDoesNotReceiveItsOwnPresenceEvents pins the subject-exclusion
// rule: changes about YOUR device are dropped from your own stream. Phone's
// status change is real (laptop receives it), but phone itself must see
// tablet's arrival next — never an echo of its own state, which would look
// like the server conferring liveness (req:device-presence criterion 5).
func TestDeviceDoesNotReceiveItsOwnPresenceEvents(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	phone.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_PresenceStatusChange{
		PresenceStatusChange: &walkiev1.PresenceStatusChange{Status: "busy"},
	}})
	if got := laptop.nextPresence(t); got.GetStatus() != "busy" {
		t.Fatalf("setup: laptop saw %+v, want phone busy", got)
	}

	tablet := rig.connectDevice(t, "tablet.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "tablet.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want tablet", got)
	}

	// Phone's OWN status change produced it no envelope: the next event in
	// its stream is tablet's arrival.
	if got := phone.nextPresence(t); got.GetDevice() != "tablet.tail-scale.ts.net." {
		t.Fatalf("phone saw %+v, want tablet next (own events must be excluded)", got)
	}
	_ = tablet
}

// TestOversizedStatusRejectedNotTruncated: the receiver-enforced 256-byte
// bound REJECTS (structured ProtocolError, label not applied) rather than
// truncating silently, and the connection survives the rejection so the user
// can try again.
func TestOversizedStatusRejectedNotTruncated(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	tooBig := strings.Repeat("x", presence.MaxStatusBytes+1)
	phone.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_PresenceStatusChange{
		PresenceStatusChange: &walkiev1.PresenceStatusChange{Status: tooBig},
	}})

	var pe *walkiev1.ProtocolError
	for {
		env := phone.nextEnvelope(t)
		if e := env.GetProtocolError(); e != nil {
			pe = e
			break
		}
	}
	if pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED {
		t.Fatalf("rejection code = %v, want MALFORMED", pe.GetCode())
	}
	if !strings.Contains(pe.GetDetail(), "256") {
		t.Fatalf("rejection detail %q should name the bound", pe.GetDetail())
	}
	laptop.assertSilent(t) // rejected means never applied, never broadcast

	// Exactly at the bound is legal, and the full text arrives untruncated.
	exact := strings.Repeat("y", presence.MaxStatusBytes)
	phone.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_PresenceStatusChange{
		PresenceStatusChange: &walkiev1.PresenceStatusChange{Status: exact},
	}})
	got := laptop.nextPresence(t)
	if got.GetStatus() != exact {
		t.Fatalf("broadcast status len = %d, want %d (untruncated)", len(got.GetStatus()), len(exact))
	}
}

// TestHeartbeatRefreshObservablePastOriginalDeadline proves the Heartbeat
// dispatch actually feeds the tracker: phone's connect arms a lease ending at
// connect+TTL; a heartbeat near the end moves the deadline past it. Without
// the wiring, phone would go offline at the ORIGINAL deadline even though it
// kept beating.
func TestHeartbeatRefreshObservablePastOriginalDeadline(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	// Beat just before the original deadline lapses...
	rig.clk.Advance(e2eTTL - time.Second)
	phone.heartbeat(t)

	// ...and cross it. Still online — proven by causality: tablet's event
	// would be queued behind a phone-offline if the refresh had not landed.
	rig.clk.Advance(time.Second)
	rig.connectDevice(t, "tablet.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "tablet.tail-scale.ts.net." {
		t.Fatalf("saw %+v, want tablet (phone must still be online)", got)
	}

	// Then the refreshed lease really expires, one TTL after the beat.
	rig.clk.Advance(e2eTTL - time.Second)
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." ||
		got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("saw %+v, want phone OFFLINE after refreshed TTL", got)
	}
}

// TestObservedSeveranceGoesOfflineImmediately pins the OTHER death shape: the
// transport dies ABRUPTLY but observably — a bare connection abort, no
// WebSocket close frame, no final protocol write, exactly what an RST or a
// NIC reset does. The server's parked read wakes with an error, the handler
// exits, and ConnectionLost records the loss at once: offline immediately,
// last-seen at detection time.
//
// This is also why the defining test above models the kill by ABANDONING the
// connection instead: an abandoned half-open pipe never wakes the server's
// reader, so nothing is observed and only the TTL can end the device.
// Observed severance and silent death are different facts with different
// timestamps; both behaviours are deliberate.
func TestObservedSeveranceGoesOfflineImmediately(t *testing.T) {
	rig := startPresenceRig(t)

	laptop := rig.connectDevice(t, "laptop.tail-scale.ts.net.")
	phone := rig.connectDevice(t, "phone.tail-scale.ts.net.")
	if got := laptop.nextPresence(t); got.GetDevice() != "phone.tail-scale.ts.net." {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}

	rig.clk.Advance(7 * time.Second)
	severedAt := rig.clk.Now()

	// The RST analogue: kill the transport under the WebSocket. No close
	// frame is written (CloseNow bypasses the handshake), but the server's
	// read DOES wake — that is what makes this severance observable.
	phone.raw.Close()

	got := laptop.nextPresence(t)
	if got.GetDevice() != "phone.tail-scale.ts.net." || got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("laptop saw %+v, want phone OFFLINE immediately", got)
	}
	if !got.GetLastSeen().AsTime().Equal(severedAt) {
		t.Fatalf("last-seen = %v, want detection moment %v", got.GetLastSeen().AsTime(), severedAt)
	}
}
