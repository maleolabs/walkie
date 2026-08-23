package control

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/testnet"
)

// End-to-end proof of the wire half against the REAL coordinator: the
// production WebSocketDialer, the real Server (identity gate, version gate,
// dispatch, heartbeat pong), real envelope bytes over testnet links on the
// fake clock. Everything the loop-level tests prove with scripted sessions
// must also be true here, where the peer is the actual other end.
//
// The rig shape (pipe listener + address-stamped conns + StaticResolver)
// follows internal/coordinator's own presence suite — same patterns, re-declared
// here because those helpers are package-internal to its tests.

// connListener hands the server pre-connected link endpoints from Accept, so
// tests control exactly which simulated tailnet peers exist without real
// sockets. Serve's injected-listener contract makes this a first-class
// production shape.
type connListener struct {
	addr      net.Addr
	conns     chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func newConnListener() *connListener {
	return &connListener{
		// The coordinator's synthetic tailnet address: a specific IP, never
		// a wildcard — mirroring the tailnet-only bind rule even here.
		addr:  &net.TCPAddr{IP: net.IPv4(100, 64, 0, 254), Port: 443},
		conns: make(chan net.Conn, 64),
		done:  make(chan struct{}),
	}
}

func (l *connListener) enqueue(c net.Conn) error {
	select {
	case l.conns <- c:
		return nil
	case <-l.done:
		return net.ErrClosed
	}
}

func (l *connListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *connListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}

func (l *connListener) Addr() net.Addr { return l.addr }

// addrConn stamps a pipe endpoint with explicit tailnet addresses. net.Pipe
// reports "pipe" for both ends, which the identity gate rightly refuses.
type addrConn struct {
	net.Conn
	local, remote net.Addr
}

func (c *addrConn) LocalAddr() net.Addr  { return c.local }
func (c *addrConn) RemoteAddr() net.Addr { return c.remote }

func tailnetAddr(lastByte byte) *net.TCPAddr {
	return &net.TCPAddr{IP: net.IPv4(100, 64, 0, lastByte), Port: 40000 + int(lastByte)}
}

// wsE2ERig is one real coordinator under test plus the dial plumbing the
// client needs to reach it across testnet links.
type wsE2ERig struct {
	fake     *clock.Fake
	resolver *tsauth.StaticResolver
	logger   *slog.Logger
	target   *connListener

	mu       sync.Mutex
	nextIP   byte
	servers  []*coordinator.Server
	listners []*connListener
	cancels  []context.CancelFunc
	ips      map[byte]bool
	live     []*addrConn // every server-side conn handed to a coordinator
}

func startWSE2ERig(t *testing.T, logs *strings.Builder) *wsE2ERig {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, nil))
	rig := &wsE2ERig{
		fake:     clock.NewFake(epoch),
		resolver: tsauth.NewStaticResolver(nil),
		target:   newConnListener(),
		nextIP:   1,
		ips:      map[byte]bool{},
		logger:   logger,
	}
	rig.startServer()
	return rig
}

// startServer stands up one real coordinator generation on a fresh listener
// and points the dial target at it. Restart = stop the old generation and
// call this again; clients redialing land on the new one.
func (r *wsE2ERig) startServer() {
	ln := newConnListener()
	resolver := r.resolver
	srv := coordinator.NewServer(resolver, r.fake, r.logger, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = srv.Serve(ctx, ln)
	}()
	r.mu.Lock()
	r.target = ln
	r.servers = append(r.servers, srv)
	r.listners = append(r.listners, ln)
	r.cancels = append(r.cancels, cancel)
	r.mu.Unlock()
}

// crashAndRestart models a coordinator crash: the listener stops accepting,
// every live server-side connection is severed ABRUPTLY (no close frames —
// the RST analogue), and a fresh generation takes over so redials succeed.
// The server-side ends are closed rather than the clients' so the kill lands
// from the coordinator's direction, exactly like a dead process.
func (r *wsE2ERig) crashAndRestart() {
	r.mu.Lock()
	r.target.Close()
	for _, c := range r.live {
		_ = c.Close()
	}
	r.live = nil
	cancels := r.cancels
	r.cancels = nil
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	r.startServer()
}

func (r *wsE2ERig) close(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ln := range r.listners {
		ln.Close()
	}
	for _, cancel := range r.cancels {
		cancel()
	}
}

// reserveTailnetIP allocates a stable simulated tailnet address for one
// device so the identity gate's WhoIs lookup can be staged for exactly it.
func (r *wsE2ERig) reserveTailnetIP() byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	ip := r.nextIP
	r.nextIP++
	r.ips[ip] = true
	return ip
}

// dialWebSocket is the production DialFunc wired for the rig: fresh link
// pair per attempt, server end into the CURRENT listener, client end under
// a WebSocketDialer whose HTTP transport dials the pipe directly.
func (r *wsE2ERig) dialWebSocket(ip byte) DialFunc {
	return func(ctx context.Context) (Session, error) {
		r.mu.Lock()
		target := r.target
		r.mu.Unlock()

		link := testnet.NewLink(r.fake, testnet.Conditions{})
		clientEnd, serverEnd := link.Pipe()
		serverSide := &addrConn{Conn: serverEnd, local: target.Addr(), remote: tailnetAddr(ip)}
		r.mu.Lock()
		r.live = append(r.live, serverSide)
		attempts := len(r.live)
		r.mu.Unlock()
		_ = attempts
		if err := target.enqueue(serverSide); err != nil {
			return nil, err
		}
		dialer := NewWebSocketDialer("ws://coordinator/v1", &http.Client{
			Transport: &http.Transport{
				DialContext: func(context.Context, string, string) (net.Conn, error) {
					return &addrConn{Conn: clientEnd, local: tailnetAddr(ip), remote: target.Addr()}, nil
				},
			},
		})
		return dialer.Dial(ctx)
	}
}

func TestWebSocketClientConnectsToRealCoordinator(t *testing.T) {
	logs := &strings.Builder{}
	rig := startWSE2ERig(t, logs)
	t.Cleanup(func() { rig.close(t) })

	const device = "laptop.tail-scale.ts.net."
	ip := rig.reserveTailnetIP()
	host := fmt.Sprintf("100.64.0.%d", ip)
	rig.resolver.Set(host, tsauth.Identity{
		LoginName: "laptop@example.com",
		NodeName:  device,
	})

	fake := rig.fake
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	cl, err := NewClient(DefaultConfig(), mach, rig.dialWebSocket(ip), 1, 2, fake, rig.logger)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	// The pong half of the application-level heartbeat, observed directly:
	// the coordinator answers every client Heartbeat with one, and the read
	// loop hands it to the consumer like any inbound frame. Atomic because
	// the handler runs on the read loop while the driver polls.
	var pong atomic.Bool
	cl.OnEnvelope(func(env *walkiev1.Envelope) {
		if env.GetHeartbeat() != nil {
			pong.Store(true)
		}
	})
	cc := newChangeCollector(mach.Subscribe())
	cl.Start()
	t.Cleanup(cl.Stop)

	settled := false
	for i := 0; i < 30_000; i++ {
		if mach.State() == StateOnline {
			settled = true
			break
		}
		fake.Advance(100 * time.Millisecond)
		yieldTo()
	}
	if !settled {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Logf("DEBUG ws stacks:\n%s", buf[:n])
		t.Fatalf("initial connect never settled (state=%s)", mach.State())
	}

	// The identity gate resolved the tailnet identity and the HelloAck
	// echoed it back — the client LEARNED who it is, per adr:004.
	if got := cl.ConnectedAs(); got != device {
		t.Fatalf("ConnectedAs = %q, want resolved identity %q", got, device)
	}

	// Drive across at least one heartbeat period: the beat goes out, the
	// coordinator's dispatch answers it, and the pong lands on the client.
	pongOK := false
	for i := 0; i < 30_000 && !pongOK; i++ {
		fake.Advance(100 * time.Millisecond)
		yieldTo()
		if pong.Load() && mach.State() == StateOnline {
			pongOK = true
		}
	}
	if !pongOK {
		t.Fatal("no heartbeat pong observed; heartbeats went unanswered")
	}
	if mach.State() != StateOnline {
		t.Fatalf("state = %s after pong, want online", mach.State())
	}

	if len(cc.snapshot()) == 0 {
		t.Fatal("no state changes recorded")
	}
}

func TestWebSocketClientReconnectsAfterCoordinatorCrash(t *testing.T) {
	// Criterion 1 against the REAL protocol stack: kill the coordinator
	// (listener closed, connections severed, no goodbyes) and stand up a
	// fresh generation; the client must come back on its own.
	logs := &strings.Builder{}
	rig := startWSE2ERig(t, logs)
	t.Cleanup(func() { rig.close(t) })

	const device = "phone.tail-scale.ts.net."
	ip := rig.reserveTailnetIP()
	host := fmt.Sprintf("100.64.0.%d", ip)
	rig.resolver.Set(host, tsauth.Identity{
		LoginName: "phone@example.com",
		NodeName:  device,
	})

	fake := rig.fake
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	cl, err := NewClient(DefaultConfig(), mach, rig.dialWebSocket(ip), 3, 4, fake, rig.logger)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cc := newChangeCollector(mach.Subscribe())
	cl.Start()
	t.Cleanup(cl.Stop)

	settled := false
	for i := 0; i < 30_000; i++ {
		if mach.State() == StateOnline {
			settled = true
			break
		}
		fake.Advance(100 * time.Millisecond)
		yieldTo()
	}
	if !settled {
		t.Fatalf("initial connect never settled (state=%s)", mach.State())
	}

	rig.crashAndRestart()

	// The death reaches each client via goroutine scheduling, not the clock:
	// WAIT (frozen clock) until the machine has registered it, otherwise the
	// first "still online" check ends this phase before it began.
	runtime_GoschedUntilAll(t, func() bool { return mach.State() == StateDisconnected })

	settled = false
	for i := 0; i < 40_000; i++ {
		if mach.State() == StateOnline {
			settled = true
			break
		}
		fake.Advance(100 * time.Millisecond)
		yieldTo()
	}
	if !settled {
		t.Fatalf("post-crash reconnect never settled (state=%s)", mach.State())
	}
	if mach.State() != StateOnline {
		t.Fatalf("state = %s after coordinator restart, want online without user action", mach.State())
	}

	// Collectors ride their own pump goroutines: catch them up with the
	// machines while the clock is frozen before counting transitions.
	runtime_GoschedUntilAll(t, func() bool {
		n := 0
		for _, ch := range cc.snapshot() {
			if ch.To == StateOnline {
				n++
			}
		}
		return n >= 2
	})
	onlineCount := 0
	for _, ch := range cc.snapshot() {
		if ch.To == StateOnline {
			onlineCount++
		}
	}
	if onlineCount < 2 {
		rig.mu.Lock()
		liveN := len(rig.live)
		srvN := len(rig.servers)
		rig.mu.Unlock()
		t.Logf("DEBUG cc=%+v liveConns=%d servers=%d", cc.snapshot(), liveN, srvN)
		t.Fatalf("%d online transitions, want at least 2 (initial + post-crash) state=%s\nLOG:\n%s",
			onlineCount, mach.State(), logs.String())
	}
}
