package control

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/testnet"
	"google.golang.org/protobuf/proto"
)

// Test harness for the wire half: a framed session over any net.Conn (so
// testnet links carry real envelope bytes), and a scripted coordinator that
// answers the protocol the way handleHello/dispatch do — HelloAck plus queue
// suffix, heartbeat echoes, nothing else — without dragging an HTTP stack
// into every simulated client.
//
// The framing is length-prefixed protobuf: 4-byte big-endian length, then
// the encoded Envelope. It exists ONLY in tests; production speaks WebSocket
// (session.go), and the end-to-end suite proves that adapter against the
// real coordinator separately.

const frameHeaderBytes = 4

func writeFrame(conn net.Conn, env *walkiev1.Envelope) error {
	data, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("harness: marshal: %w", err)
	}
	var head [frameHeaderBytes]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(data)))
	if _, err := conn.Write(head[:]); err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

func readFrame(conn net.Conn) (*walkiev1.Envelope, error) {
	var head [frameHeaderBytes]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n > maxClientEnvelopeBytes {
		return nil, fmt.Errorf("harness: frame of %d bytes exceeds cap", n)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	env, err := unmarshalEnvelope(data)
	if err != nil {
		return nil, err
	}
	return env, nil
}

// framedSession is the test [Session]: envelopes over a length-prefixed
// net.Conn. One reader, one writer — the same discipline wsSession documents.
type framedSession struct {
	conn net.Conn
	once sync.Once
}

func newFramedSession(conn net.Conn) *framedSession {
	return &framedSession{conn: conn}
}

func (s *framedSession) Send(env *walkiev1.Envelope) error {
	return writeFrame(s.conn, env)
}

func (s *framedSession) Recv() (*walkiev1.Envelope, error) {
	return readFrame(s.conn)
}

func (s *framedSession) Close() error {
	s.once.Do(func() { _ = s.conn.Close() })
	return nil
}

// peerBehavior decides a scripted coordinator's reply to one inbound
// envelope. The returned slice is written back in order; nil means silence
// (the frame is consumed, nothing comes back).
type peerBehavior func(env *walkiev1.Envelope) []*walkiev1.Envelope

// echoBehavior is the standard scripted coordinator: Hello gets an honest
// HelloAck (echoing the resolved device name), everything else is echoed
// verbatim. The echo is what keeps long scenarios alive — a client's
// watchdog counts ANY inbound frame as life, so each beat coming back is a
// pong for scenario purposes.
func echoBehavior(device string) peerBehavior {
	return func(env *walkiev1.Envelope) []*walkiev1.Envelope {
		switch env.GetPayload().(type) {
		case *walkiev1.Envelope_Hello:
			return []*walkiev1.Envelope{helloAckEnvelope(device)}
		default:
			return []*walkiev1.Envelope{env}
		}
	}
}

func helloAckEnvelope(device string) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_HelloAck{HelloAck: &walkiev1.HelloAck{
			Device:          device,
			ProtocolVersion: walkiev1.MaxProtocolVersion,
		}},
	}
}

// fakeCoordinator accepts freshly dialed server-side conns and runs
// behavior on each, recording what crosses its side of the wire. killConns
// models a coordinator crash: every live connection dies at once (the RST
// analogue — no close frames), while new dials keep being accepted, exactly
// like a restarted process on the same tailnet address.
//
// The observation surface (hellos, acks, sentLog) is what criterion 4's
// wire-counting asserts against: claims about "what crossed the wire" are
// made against a complete record, never against what a helper happened to
// pull. Generation numbers stamp the log so post-reconnect traffic can be
// isolated deterministically.
type fakeCoordinator struct {
	device   string
	behavior peerBehavior

	mu      sync.Mutex
	accept  chan net.Conn
	live    map[net.Conn]struct{}
	gen     int
	hellos  []uint64 // Hello.last_acked_position values, in arrival order
	acks    []uint64 // QueueAck positions received, in arrival order
	sentLog []sentFrame
}

type sentFrame struct {
	gen  int    // connection generation: bumped by killConns
	pos  uint64 // envelope position (0 for non-queue frames)
	kind string
}

func newFakeCoordinator(device string, behavior peerBehavior) *fakeCoordinator {
	c := &fakeCoordinator{
		device:   device,
		behavior: behavior,
		accept:   make(chan net.Conn, 256),
		live:     make(map[net.Conn]struct{}),
	}
	go c.acceptLoop()
	return c
}

func (c *fakeCoordinator) acceptLoop() {
	for conn := range c.accept {
		c.mu.Lock()
		c.live[conn] = struct{}{}
		c.mu.Unlock()
		go c.serve(conn)
	}
}

func (c *fakeCoordinator) serve(conn net.Conn) {
	defer func() {
		c.mu.Lock()
		delete(c.live, conn)
		c.mu.Unlock()
	}()
	for {
		env, err := readFrame(conn)
		if err != nil {
			return
		}
		switch p := env.GetPayload().(type) {
		case *walkiev1.Envelope_Hello:
			c.mu.Lock()
			c.hellos = append(c.hellos, p.Hello.GetLastAckedPosition())
			c.mu.Unlock()
		case *walkiev1.Envelope_QueueAck:
			c.mu.Lock()
			c.acks = append(c.acks, p.QueueAck.GetAcknowledgedPosition())
			c.mu.Unlock()
		}
		for _, reply := range c.behavior(env) {
			if err := writeFrame(conn, reply); err != nil {
				return
			}
			c.mu.Lock()
			c.sentLog = append(c.sentLog, sentFrame{
				gen:  c.gen,
				pos:  reply.GetPosition(),
				kind: fmt.Sprintf("%T", reply.GetPayload()),
			})
			c.mu.Unlock()
		}
	}
}

// dialInto hands one server-side conn to the acceptor. Used by DialFuncs the
// tests build; always succeeds here because a restarted coordinator listens
// on the same address — reachability failure is modeled by links, not by the
// acceptor.
func (c *fakeCoordinator) dialInto(serverEnd net.Conn) error {
	select {
	case c.accept <- serverEnd:
		return nil
	default:
		return errors.New("harness: coordinator accept queue full")
	}
}

// killConns closes every live server-side connection at once and bumps the
// generation. This is the coordinator-crash event: clients observe it as an
// abrupt transport death on their next read or write, with no close frames —
// the same signature Fleet.DropAll produces and criterion 1 names.
func (c *fakeCoordinator) killConns() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	for conn := range c.live {
		_ = conn.Close()
	}
	c.live = make(map[net.Conn]struct{})
}

func (c *fakeCoordinator) helloPositions() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.hellos...)
}

func (c *fakeCoordinator) ackPositions() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.acks...)
}

// sentAfter returns the frames written to clients at generation >= gen.
func (c *fakeCoordinator) sentAfter(gen int) []sentFrame {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []sentFrame
	for _, f := range c.sentLog {
		if f.gen >= gen {
			out = append(out, f)
		}
	}
	return out
}

// dialThrough builds a DialFunc over ONE persistent link: every attempt gets
// a fresh pipe pair governed by that link's conditions, which is how
// partition and latency persist across reconnect attempts. The scripted
// coordinator takes the server end of each pair.
func dialThrough(link *testnet.Link, coord *fakeCoordinator) DialFunc {
	return func(context.Context) (Session, error) {
		clientEnd, serverEnd := link.Pipe()
		if err := coord.dialInto(serverEnd); err != nil {
			return nil, err
		}
		return newFramedSession(clientEnd), nil
	}
}

// yieldTo lets every currently-runnable goroutine take turns on the
// processor. One Gosched is not enough on a throttled or contended box: the
// runtime may keep re-electing the driver while code under test sits
// runnable, which shows up as fake-time racing ahead of the very goroutines
// the test is driving. A bounded burst of yields makes "wait for the system
// to settle" mean what it says without introducing any real-time sleep.
func yieldTo() {
	for range 16 {
		runtime.Gosched()
	}
}

// drive advances the clock in fixed steps until cond holds or the step
// budget runs out, failing the test loudly on exhaustion. Everything the
// clients wait on is armed on this clock, so advancing IS the passage of
// time; between steps the woken goroutines need their turns on the
// processor — the same discipline Fleet.AdvanceUntilQuiescent applies — so
// each iteration yields before re-checking.
//
// A budget exhaustion is a bug in the code under test or the scenario — it
// must look like one, not hang.
func drive(t *testing.T, clk *clock.Fake, step time.Duration, maxSteps int, cond func() bool) {
	t.Helper()
	for i := 0; i < maxSteps; i++ {
		if cond() {
			return
		}
		clk.Advance(step)
		yieldTo()
	}
	t.Fatalf("scenario did not settle within %d steps of %v (clock at %v)", maxSteps, step, clk.Now())
}

// collectChanges drains sub's change stream into dst until closeC closes or
// the stream ends. Runs as its own goroutine; reads of dst are guarded by
// the caller's mutex discipline (each collector owns its slice behind a
// mutex the assertions take).
type changeCollector struct {
	mu     sync.Mutex
	events []Change
	done   chan struct{}
}

func newChangeCollector(sub *Subscription) *changeCollector {
	cc := &changeCollector{done: make(chan struct{})}
	go func() {
		defer close(cc.done)
		for ch := range sub.C() {
			cc.mu.Lock()
			cc.events = append(cc.events, ch)
			cc.mu.Unlock()
		}
	}()
	return cc
}

func (cc *changeCollector) snapshot() []Change {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return append([]Change(nil), cc.events...)
}

// onlineTimes extracts when the machine reached StateOnline from a change
// history.
func onlineTimes(events []Change) []time.Time {
	var out []time.Time
	for _, ch := range events {
		if ch.To == StateOnline {
			out = append(out, ch.At)
		}
	}
	return out
}

// maxClientsWithin reports the largest number of timestamps inside any single
// window — the "one spike" metric criterion 2 asserts against. Inherited
// shape from internal/testnet's fleet_test.go, where the same metric is
// proven to detect a total herd under zero jitter.
func maxClientsWithin(sorted []time.Time, window time.Duration) int {
	best := 0
	for j := range sorted {
		n := 0
		for k := range sorted {
			if !sorted[k].Before(sorted[j]) && sorted[k].Before(sorted[j].Add(window)) {
				n++
			}
		}
		best = max(best, n)
	}
	return best
}
