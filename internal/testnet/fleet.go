package testnet

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// Fleet simulates walkie's deployment upper bound — twenty client-shaped
// endpoints against one server-shaped endpoint, all on one *clock.Fake — in a
// single process.
//
// Owning work item:
//
//	eka get walkie/ts:test-harness   (criterion 5)
//
// # The call site it is designed for
//
// ts:reconnect-resume must prove that "twenty simulated clients reconnecting
// after a coordinator restart do not cluster into one spike". That test needs
// four things at once: twenty independent endpoints, one shared injected clock,
// per-client reconnect schedules that goroutine scheduling cannot perturb, and
// an aggregate view of when everyone reconnected. A Fleet provides all four;
// the reconnect policy itself stays with the consumer, which is why the event
// handler is a parameter and not a built-in loop.
//
// # Why the concurrency discipline is the fleet's, not the test's
//
// The brief for criterion 5 carries a deadlock warning that shaped this whole
// type: net.Pipe is synchronous and unbuffered, and *clock.Fake blocks Sleep
// until someone advances. Twenty client goroutines parked on a clock nobody
// advances is not a slow test, it is a hung one. A bare Advance loop does not
// fix that either — it introduces the opposite failure, where the driver
// declares quiescence while a woken goroutine has not yet run its handler or
// armed its next timer, and the test asserts on a simulation that is still
// mid-flight.
//
// So the fleet owns the discipline. Every client runs a reactor goroutine that
// lives from [Fleet.Start] to [Fleet.Shutdown], receiving scheduled events off
// its own channel and parking on the shared clock until each event's absolute
// deadline ([clock.Fake.Until] registers the deadline atomically, so the fire
// time is correct no matter when the reactor gets scheduled).
//
// Outstanding work is tracked by one atomic counter, pending: an event adds
// one before it is offered to its client's channel — never after the send
// lands, or a driver could read pending == 0 while an event sits queued but
// uncounted and declare quiescence mid-flight. Receiving moves the event from
// queued to open turn (no change — still one unit of work), and finishing its
// handler removes one. Whatever the interleaving,
//
//	pending == 0  ⇔  every scheduled event has been fully handled
//
// That single counter is what makes quiescence exact rather than
// probabilistic — and it lets the driver check it without touching the fleet
// mutex, which matters more than it sounds: an earlier revision had the driver
// scan state under the lock in a hot loop, and mutex barging let the driver
// starve reactors out of their own lock for the entire step budget. The
// driver now contends only with parkers on the clock, never with handlers.
//
// [Fleet.AdvanceUntilQuiescent] advances while reactors are parked on the
// clock and yields the processor while they are mid-transition, failing the
// test loudly if the budget runs out. A simulation that never settles is a bug
// in the scenario, and it must look like one instead of hanging or passing
// silently.
//
// # Determinism by construction
//
// Each client gets its own rand source seeded as PCG(seed, client index), so
// client i's schedule is a pure function of (seed, i). Concurrent clients
// never share a draw sequence, which means interleaving cannot change anyone's
// backoff — the property ts:reconnect-resume's herd assertion leans on. This is
// the DatagramLink rule applied per client instead of per link: a shared source
// serialises draws and makes schedules depend on send order; independent
// sources make schedules depend on nothing but the seed.
//
// Event timestamps recorded via [Fleet.Log] are the events' scheduled times,
// not processing times, so the aggregate timeline is reproducible even though
// the order in which concurrent handlers append to the log is not. Assert on
// timestamps and counts, not on log order.
//
// # What a Fleet deliberately does not model
//
// Re-establishing sockets. A coordinator restart is simulated as scheduled
// events plus [Fleet.DropAll] on the existing links; a consumer that needs
// fresh pipes per reconnect builds them from [NewLink] directly. The fleet is
// the deployment shape and the clock discipline, not a protocol
// implementation.
//
// Handlers run on their client's reactor goroutine, so different clients'
// handlers may interleave; guard shared state (Log takes the fleet lock) and
// keep handlers free of indefinite blocking — a handler stuck on a pipe write
// with no reader holds its unit of pending work forever, which
// AdvanceUntilQuiescent reports as budget exhaustion rather than leaving
// silent.
type Fleet struct {
	clk     *clock.Fake
	handler atomic.Pointer[func(FleetEvent)]

	n       int32 // reactor count; entered must reach it before quiescence
	entered atomic.Int64
	pending atomic.Int64 // queued events + open turns, across the whole fleet

	mu     sync.Mutex // guards only the event log
	events []FleetEvent

	// Topology, fixed at construction and read-only afterwards, hence lock-free.
	links       []*Link
	clientConns []net.Conn
	serverConns []net.Conn
	clients     []*fleetClient

	quit     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// clientQueueCapacity bounds how many events may pile up for one client.
// Self-chaining simulations hold at most one outstanding event per client;
// the slack exists for tests that pre-schedule several phases. Overflow is a
// scenario bug and panics rather than dropping silently.
const clientQueueCapacity = 16

type fleetClient struct {
	events chan FleetEvent
	rng    *rand.Rand
}

// FleetEvent is one thing that happened in a simulation: which client, what
// kind, and at what fake time. Kind strings are chosen by the test that
// schedules the events; the fleet attaches no meaning to them.
type FleetEvent struct {
	At     time.Time
	Client int
	Kind   string
}

// NewFleet returns a fleet of n clients against one server endpoint, all
// governed by clk. Each client gets its own stream [Link] — walkie's control
// plane is one WebSocket per client to the coordinator (arc:system-overview) —
// and its own jitter source derived deterministically from seed; see the
// determinism note on [Fleet].
//
// n <= 0 panics: a fleet of zero clients simulates nothing, and a test that
// passes against an empty fleet proves even less than it appears to.
func NewFleet(clk *clock.Fake, seed uint64, n int) *Fleet {
	if n <= 0 {
		panic("testnet: NewFleet requires at least one client")
	}
	f := &Fleet{
		clk:         clk,
		quit:        make(chan struct{}),
		n:           int32(n),
		links:       make([]*Link, n),
		clientConns: make([]net.Conn, n),
		serverConns: make([]net.Conn, n),
		clients:     make([]*fleetClient, n),
	}
	for i := range n {
		link := NewLink(clk, Conditions{})
		clientSide, serverSide := link.Pipe()
		// Per-client source, never a shared or global one: independent PCG
		// streams are what make client i's schedule a pure function of
		// (seed, i) no matter how the goroutines interleave.
		f.clients[i] = &fleetClient{
			events: make(chan FleetEvent, clientQueueCapacity),
			rng:    rand.New(rand.NewPCG(seed, uint64(i))),
		}
		f.links[i] = link
		f.clientConns[i] = clientSide
		f.serverConns[i] = serverSide
	}
	return f
}

// Size reports the number of clients in the fleet.
func (f *Fleet) Size() int { return len(f.clients) }

// Link returns client i's stream link, for latency injection and partition
// control on that one client's connection.
func (f *Fleet) Link(i int) *Link { return f.links[i] }

// ClientConn returns the client-side end of client i's connection.
func (f *Fleet) ClientConn(i int) net.Conn { return f.clientConns[i] }

// ServerConn returns the server-side end of client i's connection — the
// coordinator's end, for tests that stand up server-side readers or writers.
func (f *Fleet) ServerConn(i int) net.Conn { return f.serverConns[i] }

// PartitionAll severs every client's link at once, as a tailnet-wide outage
// would. Individual clients can be isolated via [Fleet.Link].
func (f *Fleet) PartitionAll() {
	for _, l := range f.links {
		l.Partition()
	}
}

// HealAll restores every client's link.
func (f *Fleet) HealAll() {
	for _, l := range f.links {
		l.Heal()
	}
}

// DropAll closes the server end of every client's connection — the effect a
// coordinator restart has on established WebSockets. Clients observe this as
// io.ErrClosedPipe on their next I/O, which is how production reconnect logic
// detects the same event; simulated clients react by scheduling their next
// event via [Fleet.Schedule].
func (f *Fleet) DropAll() {
	for _, c := range f.serverConns {
		_ = c.Close()
	}
}

// Rand returns client i's jitter source. It is deliberately not mutex-guarded:
// rand.Rand draws are meant to stay confined to client i's reactor goroutine,
// which is what keeps client i's schedule a pure function of (seed, i).
// Sharing it across goroutines is a data race the race detector will catch.
func (f *Fleet) Rand(i int) *rand.Rand { return f.clients[i].rng }

// Schedule queues an event for client i: handler(kind) will run at fake time
// at, after any event already queued for that client. Events scheduled before
// [Fleet.Start] wait in the queue until Start launches the reactors.
//
// One limitation worth knowing: Schedule does not wake a client that is
// already parked waiting for an earlier event. An event landing mid-park is
// processed when the park completes — late relative to its own deadline, but
// still logged at that deadline by [Fleet.Log]. Self-chaining simulations —
// the designed usage, where each handler schedules the client's own next
// event — never hit this, because a client's park is always for its own
// earliest event.
//
// Panics if more than clientQueueCapacity events are outstanding for one
// client: a scenario that piles up that much work for a single simulated
// device has lost the plot, and silently stretching the queue would hide it.
func (f *Fleet) Schedule(i int, at time.Time, kind string) {
	ev := FleetEvent{At: at, Client: i, Kind: kind}
	select {
	case <-f.quit:
		return // torn down; the event dies with the simulation
	default:
	}
	// Count before offering: incrementing only once the send has landed
	// leaves a window in which the event is queued but uncounted, and a
	// driver reading pending == 0 inside that window stops the simulation
	// with work still outstanding. The deferred decrement covers the
	// overflow path, so the panic below cannot leak a phantom unit of work.
	f.pending.Add(1)
	queued := false
	defer func() {
		if !queued {
			f.pending.Add(-1)
		}
	}()
	select {
	case f.clients[i].events <- ev:
		queued = true
	default:
		panic(fmt.Sprintf("testnet: client %d has more than %d queued events — bound the scenario", i, clientQueueCapacity))
	}
}

// Start sets the event handler and launches one reactor goroutine per client.
// Calling Start twice panics: a mid-simulation handler swap would make
// behaviour depend on when the swap happened, which is exactly the
// nondeterminism this package exists to prevent.
func (f *Fleet) Start(handler func(FleetEvent)) {
	if handler == nil {
		panic("testnet: Fleet.Start requires a handler")
	}
	if !f.handler.CompareAndSwap(nil, &handler) {
		panic("testnet: Fleet.Start called twice")
	}
	for i := range f.clients {
		f.wg.Add(1)
		go f.run(i)
	}
}

// run is one client's reactor. It lives until Shutdown, cycling between its
// event channel and the shared clock; an idle reactor holds no accounting
// state, so "what could still happen" is always visible in pending.
func (f *Fleet) run(i int) {
	defer f.wg.Done()
	f.entered.Add(1)

	c := f.clients[i]
	for {
		var ev FleetEvent
		select {
		case ev = <-c.events:
		case <-f.quit:
			return
		}

		// Receiving moves the event from queued to open turn: still one unit
		// of pending work, no counter change. The matching decrement happens
		// when the turn completes below.

		// Park until the event's absolute deadline. Until registers the
		// deadline atomically under the clock lock — computing a remaining
		// duration and waiting for it would span two acquisitions, and on a
		// shared clock the driver can Advance in between, placing the waiter
		// at a wrong absolute time. The quit branch exists so Shutdown can
		// reclaim reactors parked far in the future; the abandoned waiter
		// fires into its buffer unread and is collected.
		tick := f.clk.Until(ev.At)
		select {
		case <-tick:
		case <-f.quit:
			f.pending.Add(-1)
			return
		}

		// Outside locks: the handler may Schedule (touching only atomics and
		// its client's channel) and may block on pipe I/O. Fires are
		// delivered by Advance after it releases the clock lock, so a handler
		// that rearms cannot deadlock the driver — the property internal/clock
		// pins with its own test.
		if h := f.handler.Load(); h != nil {
			(*h)(ev)
		}
		f.pending.Add(-1)
	}
}

// Log appends ev verbatim, keeping its scheduled At rather than stamping the
// processing time. Handlers that must produce a reproducible timeline log this
// way: processing time drifts with driver step alignment, scheduled time does
// not.
func (f *Fleet) Log(ev FleetEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
}

// Events returns a snapshot of the log. Order reflects when concurrent
// handlers happened to append, not when events were due; compare timestamps
// and counts, not positions.
func (f *Fleet) Events() []FleetEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]FleetEvent, len(f.events))
	copy(out, f.events)
	return out
}

// AdvanceUntilQuiescent drives the clock forward until the simulation has no
// outstanding work — every scheduled event fully handled — or fails the test
// via tb.Fatalf once maxSteps clock advances have been taken without getting
// there.
//
// Each iteration advances while reactors are parked on the clock (their fires
// are what make progress) and yields the processor otherwise, because pending
// work with nobody parked means a reactor is between channel and clock and
// needs its turn on a processor. This is a progress guarantee, not
// correctness-by-luck: outcomes are interleaving-independent because deadlines
// are absolute (Until) and the log records scheduled times (Log); the yield
// only makes sure the budget is spent on the scenario rather than on starving
// it.
//
// Exhaustion is always a bug in the scenario — a handler that never stops
// rescheduling, a pipe write with no reader — and the error names the
// outstanding work so the scenario can be fixed.
func (f *Fleet) AdvanceUntilQuiescent(tb testing.TB, step time.Duration, maxSteps int) {
	if err := f.advanceUntilQuiescent(step, maxSteps); err != nil {
		tb.Fatalf("testnet: %v", err)
	}
}

// maxIdleIterations bounds how many consecutive yield-only iterations the
// driver may take without the clock moving. Healthy transitions between park
// and handler take a handful; five orders of magnitude more means the
// simulation is wedged with work outstanding but nothing parked — a stuck
// handler or an unscheduled reactor — and must fail loudly rather than spin.
const maxIdleIterations = 10000

// advanceUntilQuiescent is the testable core of
// [Fleet.AdvanceUntilQuiescent]: same loop, error instead of Fatalf, so the
// exhaustion path itself can be asserted on without stubbing testing.TB.
//
// maxSteps bounds advances — iterations that move the clock — so the budget
// names real time coverage, not scheduler noise. Yields are bounded separately
// by maxIdleIterations: they cost no fake time, and a scenario that needs
// unbounded yielding is wedged, not slow.
func (f *Fleet) advanceUntilQuiescent(step time.Duration, maxSteps int) error {
	if step <= 0 {
		return errors.New("advance-until-quiescent needs a positive step")
	}
	idle := 0
	for advances := 0; advances < maxSteps; {
		if f.pending.Load() == 0 && f.entered.Load() == int64(f.n) {
			return nil
		}
		if f.clk.Waiters() > 0 {
			f.clk.Advance(step)
			advances++
			idle = 0
			// Yield here too: an advance can wake a reactor whose handler
			// then needs a processor before it re-parks. Without this, a
			// single-P run could keep advancing past a starved reactor.
			runtime.Gosched()
			continue
		}
		idle++
		if idle > maxIdleIterations {
			return fmt.Errorf("fleet stalled with %d units of work outstanding and nothing parked on the clock (%d/%d reactors running) — a handler is stuck or a reactor never started",
				f.pending.Load(), f.entered.Load(), f.n)
		}
		runtime.Gosched()
	}
	return fmt.Errorf("fleet did not quiesce after %d advances of %v (%d units of work outstanding, %d/%d reactors running) — the scenario never settles",
		maxSteps, step, f.pending.Load(), f.entered.Load(), f.n)
}

// Shutdown tears the simulation down: closes every connection so neither
// pipe-blocked handlers nor server-side readers can wedge the stop, then
// signals the reactors and waits for them. Safe to call once; a second call
// would double-close connections, which is why it is not idempotent by design
// — tests tear down exactly once via t.Cleanup.
func (f *Fleet) Shutdown() {
	for _, c := range f.serverConns {
		_ = c.Close()
	}
	for _, c := range f.clientConns {
		_ = c.Close()
	}
	f.stopOnce.Do(func() { close(f.quit) })
	f.wg.Wait()
}
