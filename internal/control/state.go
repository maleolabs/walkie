package control

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// State is one named state of the control-plane connection.
//
// ts:reconnect-resume requires the machine to be EXPLICIT: five named states
// with a legal-transition table, not a scattering of booleans. The failure
// mode booleans produce is well known and worth naming here: `reconnecting`
// and `degraded` and `wasOnline` drift out of sync under a race, and every
// consumer then invents its own resolution of which flag wins. One state
// value, one writer (the transition gate below), no drift to have.
type State uint8

const (
	// StateDisconnected means no socket exists. This is also the machine's
	// construction state: before the first dial attempt nothing is known
	// about the coordinator, and "unknown" and "not connected" are the same
	// fact for every consumer.
	StateDisconnected State = iota

	// StateConnecting means a dial attempt is in flight. The socket does not
	// exist yet; if the dial fails the machine returns to disconnected.
	StateConnecting

	// StateHandshaking means the transport is up and the Hello/HelloAck
	// exchange (adr:003-wire-protocol) is in flight. Distinct from connecting
	// because the two phases fail differently — a dial failure means the
	// coordinator is unreachable, a handshake failure means it answered and
	// refused — and criterion 6 makes both visible to the user as what they
	// are.
	StateHandshaking

	// StateOnline means HelloAck arrived: the session is established and
	// queue resumption (Hello.last_acked_position) has been offered.
	StateOnline

	// StateDegraded means the session's socket is still open but the peer
	// has gone silent past the dead-peer interval — the half-open-socket
	// case an application-level heartbeat exists to catch (criterion 3). It
	// is a waypoint, not a resting state: the watchdog tears the connection
	// down deliberately moments later, because a socket nobody can prove
	// alive is not a connection to route messages through.
	StateDegraded
)

// String renders the state for logs, the terminal status line
// (sto:terminal-ui) and the control-socket event stream (ts:control-socket).
// The spellings are wire-stable on purpose: consumers key on them.
func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateHandshaking:
		return "handshaking"
	case StateOnline:
		return "online"
	case StateDegraded:
		return "degraded"
	default:
		return fmt.Sprintf("state(%d)", uint8(s))
	}
}

// legalTransitions is the whole transition law of the connection, in one
// table. Every edge carries its reason for existing; an edge not listed here
// cannot be taken (Transition refuses loudly), which is what makes illegal
// states unrepresentable rather than merely discouraged.
//
// The happy path is the one from the item description:
//
//	disconnected -> connecting -> handshaking -> online -> degraded -> disconnected
//
// The remaining edges are the failure shortcuts, each load-bearing:
//
//   - connecting -> disconnected: the dial failed (coordinator down — the
//     exact event criterion 1 exists for). Backoff schedules the retry; the
//     user sees "disconnected", not a lie about a socket that isn't there.
//   - handshaking -> disconnected: the coordinator answered but the handshake
//     failed or was refused (version gate, ProtocolError, timeout). A refused
//     handshake must not be rendered as "connecting" forever.
//   - online -> disconnected: a transport error or clean close — the
//     partition case. Deliberately DIRECT: no degraded waypoint. Degraded
//     means "silence detected by heartbeat"; a surfaced transport error is
//     already proof of death, and routing it through degraded would make the
//     two failure modes indistinguishable to every consumer — the exact
//     confusion criterion 3 draws the line against.
//
// There is deliberately NO degraded -> online edge. Recovery from a dead
// peer goes through teardown and reconnect, so the machine can never claim
// continuity across a gap it just proved exists. If a future design wants
// in-place recovery, that is a revision to this table and to the tests that
// pin it — not a silent extra edge.
var legalTransitions = map[State]map[State]string{
	StateDisconnected: {
		StateConnecting: "dial attempt started",
	},
	StateConnecting: {
		StateHandshaking:  "transport established",
		StateDisconnected: "dial failed",
	},
	StateHandshaking: {
		StateOnline:       "handshake completed",
		StateDisconnected: "handshake failed",
	},
	StateOnline: {
		StateDegraded:     "peer went silent",
		StateDisconnected: "transport error or close",
	},
	StateDegraded: {
		StateDisconnected: "dead peer torn down",
	},
}

// canTransition reports whether from->to is legal.
func canTransition(from, to State) bool {
	_, ok := legalTransitions[from][to]
	return ok
}

// Change is one completed transition, delivered to subscribers.
//
// It is a statement of what happened, carrying both endpoints and the reason:
// a status line that says only "online" answers "is it working" but not "what
// happened just now", and criterion 6 asks the UI to show the connection's
// behaviour, not just its verdict. At comes from the injected clock so tests
// can order events without real time.
type Change struct {
	From   State
	To     State
	Reason string
	At     time.Time
}

// subscriptionBuffer bounds how far the delivery pump may run ahead of a
// consumer before it blocks. It exists so one slow consumer cannot stall the
// pump goroutine's select loop; it is NOT a drop bound — see Subscribe for
// why nothing here is ever dropped.
const subscriptionBuffer = 16

// Machine is the explicit connection state machine: one current State, one
// transition gate, and a subscriber list that cannot miss a change.
//
// Owning work item:
//
//	eka get walkie/ts:reconnect-resume   (acceptance criterion 6)
//
// # Who drives it
//
// The reconnect supervisor built on this package drives transitions; slice 2
// wires that supervisor to real dials and read loops. Transition is exported
// anyway — the legality table makes misuse loud instead of dangerous — but
// code outside this package calling it directly should expect a review
// question, because a second driver is how two writers recreate the boolean
// drift the machine exists to prevent.
//
// # Illegal transitions fail loudly
//
// An illegal Transition PANICS. That is deliberate and it is the house rule
// for programmer errors (compare testnet.Fleet's queue overflow): the table
// above is small enough to hold in one's head, so reaching an unlisted edge
// means the caller's model of the machine is wrong, and every later
// transition would compound the lie. Failing at the call site with both
// states in the message beats corrupting every downstream consumer. Runtime
// races are different: a same-state Transition is a no-op (returns false),
// because two goroutines concluding "still online" is not a bug.
//
// # Delivery cannot miss (criterion 6)
//
// Subscribers receive EVERY change, in order, with no drops — unlike the
// presence tracker, whose drop policy is licensed by its data being
// self-healing within one TTL. A connection-state change has no refresh that
// would heal a drop: a lost "degraded" could leave a status line wrong for
// hours, which is precisely the "cannot fail to notice" the criterion
// forbids. Each Subscription therefore holds an unbounded queue drained by
// its own pump goroutine; the publisher never blocks and never drops. The
// cost is honest and bounded in practice: transitions are human-rare events,
// and a consumer that stops draining while its client keeps flapping has a
// leak its operator will see in goroutine dumps — visible failure, not
// silent loss.
type Machine struct {
	clk    clock.Clock
	logger *slog.Logger

	mu      sync.Mutex
	cur     State
	subs    map[*Subscription]struct{}
	closed  bool
	closeMu sync.Once
	closedC chan struct{}
}

// NewMachine returns a Machine starting in StateDisconnected, stamping change
// times from clk. clk must not be nil: a wall-clock leak into a package whose
// every test runs on *clock.Fake is exactly the accident internal/clock
// exists to prevent. A nil logger falls back to slog's default.
func NewMachine(clk clock.Clock, logger *slog.Logger) (*Machine, error) {
	if clk == nil {
		return nil, fmt.Errorf("control: new machine: clk must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Machine{
		clk:     clk,
		logger:  logger,
		cur:     StateDisconnected,
		subs:    make(map[*Subscription]struct{}),
		closedC: make(chan struct{}),
	}, nil
}

// State reports the current state. Safe for concurrent use.
func (m *Machine) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur
}

// Transition moves the machine from its current state to `to`, recording
// `reason` on the delivered Change. It reports true when the state changed
// and false when `to` equalled the current state (a no-op, no event).
//
// An illegal edge panics — see "Illegal transitions fail loudly" on the type
// comment. Transitions after Close are refused with an error rather than
// panicking: shutdown races (a watchdog firing mid-Close) are ordinary, not
// defects, and the machine must not turn teardown ordering into a crash.
func (m *Machine) Transition(to State, reason string) (bool, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false, fmt.Errorf("control: transition to %s refused: machine closed", to)
	}
	from := m.cur
	if from == to {
		m.mu.Unlock()
		return false, nil
	}
	if !canTransition(from, to) {
		m.mu.Unlock()
		panic(fmt.Sprintf("control: ILLEGAL transition %s -> %s (reason %q); the legal edges live in legalTransitions — fix the caller, not the table", from, to, reason))
	}

	ch := Change{From: from, To: to, Reason: reason, At: m.clk.Now()}
	m.cur = to

	// Fan out while holding m.mu, mirroring the presence tracker: removal by
	// Close happens under the same lock, so no send can target a removed
	// subscription. Appends land in each subscriber's queue (never blocking,
	// never dropping — see Subscribe), and the pump wakes asynchronously.
	for s := range m.subs {
		s.enqueue(ch)
	}
	m.mu.Unlock()

	m.logger.Info("connection state changed",
		slog.String("from", from.String()),
		slog.String("to", to.String()),
		slog.String("reason", reason),
	)
	return true, nil
}

// Subscribe returns a Subscription receiving every subsequent Change.
//
// Ordering with State(): subscribe FIRST, then read State(). A change landing
// between the two calls then appears in both the channel and the direct read
// — a harmless duplicate, because a Change names its To state absolutely.
// Reading State() first could MISS a change outright, which criterion 6
// forbids. Consumers that want exactly-once semantics should render from the
// stream alone after seeding their view from State().
func (m *Machine) Subscribe() *Subscription {
	s := &Subscription{
		mach: m,
		sig:  make(chan struct{}, 1),
		out:  make(chan Change, subscriptionBuffer),
		done: make(chan struct{}),
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		// Closed machine: hand back an already-finished subscription whose
		// channel closes immediately, matching the presence tracker's shape
		// — late subscribers observe shutdown, they don't hang on it.
		close(s.out)
		close(s.done)
		return s
	}
	m.subs[s] = struct{}{}
	m.mu.Unlock()
	go s.pump()
	return s
}

// Close shuts the machine down: further transitions are refused, and every
// subscription FLUSHES everything already owed to it before its channel
// closes — a consumer ranging over C() sees the complete story and then a
// clean end of stream. Idempotent.
func (m *Machine) Close() {
	m.closeMu.Do(func() {
		m.mu.Lock()
		m.closed = true
		subs := make([]*Subscription, 0, len(m.subs))
		for s := range m.subs {
			subs = append(subs, s)
			delete(m.subs, s)
		}
		m.mu.Unlock()
		close(m.closedC)
		for _, s := range subs {
			s.finish(true)
		}
	})
}

// Subscription receives every Change from the moment of Subscribe until the
// machine or the subscription itself is closed. C is fed by a dedicated pump
// goroutine from an unbounded internal queue — the no-drop contract is on
// Machine, and explains why the queue is unbounded rather than a dropped-
// send buffer.
type Subscription struct {
	mach *Machine

	mu   sync.Mutex
	pend []Change

	sig  chan struct{} // capacity-1 wake signal for the pump
	out  chan Change   // consumer-facing, buffered
	done chan struct{}
	once sync.Once

	// drainOnClose records WHICH kind of close won the once: true when the
	// MACHINE closed (flush everything owed before ending the stream), false
	// when the CONSUMER closed (stop promptly; what it never received, it
	// asked not to). Written before done closes, read after — the channel
	// close is the happens-before edge.
	drainOnClose bool
}

// enqueue files one change and nudges the pump. Called with the machine's
// mutex held (see Transition), which is what makes unsubscribe-vs-send races
// impossible: a subscription removed from the map is never enqueued again.
func (s *Subscription) enqueue(ch Change) {
	s.mu.Lock()
	s.pend = append(s.pend, ch)
	s.mu.Unlock()
	select {
	case s.sig <- struct{}{}:
	default:
	}
}

// finish terminates the pump. drain=true (machine shutdown) flushes every
// owed change into C before it closes; drain=false (consumer's own Close)
// stops promptly and abandons whatever was never picked up. Idempotent via
// once; the first caller's mode wins.
func (s *Subscription) finish(drain bool) {
	s.once.Do(func() {
		s.drainOnClose = drain
		close(s.done)
	})
}

// Close unsubscribes: the pump stops promptly and C closes. Changes already
// sitting in C's buffer remain readable (closing a channel never discards
// its buffered values); changes still queued behind them are dropped — the
// consumer said it was done listening. Idempotent; safe concurrently with
// delivery.
func (s *Subscription) Close() {
	s.mach.mu.Lock()
	_, present := s.mach.subs[s]
	delete(s.mach.subs, s)
	s.mach.mu.Unlock()
	if present {
		s.finish(false)
	}
}

// C receives changes until Close (of the subscription or the machine). The
// channel always closes eventually — consumers may range over it.
func (s *Subscription) C() <-chan Change { return s.out }

// pump drains the pending queue into C, blocking on C when the consumer is
// slow — backpressure lands in the queue's growth, never in a dropped change
// and never in the publisher.
//
// The done branch carries the two close semantics from finish: a consumer
// Close returns immediately (buffered values stay readable), a machine Close
// keeps draining until the queue is empty, because the no-drop contract does
// not lapse at shutdown and a consumer ranging over C() drains as fast as
// the pump pushes.
func (s *Subscription) pump() {
	defer close(s.out)
	for {
		s.mu.Lock()
		pending := s.pend
		s.pend = nil
		s.mu.Unlock()

		for _, ch := range pending {
			select {
			case s.out <- ch:
			case <-s.done:
				if !s.drainOnClose {
					return
				}
				// Machine shutdown flush: the consumer is ranging toward a
				// channel that only this push can close, so the send makes
				// progress by construction.
				s.out <- ch
			}
		}

		select {
		case <-s.sig:
		case <-s.done:
			if !s.drainOnClose {
				return
			}
			// Machine close racing the wake signal: both selects are
			// randomly armed when sig and done are ready at once, so this
			// branch must re-check the queue instead of trusting the wake.
			// After machine close no new enqueues exist (the subscription
			// left the map and Transition refuses), so once the queue reads
			// empty here, it stays empty and exiting loses nothing.
			s.mu.Lock()
			remaining := len(s.pend)
			s.mu.Unlock()
			if remaining == 0 {
				return
			}
		}
	}
}
