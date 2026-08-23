package control

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// Sender writes one envelope to the live peer. It is the watchdog's entire
// view of the transport: slice 2 adapts the real WebSocket to it, and tests
// substitute a function — which is what keeps this file free of wire code
// while the detection logic is still fully exercised.
//
// A returned error means the transport FAILED (write refused, link severed).
// Silence is not an error: a write into a half-open socket can succeed into
// the void, which is exactly why outbound success proves nothing and the
// dead-peer deadline below runs on INBOUND evidence instead.
type Sender func(*walkiev1.Envelope) error

// heartbeatEnvelope builds the wire frame. Heartbeat carries no fields by
// schema (ts:protocol-schema-v1): its arrival IS the message, observed
// evidence of life on both ends, and there is deliberately nothing in it a
// client could assert.
func heartbeatEnvelope() *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}},
	}
}

// Watchdog keeps one live session observably alive and detects the peer that
// stopped responding while the socket stayed open (criterion 3).
//
// Owning work item:
//
//	eka get walkie/ts:reconnect-resume
//
// # Why TCP does not do this job
//
// A socket can stay open long after the peer is gone: NAT rebinding, a
// power-cut mid-flight, a kernel holding a half-open connection — none of
// these deliver an error to the surviving end's read or write. The item
// description calls for an application-level heartbeat for exactly this
// reason, and req:device-presence already relies on the same fact server-
// side (the liveness TTL exists because absence of heartbeats is the only
// honest signal a killed device gives).
//
// # The two failure modes, kept distinct
//
// Criterion 3 names one way a session dies; transports provide another, and
// conflating them would hide real information from every consumer of the
// state machine:
//
//   - SILENT PEER ON AN OPEN SOCKET: sends succeed into the void, nothing
//     comes back. Detected when no inbound frame arrives for
//     [Config.DeadPeerInterval]. The machine transitions online -> degraded
//     ("we noticed") -> disconnected ("tearing down deliberately") — both
//     steps observable, per criterion 6, so the user sees the diagnosis and
//     not just the disappearance.
//   - TRANSPORT ERROR / PARTITION: a send fails or the wire layer surfaces
//     an error. Proof of death already in hand; the machine transitions
//     straight to disconnected with NO degraded waypoint. The direct edge is
//     what makes the two modes distinguishable in the event stream.
//
// # What counts as life
//
// ANY inbound frame refreshes the deadline, not only Heartbeat replies —
// presence updates, queued deliveries and text all prove the pipe and the
// peer are alive, and requiring pongs specifically would mark a session
// dead in the middle of a message burst. The wire layer calls [Watchdog.
// Activity] from its read loop; this package owns the policy, slice 2 owns
// the plumbing.
//
// # Concurrency shape
//
// The same discipline as the presence tracker's leases: every observation
// rotates to a FRESH deadline timer with a fresh generation counter, rather
// than resetting one shared timer. Reset-and-fire windows differ subtly
// between real and fake clocks (a fire committed just before a Reset can
// still sit buffered); rotating makes each watcher own exactly one deadline,
// and a stale fire loses the gen check and expires nothing. At one watchdog
// per client session the allocation cost is noise next to the correctness.
type Watchdog struct {
	cfg    Config
	mach   *Machine
	send   Sender
	clk    clock.Clock
	logger *slog.Logger

	mu       sync.Mutex
	gen      uint64      // current silence-deadline generation
	hbTimer  clock.Timer // periodic heartbeat arming
	deadline clock.Timer // the CURRENT silence-deadline timer, for retirement on Stop
	stopped  bool
	quit     chan struct{}
	stopOnce sync.Once

	// nextBeat is the absolute time of the next scheduled beat. Initialised
	// by StartWatchdog under mu, then owned exclusively by the beat loop —
	// anchoring the cadence to ABSOLUTE slots (start + k*period) rather than
	// to whatever moment the loop happens to get scheduled is what keeps the
	// heartbeat period honest on a loaded process: a slow iteration delays
	// one beat, it does not drift the whole schedule.
	nextBeat time.Time

	// beatDone closes when the heartbeat loop has fully exited. Stop cannot
	// join the loop (the loop itself calls Stop on terminal verdicts — a
	// joining Stop would deadlock on its own caller), so tests synchronise
	// on this instead of sleeping: after <-beatDone, no further send can
	// happen, which is what makes "no beats after Stop" assertable without
	// real time.
	beatDone chan struct{}
}

// StartWatchdog validates cfg, then begins heartbeating the peer through
// send and watching for silence, driving mach as verdicts arrive. The caller
// starts it once the machine reaches StateOnline and MUST stop it when the
// session ends — a watchdog whose session died with it still running would
// keep transitioning a machine that has moved on.
//
// The initial silence deadline starts NOW: the transport just proved itself
// during the handshake, so connect time counts as the last observation of
// life.
func StartWatchdog(cfg Config, mach *Machine, send Sender, clk clock.Clock, logger *slog.Logger) (*Watchdog, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("control: start watchdog: %w", err)
	}
	if mach == nil {
		return nil, fmt.Errorf("control: start watchdog: mach must not be nil")
	}
	if send == nil {
		return nil, fmt.Errorf("control: start watchdog: send must not be nil")
	}
	if clk == nil {
		return nil, fmt.Errorf("control: start watchdog: clk must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	w := &Watchdog{
		cfg:      cfg,
		mach:     mach,
		send:     send,
		clk:      clk,
		logger:   logger,
		quit:     make(chan struct{}),
		beatDone: make(chan struct{}),
	}

	// Arm the periodic heartbeat under the lock so a racing Stop cannot
	// strand the first timer, then start the loop.
	w.mu.Lock()
	w.hbTimer = clk.NewTimer(cfg.HeartbeatPeriod)
	w.nextBeat = clk.Now().Add(cfg.HeartbeatPeriod)
	w.armDeadlineLocked()
	w.mu.Unlock()

	go w.beatLoop()
	return w, nil
}

// armDeadlineLocked arms one fresh silence-deadline watcher with a new
// generation. Callers hold w.mu. Fresh-timer-per-deadline, not Reset: see
// the concurrency note on the type comment. The superseded timer is stopped
// HERE rather than left for its watcher to retire lazily — a fleet-scale
// simulation rotates one deadline per inbound frame, and eagerly retiring
// keeps the clock's waiter list proportional to live sessions (its watcher,
// if it later wakes against a committed fire, still loses the gen check and
// expires nothing).
func (w *Watchdog) armDeadlineLocked() {
	w.gen++
	tm := w.clk.NewTimer(w.cfg.DeadPeerInterval)
	if w.deadline != nil {
		w.deadline.Stop()
	}
	w.deadline = tm
	go w.watchDead(w.gen, tm)
}

// Activity records that an inbound frame arrived: the freshest possible
// proof the peer and pipe are alive. Called from the wire layer's read loop;
// safe for concurrent use.
func (w *Watchdog) Activity() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	// Rotate the lease: retire the old watcher (its fire, if committed, will
	// lose the gen check), arm a fresh one at now+interval.
	w.armDeadlineLocked()
	w.mu.Unlock()
}

// SendFailed reports a transport failure surfaced by the wire layer — a
// failed write, a read error, a partition error. This is failure mode (b):
// death already proven, so the machine goes straight to disconnected with no
// degraded waypoint, and the watchdog retires itself.
//
// The verdict is CLAIMED under w.mu (stopped set before any transition): a
// silence deadline firing concurrently is a competing verdict for the same
// death, and exactly one of them may drive the machine — the loser must see
// stopped and retire, or the illegal-edge panic turns a routine race into a
// crash.
func (w *Watchdog) SendFailed(err error) {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return // another verdict (or Stop) already owns this death
	}
	w.stopped = true
	w.mu.Unlock()

	w.logger.Warn("control: transport failure",
		slog.String("reason", err.Error()),
	)
	w.transitionDisconnected(fmt.Sprintf("transport error: %v", err))
}

// transitionDisconnected performs the terminal transition for BOTH failure
// modes' second half and for mode (b) whole. Legality absorbs whichever
// state the machine actually sits in (online->disconnected and
// degraded->disconnected are both legal edges; a same-state call is a
// no-op), so callers name the truth and the table polices the path.
func (w *Watchdog) transitionDisconnected(reason string) {
	if _, err := w.mach.Transition(StateDisconnected, reason); err != nil {
		// Machine already closed: shutdown raced the verdict. Nothing left
		// to drive; the log line above is the record.
		w.logger.Debug("control: terminal transition refused",
			slog.String("reason", err.Error()),
		)
	}
	w.Stop()
}

// beatLoop sends a Heartbeat every HeartbeatPeriod until the watchdog stops
// or a send fails.
//
// The cadence is anchored to ABSOLUTE slots (start + k*period), and each
// iteration arms the NEXT slot's timer BEFORE sending the current beat. The
// ordering is load-bearing, not stylistic: it means once a beat is
// observable (its bytes handed to the wire), the following slot is already
// waiting on the clock. A consumer that can observe sends therefore sees an
// exact cadence — k elapsed periods, k beats — no matter how this goroutine
// competes for scheduling. On the injected clock that property is what makes
// beat counts assertable without sleeps; in production it simply means the
// schedule never drifts on a loaded process.
//
// A committed fire whose slot was skipped (severe lag) still beats once when
// received — an extra heartbeat is always safe; a missing one is not.
func (w *Watchdog) beatLoop() {
	defer close(w.beatDone)
	next := w.nextBeat
	for {
		select {
		case <-w.quit:
			return
		case <-w.hbTimer.C():
		}

		// A fire can already be committed when Stop runs: the fire sits in
		// the timer's buffered channel and this select may pick it even
		// though quit is closed. Re-check stopped before beating, or a
		// retired watchdog would send exactly one posthumous heartbeat.
		w.mu.Lock()
		if w.stopped {
			w.mu.Unlock()
			return
		}
		// Arm the next slot FIRST (see the ordering note above). next is
		// pushed past any slots consumed while this goroutine waited to be
		// scheduled, so the delay below is always positive and the fake
		// clock's immediate-fire path never engages. Reset is safe on a live
		// timer by contract (internal/clock).
		now := w.clk.Now()
		for !next.After(now) {
			next = next.Add(w.cfg.HeartbeatPeriod)
		}
		w.hbTimer.Reset(next.Sub(now))
		w.mu.Unlock()

		// The send runs OUTSIDE the mutex on purpose: Sender is the wire
		// layer's callback and may block on real I/O — holding mu here would
		// stall Activity() (the read loop) behind a network write. If Stop
		// lands mid-send the worst case is one final beat whose bytes die
		// with the session; the terminal transition below or the owner's own
		// Stop retires the loop either way.
		if err := w.send(heartbeatEnvelope()); err != nil {
			w.logger.Warn("control: heartbeat send failed",
				slog.String("reason", err.Error()),
			)
			w.SendFailed(fmt.Errorf("heartbeat send failed: %w", err))
			return
		}
	}
}

// watchDead waits for one generation's silence deadline and declares the
// peer dead if the generation is still current. One goroutine per deadline;
// exits via quit (watchdog stopped) or by delivering the verdict.
//
// # The verdict race, and why the claim is under the lock
//
// A silence deadline and a transport error can observe the SAME death at
// nearly the same moment. Both verdicts funnel through a claim — stopped set
// under w.mu before any machine transition — so exactly one of them drives
// the machine and the loser retires. Without the claim, a teardown that won
// the race moves the machine to disconnected and the late degraded verdict
// then hits an illegal edge (the table panics loudly by design); the panic
// would be reporting a real interleaving with the wrong remedy. Found by
// ts:reconnect-resume's client-level tests under -race repetition; the fix
// is here because the policy owns its verdicts.
func (w *Watchdog) watchDead(gen uint64, tm clock.Timer) {
	select {
	case <-w.quit:
		return
	case <-tm.C():
	}

	w.mu.Lock()
	if w.stopped || gen != w.gen {
		// Superseded by newer activity, stopped outright, or beaten by a
		// transport-error verdict: this fire is stale and must expire
		// nothing — the winner owns the truth. Same gen-check shape as the
		// presence tracker's leases.
		//
		// Stop the deadline explicitly: the fire already happened (that is
		// why this goroutine is running), but on the injected clock every
		// un-stopped timer holds a waiter slot until it fires, and a
		// chatty session rotates one deadline per inbound frame. Retiring
		// each superseded timer here keeps a fleet-scale simulation's
		// waiter list proportional to live sessions, not to total traffic.
		tm.Stop()
		w.mu.Unlock()
		return
	}
	if w.mach.State() != StateOnline {
		// The session died another way while this fire sat queued: the
		// machine already tells the true story (directly disconnected, no
		// degraded waypoint). Retire without a second verdict.
		tm.Stop()
		w.mu.Unlock()
		w.Stop()
		return
	}
	w.stopped = true // claim: no other verdict may start from here
	w.mu.Unlock()

	// Failure mode (a): socket open, peer silent past the interval. Two
	// deliberate transitions — degraded announces the DIAGNOSIS, disconnected
	// performs the TEARDOWN. Splitting them is what lets criterion 6 show
	// the user "peer went silent" rather than jumping straight to a bare
	// reconnect.
	if _, err := w.mach.Transition(StateDegraded, fmt.Sprintf("no inbound traffic for %s", w.cfg.DeadPeerInterval)); err != nil {
		w.logger.Debug("control: degraded transition refused", slog.String("reason", err.Error()))
		w.Stop()
		return
	}
	w.logger.Warn("control: dead peer detected",
		slog.Duration("silence", w.cfg.DeadPeerInterval),
	)
	w.transitionDisconnected("dead peer: tearing down deliberately")
}

// Stop retires the watchdog: timers disarmed, loops exit, further Activity
// and verdicts are ignored. Idempotent; safe to call from any goroutine,
// including the watchdog's own (close never joins).
//
// The CURRENT silence-deadline timer is disarmed here too, not just the
// heartbeat timer: its watcher exits via quit without stopping the timer
// (it cannot know whether a fire was already committed), so Stop is the
// only place that can guarantee a retired watchdog leaves NO waiters on
// the clock — which fleet-scale simulations count on.
func (w *Watchdog) Stop() {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.stopped = true
		if w.hbTimer != nil {
			w.hbTimer.Stop()
		}
		if w.deadline != nil {
			w.deadline.Stop()
		}
		w.mu.Unlock()
		close(w.quit)
	})
}
