package control

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// epoch pins every fake clock in this package's tests: identical starts keep
// failure output comparable across tests.
var epoch = time.Date(2026, time.August, 22, 0, 0, 0, 0, time.UTC)

// pathTo walks a fresh machine from StateDisconnected to target along legal
// edges, returning the machine positioned there. Tests use it to reach each
// state without duplicating the chain by hand.
func pathTo(t *testing.T, clk clock.Clock, target State) *Machine {
	t.Helper()
	m, err := NewMachine(clk, nil)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	if target == StateDisconnected {
		return m // the construction state; nothing to walk
	}
	for _, st := range []State{StateConnecting, StateHandshaking, StateOnline, StateDegraded} {
		if _, err := m.Transition(st, "test: walking to "+target.String()); err != nil {
			t.Fatalf("walk to %s: %v", target, err)
		}
		if st == target {
			break
		}
	}
	if m.State() != target {
		t.Fatalf("pathTo landed in %s, want %s", m.State(), target)
	}
	return m
}

// collectNonBlocking drains whatever is immediately available from sub,
// preserving order. Used for absence assertions; expected events are received
// blocking instead, which doubles as synchronisation with the delivery pump.
func collectNonBlocking(sub *Subscription) []Change {
	var out []Change
	for {
		select {
		case ch := <-sub.C():
			out = append(out, ch)
		default:
			return out
		}
	}
}

func TestEveryLegalTransitionFiresAnEvent(t *testing.T) {
	// Every edge in legalTransitions, exercised through its real entry path:
	// the table is the law, so the test reads it rather than restating it.
	for from, edges := range legalTransitions {
		for to := range edges {
			from, to := from, to
			t.Run(from.String()+"->"+to.String(), func(t *testing.T) {
				fake := clock.NewFake(epoch)
				m := pathTo(t, fake, from)
				sub := m.Subscribe()

				ok, err := m.Transition(to, "test edge")
				if err != nil || !ok {
					t.Fatalf("legal transition %s->%s refused: ok=%v err=%v", from, to, ok, err)
				}
				if got := m.State(); got != to {
					t.Fatalf("state is %s, want %s", got, to)
				}

				got := <-sub.C()
				if got.From != from || got.To != to {
					t.Fatalf("change = %s->%s, want %s->%s", got.From, got.To, from, to)
				}
				if got.Reason != "test edge" {
					t.Fatalf("reason = %q, want %q", got.Reason, "test edge")
				}
				if !got.At.Equal(epoch) {
					t.Fatalf("change stamped %v, want the injected clock's %v", got.At, epoch)
				}
				if extra := collectNonBlocking(sub); len(extra) != 0 {
					t.Fatalf("unexpected extra events: %+v", extra)
				}
			})
		}
	}
}

func TestIllegalTransitionFailsLoudly(t *testing.T) {
	// Every pair NOT in the table (and not a same-state no-op) must panic at
	// the call site, leave the state untouched, and deliver nothing. Silent
	// boolean drift is the failure mode the machine exists to prevent; a
	// loud crash is the honest alternative.
	states := []State{StateDisconnected, StateConnecting, StateHandshaking, StateOnline, StateDegraded}
	for _, from := range states {
		for _, to := range states {
			if from == to || canTransition(from, to) {
				continue
			}
			from, to := from, to
			t.Run(from.String()+"->"+to.String(), func(t *testing.T) {
				fake := clock.NewFake(epoch)
				m := pathTo(t, fake, from)
				sub := m.Subscribe()

				panicCaught := func() (msg string) {
					defer func() {
						if r := recover(); r != nil {
							msg, _ = r.(string)
						}
					}()
					m.Transition(to, "must be illegal")
					return ""
				}()

				if panicCaught == "" {
					t.Fatalf("illegal transition %s->%s did not fail loudly", from, to)
				}
				if !strings.Contains(panicCaught, from.String()) || !strings.Contains(panicCaught, to.String()) {
					t.Fatalf("panic message %q must name both endpoints", panicCaught)
				}
				if got := m.State(); got != from {
					t.Fatalf("state drifted to %s after refused transition, want %s", got, from)
				}
				if extra := collectNonBlocking(sub); len(extra) != 0 {
					t.Fatalf("refused transition delivered events: %+v", extra)
				}
			})
		}
	}
}

func TestSameStateTransitionIsANoOp(t *testing.T) {
	fake := clock.NewFake(epoch)
	m := pathTo(t, fake, StateOnline)
	sub := m.Subscribe()

	ok, err := m.Transition(StateOnline, "duplicate verdict")
	if err != nil {
		t.Fatalf("same-state transition errored: %v", err)
	}
	if ok {
		t.Fatal("same-state transition reported a change")
	}
	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("no-op delivered events: %+v", extra)
	}
}

func TestFullChainDeliveredToEverySubscriberInOrder(t *testing.T) {
	// Burst-then-drain: ALL five transitions complete before any subscriber
	// reads. Criterion 6's cannot-miss contract means the burst queues, not
	// drops — a subscriber that renders slowly still sees the whole story.
	fake := clock.NewFake(epoch)
	m, err := NewMachine(fake, nil)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}

	const watchers = 3
	subs := make([]*Subscription, watchers)
	for i := range subs {
		subs[i] = m.Subscribe()
	}

	chain := []State{StateConnecting, StateHandshaking, StateOnline, StateDegraded, StateDisconnected}
	want := make([]Change, 0, len(chain))
	prev := StateDisconnected
	for _, st := range chain {
		fake.Advance(time.Second)
		if _, err := m.Transition(st, "chain"); err != nil {
			t.Fatalf("transition to %s: %v", st, err)
		}
		want = append(want, Change{From: prev, To: st, Reason: "chain", At: fake.Now()})
		prev = st
	}

	for i, sub := range subs {
		got := make([]Change, 0, len(want))
		for range want {
			got = append(got, <-sub.C())
		}
		if !changesEqual(got, want) {
			t.Errorf("subscriber %d saw:\n%+v\nwant:\n%+v", i, got, want)
		}
		if extra := collectNonBlocking(sub); len(extra) != 0 {
			t.Errorf("subscriber %d got extra events: %+v", i, extra)
		}
	}
}

func changesEqual(a, b []Change) bool {
	return slices.EqualFunc(a, b, func(x, y Change) bool {
		return x.From == y.From && x.To == y.To && x.Reason == y.Reason && x.At.Equal(y.At)
	})
}

func TestSubscribeBeforeStateNeverMisses(t *testing.T) {
	// The documented ordering rule: subscribe FIRST, then read State(). A
	// change landing between the two calls appears in both — duplicated, not
	// missed. This test pins the miss-free half: a transition strictly after
	// Subscribe is visible through BOTH surfaces.
	fake := clock.NewFake(epoch)
	m := pathTo(t, fake, StateConnecting)
	sub := m.Subscribe()

	if _, err := m.Transition(StateHandshaking, "raced read"); err != nil {
		t.Fatalf("transition: %v", err)
	}

	if got := (<-sub.C()).To; got != StateHandshaking {
		t.Fatalf("stream showed %s, want handshaking", got)
	}
	if got := m.State(); got != StateHandshaking {
		t.Fatalf("State() showed %s, want handshaking", got)
	}
}

func TestLateSubscriberSeesNothingPrior(t *testing.T) {
	fake := clock.NewFake(epoch)
	m := pathTo(t, fake, StateOnline)

	sub := m.Subscribe() // after the history happened
	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("late subscriber received history: %+v", extra)
	}
}

func TestSubscriptionCloseStopsPromptlyAndClosesTheStream(t *testing.T) {
	fake := clock.NewFake(epoch)
	m, err := NewMachine(fake, nil)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	sub := m.Subscribe()

	// Queue changes without draining, then the CONSUMER closes: the pump
	// stops promptly. Whatever already reached C's buffer stays readable
	// (closing a channel never discards buffered values); whatever didn't is
	// dropped, because the consumer said it was done listening. The
	// deterministic assertion is termination — the range must end, not hang.
	for _, st := range []State{StateConnecting, StateHandshaking, StateOnline} {
		if _, err := m.Transition(st, "close test"); err != nil {
			t.Fatalf("transition: %v", err)
		}
	}
	sub.Close()

	count := 0
	for range sub.C() {
		count++
	}
	if count > 3 {
		t.Fatalf("closed subscription invented %d changes", count-3)
	}

	// A closed subscription no longer receives new transitions.
	if _, err := m.Transition(StateDegraded, "after unsubscribe"); err != nil {
		t.Fatalf("transition after unsubscribe: %v", err)
	}
	for range sub.C() {
		t.Fatal("closed subscription received a new change")
	}
}

func TestMachineCloseFlushesOwedChangesThenCloses(t *testing.T) {
	// Machine shutdown is the OTHER close semantics: nothing a subscriber
	// was owed may vanish at shutdown (criterion 6 does not lapse), so the
	// pump flushes the queue before closing the stream.
	fake := clock.NewFake(epoch)
	m, err := NewMachine(fake, nil)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	sub := m.Subscribe()

	const queued = 3
	for _, st := range []State{StateConnecting, StateHandshaking, StateOnline} {
		if _, err := m.Transition(st, "flush test"); err != nil {
			t.Fatalf("transition: %v", err)
		}
	}
	m.Close()

	count := 0
	for got := range sub.C() {
		count++
		if got.Reason != "flush test" {
			t.Fatalf("flushed change carries reason %q", got.Reason)
		}
	}
	if count != queued {
		t.Fatalf("machine close delivered %d of %d owed changes", count, queued)
	}
}

func TestMachineCloseRefusesTransitionsAndFinishesSubscriptions(t *testing.T) {
	fake := clock.NewFake(epoch)
	m, err := NewMachine(fake, nil)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	sub := m.Subscribe()
	if _, err := m.Transition(StateConnecting, "before close"); err != nil {
		t.Fatalf("transition: %v", err)
	}

	m.Close()

	if _, err := m.Transition(StateHandshaking, "after close"); err == nil {
		t.Fatal("transition after Close was accepted")
	}
	// The owed change still arrives, then the channel closes.
	if got := (<-sub.C()).To; got != StateConnecting {
		t.Fatalf("post-close drain showed %s", got)
	}
	for range sub.C() {
	}
	// Idempotent.
	m.Close()
}

func TestConcurrentSameStateTransitionsAreHarmless(t *testing.T) {
	// Many goroutines concluding "still online" must stay no-ops: the
	// machine serialises them, delivers nothing, and never panics. This is
	// the race-shaped case the same-state rule exists for.
	//
	// The join is a WaitGroup on purpose. An earlier draft signalled
	// completion by sending on one shared channel that every goroutine also
	// closed via defer — the first close then panics every still-blocked
	// send ("send on closed channel"), a failure that only shows up under
	// -race -count repetition. A channel is the wrong tool for "wait for N
	// goroutines"; the WaitGroup states that intent and cannot be closed.
	fake := clock.NewFake(epoch)
	m := pathTo(t, fake, StateOnline)
	sub := m.Subscribe()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				m.Transition(StateOnline, "concurrent duplicate") //nolint:errcheck // no-op by contract
			}
		}()
	}
	wg.Wait()

	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("duplicate transitions produced events: %+v", extra)
	}
	if got := m.State(); got != StateOnline {
		t.Fatalf("state drifted to %s", got)
	}
}

func TestNewMachineRefusesNilClock(t *testing.T) {
	if _, err := NewMachine(nil, nil); err == nil {
		t.Fatal("nil clock accepted")
	}
}
