package clock

import (
	"sync"
	"time"
)

var (
	_ Clock = realClock{}
	_ Clock = (*Fake)(nil)
)

// Fake is a Clock that moves only when [Fake.Advance] is called. It is safe for
// concurrent use, which matters because the code under test usually arms its
// timers from a different goroutine than the test that advances them.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
	// owner is set when this waiter backs a [fakeTimer], nil for the plain
	// After/Sleep waiters. The back-pointer lets Stop and Reset find and
	// retire exactly their own entry while all other waiters keep firing.
	owner *fakeTimer
}

// fakeTimer is the [*Fake] implementation of [Timer]. All of its state
// transitions happen under the owning Fake's mutex — the same lock that
// Advance holds while deciding which waiters are due — so "stopped" and
// "fired" can never interleave badly: once Advance has claimed a waiter under
// the lock it marks the owner fired before releasing it, and a racing Stop
// then correctly reports false instead of pretending to cancel a fire that is
// already on its way to a buffered channel.
type fakeTimer struct {
	f  *Fake
	ch chan time.Time
	st timerState
}

type timerState uint8

const (
	timerStopped timerState = iota
	timerPending
	timerFired
)

// NewTimer returns a one-shot cancellable timer; see the [Clock] interface for
// why cancellation exists at all.
//
// A non-positive d fires immediately: the fire is written into the buffered
// channel here, after releasing the lock, matching how Advance delivers its
// fires outside the lock.
func (f *Fake) NewTimer(d time.Duration) Timer {
	t := &fakeTimer{f: f, ch: make(chan time.Time, 1)}

	f.mu.Lock()
	now := f.now
	if d <= 0 {
		t.st = timerFired
	} else {
		t.st = timerPending
		f.waiters = append(f.waiters, &waiter{at: now.Add(d), ch: t.ch, owner: t})
	}
	f.mu.Unlock()

	if d <= 0 {
		t.ch <- now
	}
	return t
}

// C receives the single fire.
func (t *fakeTimer) C() <-chan time.Time { return t.ch }

// Stop cancels a pending fire. It reports false if the timer already fired or
// was already stopped — including the window where Advance has claimed the
// waiter but not yet delivered it, which callers must treat as "may have
// fired", same contract as *time.Timer.Stop.
func (t *fakeTimer) Stop() bool {
	f := t.f
	f.mu.Lock()
	defer f.mu.Unlock()

	switch t.st {
	case timerPending:
		t.st = timerStopped
		f.removeWaiter(t)
		return true
	default: // fired or already stopped
		return false
	}
}

// Reset rearms the timer, atomically cancelling any pending fire. Safe on a
// live timer, unlike *time.Timer.Reset — see the interface comment for why
// that divergence is deliberate.
func (t *fakeTimer) Reset(d time.Duration) bool {
	f := t.f
	f.mu.Lock()

	wasPending := t.st == timerPending
	if wasPending {
		f.removeWaiter(t)
	}
	now := f.now
	if d <= 0 {
		t.st = timerFired
	} else {
		t.st = timerPending
		f.waiters = append(f.waiters, &waiter{at: now.Add(d), ch: t.ch, owner: t})
	}
	f.mu.Unlock()

	if d <= 0 {
		// Non-blocking on purpose: if a previously committed fire has been
		// claimed by Advance but not yet delivered, its send is already in
		// flight to this channel. Dropping the new immediate fire loses
		// nothing — the receiver still gets exactly one wake-up — whereas a
		// blocking send would park the caller on a channel only the receiver
		// can drain, i.e. a scheduling-dependent stall of the kind this
		// package exists to make impossible.
		select {
		case t.ch <- now:
		default:
		}
	}
	return wasPending
}

// removeWaiter retires the outstanding waiter belonging to t, if any. Called
// with f.mu held.
func (f *Fake) removeWaiter(t *fakeTimer) {
	for i, w := range f.waiters {
		if w.owner == t {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			return
		}
	}
}

// NewFake returns a Fake positioned at start.
//
// Pass an explicit start time rather than time.Now: a fake clock seeded from
// the wall clock is only half fake, and reintroduces exactly the
// non-determinism this type exists to remove.
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

// Now reports the fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since reports the fake time elapsed since t.
func (f *Fake) Since(t time.Time) time.Duration { return f.Now().Sub(t) }

// After returns a channel that receives when the fake clock has advanced past
// d. A non-positive d fires immediately.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.waiters = append(f.waiters, &waiter{at: f.now.Add(d), ch: ch})
	return ch
}

// Sleep blocks until the clock has been advanced by at least d.
//
// A test that calls Sleep and never advances will block forever. That is
// deliberate: a missing Advance shows up as an obvious deadlock instead of as a
// test that passes for the wrong reason.
func (f *Fake) Sleep(d time.Duration) { <-f.After(d) }

// Advance moves the clock forward by d and fires every waiter that is now due.
//
// Waiters are fired after the lock is released, so a fired goroutine may arm a
// new timer without deadlocking.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now

	var due []*waiter
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if w.at.After(now) {
			kept = append(kept, w)
		} else {
			// Claim timer-backed waiters under the lock: from this moment a
			// racing Stop must report false, because the fire is committed.
			if w.owner != nil {
				w.owner.st = timerFired
			}
			due = append(due, w)
		}
	}
	// Release the slots the retained waiters no longer occupy, so fired timers
	// are not kept alive by the backing array.
	for i := len(kept); i < len(f.waiters); i++ {
		f.waiters[i] = nil
	}
	f.waiters = kept
	f.mu.Unlock()

	for _, w := range due {
		w.ch <- now
	}
}

// Waiters reports how many timers are outstanding.
//
// Tests use this to assert that the code under test actually armed the timer
// they are about to trigger — advancing a clock nobody is waiting on passes
// silently and proves nothing.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}
