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
