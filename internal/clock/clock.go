// Package clock is the time seam that makes walkie's timing behaviour testable.
//
// Owning work item:
//
//	eka get walkie/ts:test-harness
//
// # Why this exists
//
// The behaviour that matters most in this system is timing-dependent: presence
// expiry against a liveness TTL, queue retention, reconnect backoff with
// jitter. ts:test-harness requires that "all timeouts, backoff intervals and
// TTLs are driven by the injectable clock; no test sleeps in real time", and
// that tests be deterministic rather than flaky.
//
// That is not a style preference. A test that waits out a real 45-second
// dead-peer interval is a test nobody runs, and a test that waits 45
// milliseconds instead is testing a different system. Presence in particular
// has a defining test — kill a client with SIGKILL and assert it goes offline
// within the TTL — which is only practical against a clock you control.
//
// # The rule
//
// Any package that waits, retries or expires takes a [Clock] rather than
// calling time.Now, time.After or time.Sleep directly. Production wiring passes
// [Real]; tests pass [*Fake].
package clock

import "time"

// Clock is the subset of time that walkie depends on. It is deliberately small:
// every method added here is another method a fake has to get right.
type Clock interface {
	// Now reports the current time.
	Now() time.Time

	// Since reports the time elapsed since t, equivalent to Now().Sub(t).
	Since(t time.Time) time.Duration

	// After returns a channel that receives once, after d has elapsed. A
	// non-positive d fires immediately.
	After(d time.Duration) <-chan time.Time

	// Sleep blocks for d.
	Sleep(d time.Duration)

	// NewTimer returns a one-shot [Timer] that fires after d and can be
	// stopped or rearmed before it does. A non-positive d fires immediately.
	//
	// Why this is on the interface rather than built from After: three
	// consuming items arm timers they must retract — req:device-presence
	// refreshes a liveness TTL on every heartbeat, ts:reconnect-resume cancels
	// the pending backoff timer the moment a connection succeeds, and
	// sto:offline-queue runs retention deadlines against entries that leave
	// the queue early. Without cancellation each of those either leaks a
	// waiter that fires late (and forces stale-fire handling at every call
	// site) or re-implements timer bookkeeping per package. One well-tested
	// Timer here beats three half-correct ones elsewhere.
	NewTimer(d time.Duration) Timer
}

// Timer is a one-shot timer that can be cancelled and rearmed.
//
// It mirrors the parts of *time.Timer walkie actually uses, so production code
// wired to [Real] behaves like test code wired to [*Fake].
type Timer interface {
	// C receives the single fire. The channel is buffered, so a fire that
	// races a Stop is never lost or blocked — see Stop for what that means
	// for the return value.
	C() <-chan time.Time

	// Stop cancels a pending fire and reports whether it did so. false means
	// the timer had already fired or been stopped; the caller must treat a
	// value possibly sitting in C as delivered.
	Stop() bool

	// Reset rearms the timer to fire after d, cancelling any pending fire,
	// and reports whether there was one to cancel.
	//
	// Unlike *time.Timer.Reset, which may only be called on a stopped or
	// drained timer, this Reset is safe to call on a live timer: both
	// implementations make cancel-and-rearm atomic with respect to firing.
	// The divergence from stdlib semantics is deliberate — the heartbeat
	// refresh pattern in req:device-presence wants "move the deadline", not
	// "stop, drain, maybe restart", and getting that dance wrong is exactly
	// the kind of flaky-timer bug this package exists to prevent.
	Reset(d time.Duration) bool
}

// Real returns a Clock backed by the wall clock.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) Since(t time.Time) time.Duration        { return time.Since(t) }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (realClock) NewTimer(d time.Duration) Timer         { return &realTimer{t: time.NewTimer(d)} }

// realTimer delegates to *time.Timer. The channel is shared, not wrapped, so
// production code sees exactly the stdlib fire semantics it would see without
// this seam.
type realTimer struct{ t *time.Timer }

func (t *realTimer) C() <-chan time.Time        { return t.t.C }
func (t *realTimer) Stop() bool                 { return t.t.Stop() }
func (t *realTimer) Reset(d time.Duration) bool { return t.t.Reset(d) }
