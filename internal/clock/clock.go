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
}

// Real returns a Clock backed by the wall clock.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) Since(t time.Time) time.Duration        { return time.Since(t) }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
