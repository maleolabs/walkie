package control

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Backoff produces the reconnect delay schedule: exponential growth with
// FULL JITTER, capped.
//
// Owning work item:
//
//	eka get walkie/ts:reconnect-resume   (acceptance criterion 2)
//
// # Full jitter means full jitter
//
// The delay for attempt n is drawn UNIFORMLY from
//
//	[0, min(cap, base * 2^n))
//
// — not "base * 2^n plus a small wobble", which is decorrelated backoff and
// a different algorithm. The distinction is load-bearing: after a coordinator
// restart every disconnected client begins retrying at once, and any
// deterministic component in the delay survives across clients. Full jitter
// flattens each attempt's envelope into a uniform draw, which is the variant
// that best breaks fleet-wide synchronisation (the thundering-herd outcome
// criterion 2 forbids).
//
// # Determinism is a contract
//
// The source is an explicit math/rand/v2 PCG stream seeded at construction;
// the same seeds produce the identical schedule forever. No global rand is
// consulted anywhere, so one client's schedule depends on nothing but its
// seeds — the same rule internal/testnet applies per simulated client. This
// is what makes criterion 2's twenty-client assertion reproducible instead
// of probabilistic, and what makes a failing reconnect test debuggable at
// all.
type Backoff struct {
	base time.Duration
	cap  time.Duration
	rng  *rand.Rand
}

// NewBackoff returns a Backoff drawing delays for attempts whose uncapped
// envelope starts at base and doubles per attempt, never exceeding cap.
//
// seedA and seedB seed the PCG stream directly (rand.NewPCG(seedA, seedB));
// callers wanting independent schedules pass different pairs — the fleet
// simulator derives (seed, clientIndex) pairs for exactly this purpose.
//
// base and cap must be positive with cap >= base: a non-positive base draws
// zero-length delays forever (a hot retry loop wearing a backoff costume),
// and cap < base makes the first-attempt envelope a lie. Both are wiring
// bugs, refused here rather than endured — the house rule for programmer
// errors.
func NewBackoff(seedA, seedB uint64, base, cap time.Duration) (*Backoff, error) {
	if base <= 0 {
		return nil, fmt.Errorf("control: new backoff: base must be positive (got %s)", base)
	}
	if cap <= 0 {
		return nil, fmt.Errorf("control: new backoff: cap must be positive (got %s)", cap)
	}
	if cap < base {
		return nil, fmt.Errorf("control: new backoff: cap %s must not be below base %s", cap, base)
	}
	return &Backoff{base: base, cap: cap, rng: rand.New(rand.NewPCG(seedA, seedB))}, nil
}

// Delay returns the jittered delay for the given attempt (0-based), drawn
// uniformly from [0, min(cap, base*2^attempt)).
//
// The doubling runs as a loop rather than a shift so an absurd attempt index
// cannot overflow the duration: the loop stops the moment the envelope
// reaches the cap, so its iteration count is bounded by log2(cap/base) no
// matter what n is.
//
// The final clamp guards float rounding, not algorithm: converting a large
// duration to float64 and multiplying by a value just under 1 can round back
// up to the envelope itself, which would make the interval closed instead of
// half-open. One nanosecond of slack keeps the documented bound exact.
func (b *Backoff) Delay(attempt int) time.Duration {
	envelope := b.cap
	d := b.base
	for i := 0; i < attempt && d < b.cap; i++ {
		d *= 2
		if d <= 0 { // overflowed int64 nanoseconds: clamp to cap
			d = b.cap
			break
		}
	}
	if d < envelope {
		envelope = d
	}

	delay := time.Duration(float64(envelope) * b.rng.Float64())
	if delay >= envelope {
		delay = envelope - 1
	}
	return delay
}
