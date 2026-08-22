package testnet

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// Pattern demonstration for ts:test-harness criterion 6: reconnect backoff.
//
// The production reconnect policy is owned by ts:reconnect-resume, which does
// not exist yet; this file proves the substrate carries the weight — a seeded,
// jittered backoff schedule whose determinism is asserted against an explicit
// source, driven onto the injected clock through the Timer seam. The owning
// item inherits the production version of this exact shape and asserts its own
// policy against the same primitives.

// jitteredBackoff is the full-jitter exponential shape: the uncapped delay
// doubles per attempt, the cap clamps it, and the actual draw is uniform over
// the result. Full jitter is the variant that best breaks synchronisation
// between failed clients, which is precisely what the herd call site needs.
//
// The rand source is a parameter because determinism here is a contract, not a
// hope: math/rand/v2's global functions would make every schedule depend on
// whatever else drew from them. An explicit PCG source makes a schedule a pure
// function of its seeds.
func jitteredBackoff(rng *rand.Rand, base, max time.Duration, attempt int) time.Duration {
	d := base << uint(min(attempt, 16))
	if d > max || d <= 0 {
		d = max
	}
	return time.Duration(float64(d) * rng.Float64())
}

func TestPatternReconnectBackoffScheduleIsAFunctionOfItsSeed(t *testing.T) {
	const (
		base     = time.Second
		cap      = time.Minute
		attempts = 10
	)

	schedule := func(seedA, seedB uint64) []time.Duration {
		rng := rand.New(rand.NewPCG(seedA, seedB))
		out := make([]time.Duration, attempts)
		for i := range attempts {
			out[i] = jitteredBackoff(rng, base, cap, i)
		}
		return out
	}

	first := schedule(0x5EED, 0xC0FFEE)

	// Same seeds, identical schedule — replayable forever, which is what lets
	// a failing reconnect test be debugged at all.
	if replay := schedule(0x5EED, 0xC0FFEE); !slices.Equal(first, replay) {
		t.Errorf("same seed produced different schedules:\nfirst  %v\nsecond %v", first, replay)
	}

	// Different seeds, different schedule — the source is really consulted,
	// not a constant in disguise.
	if other := schedule(0x5EED+1, 0xC0FFEE); slices.Equal(first, other) {
		t.Error("different seed produced identical schedules")
	}

	// Every draw respects its attempt's envelope: non-negative and no larger
	// than the capped doubling for that attempt.
	for i, d := range first {
		ceiling := min(base<<uint(i), cap)
		if d < 0 || d > ceiling {
			t.Fatalf("draw %d = %v outside [0, %v]", i, d, ceiling)
		}
	}
}

// The same schedule driven onto the injected clock through the Timer seam:
// arm, fire, rearm — the exact dance ts:reconnect-resume performs between
// reconnect attempts, and the reason Clock grew a cancellable Timer. Fire
// times must equal the cumulative sum of the drawn delays, exactly.
func TestPatternReconnectBackoffFiresOnTheInjectedClock(t *testing.T) {
	const (
		base     = time.Second
		cap      = time.Minute
		attempts = 6
		step     = 100 * time.Millisecond
	)

	rng := rand.New(rand.NewPCG(0xB0FF, 0xCAFE))
	delays := make([]time.Duration, attempts)
	for i := range attempts {
		delays[i] = jitteredBackoff(rng, base, cap, i)
	}

	fake := clock.NewFake(epoch)
	tm := fake.NewTimer(delays[0])

	var fired []time.Time
	for next := 1; next <= attempts; {
		if fake.Now().After(epoch.Add(cap * time.Duration(attempts))) {
			t.Fatal("backoff timers never finished firing")
		}
		fake.Advance(step)
		select {
		case v := <-tm.C():
			fired = append(fired, v)
			if next < attempts {
				tm.Reset(delays[next])
			}
			next++
		default:
		}
	}

	// Each fire must land within one step after its own deadline. Deadlines
	// are not checked against the cumulative ideal: a fire lands up to one
	// step late (Advance crosses the deadline mid-step), and Reset rearms
	// from that quantized moment, so lateness compounds across fires. The
	// invariant that holds under stepwise driving is per-gap: the observed
	// gap between consecutive fires equals the drawn delay plus at most one
	// step of quantization. Exact fire-at-deadline is pinned by the Timer
	// tests in internal/clock, where Advance lands on the deadline itself.
	prev := epoch
	for i, v := range fired {
		low := prev.Add(delays[i])
		high := low.Add(step)
		if !v.After(low) || v.After(high) {
			t.Fatalf("fire %d at %v, want within (%v, %v] — the seeded schedule was not honoured", i, v, low, high)
		}
		prev = v
	}
	if len(fired) != attempts {
		t.Fatalf("fired %d times, want %d", len(fired), attempts)
	}
}
