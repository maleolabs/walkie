package control

import (
	"testing"
	"time"
)

// The full-jitter contract, pinned: the delay for attempt n is drawn
// uniformly from [0, min(cap, base*2^n)). Seeded determinism is what makes
// criterion 2's twenty-client herd assertion reproducible; the envelope and
// uniformity assertions are what make "full jitter" mean full jitter rather
// than decorrelated wobble.

func TestBackoffScheduleIsAFunctionOfItsSeed(t *testing.T) {
	const (
		base = time.Second
		cap  = time.Minute
		n    = 12
	)

	schedule := func(seedA, seedB uint64) []time.Duration {
		b, err := NewBackoff(seedA, seedB, base, cap)
		if err != nil {
			t.Fatalf("new backoff: %v", err)
		}
		out := make([]time.Duration, n)
		for i := range n {
			out[i] = b.Delay(i)
		}
		return out
	}

	first := schedule(0x5EED, 0xC0FFEE)

	// Same seeds, identical schedule — replayable forever.
	if replay := schedule(0x5EED, 0xC0FFEE); !slicesEqual(first, replay) {
		t.Errorf("same seeds produced different schedules:\nfirst  %v\nsecond %v", first, replay)
	}

	// Different seeds, different schedule — the source is really consulted,
	// not a constant in disguise.
	if other := schedule(0x5EED+1, 0xC0FFEE); slicesEqual(first, other) {
		t.Error("different seeds produced identical schedules")
	}
}

func slicesEqual(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBackoffDelaysRespectTheFullJitterEnvelope(t *testing.T) {
	const (
		base = 500 * time.Millisecond
		cap  = 30 * time.Second
	)
	b, err := NewBackoff(1, 2, base, cap)
	if err != nil {
		t.Fatalf("new backoff: %v", err)
	}

	for attempt := range 24 {
		got := b.Delay(attempt)
		ceiling := min(base<<uint(attempt), cap)
		if got < 0 || got >= ceiling {
			t.Fatalf("attempt %d drew %v, outside half-open [0, %v)", attempt, got, ceiling)
		}
	}
}

func TestBackoffEnvelopeReachesTheCap(t *testing.T) {
	// After enough attempts the envelope stops growing at the cap: every
	// further draw comes from [0, cap), never beyond. log2(cap/base) = 6
	// here (1s doubling to 64s clamped at 60s).
	const (
		base = time.Second
		cap  = 60 * time.Second
	)
	b, err := NewBackoff(3, 4, base, cap)
	if err != nil {
		t.Fatalf("new backoff: %v", err)
	}

	for attempt := 10; attempt < 20; attempt++ {
		got := b.Delay(attempt)
		if got < 0 || got >= cap {
			t.Fatalf("attempt %d (past cap convergence) drew %v, outside [0, %v)", attempt, got, cap)
		}
	}

	// An absurd attempt index must not overflow the duration arithmetic —
	// the loop bound is log2(cap/base), not the attempt count.
	if got := b.Delay(1 << 20); got < 0 || got >= cap {
		t.Fatalf("absurd attempt drew %v, outside [0, %v)", got, cap)
	}
}

func TestBackoffSpreadIsUniformNotClustered(t *testing.T) {
	// Uniformity IS the anti-herd property: a fleet of clients drawing from
	// one attempt's envelope spreads across it instead of piling up. Bucket
	// many draws from a converged attempt into fifths of the envelope; a
	// uniform draw fills each fifth with roughly a fifth of the samples.
	// Thresholds are generous (uniform PCG lands ~400 per bucket with 2000
	// draws) and the seed is fixed, so the assertion is deterministic — it
	// either always holds or always fails, never flakes.
	const (
		base   = 30 * time.Second // converges to the cap well before attempt 7
		cap    = time.Minute
		draws  = 2000
		bucket = cap / 5
	)
	b, err := NewBackoff(0xABCD, 0x1234, base, cap)
	if err != nil {
		t.Fatalf("new backoff: %v", err)
	}

	counts := make([]int, 5)
	for range draws {
		d := b.Delay(7)
		counts[int(d/bucket)]++
	}

	for i, c := range counts {
		if c < 300 || c > 520 {
			t.Errorf("bucket %d (%v-%v) holds %d of %d draws — distribution clustered, not uniform",
				i, time.Duration(i)*bucket, time.Duration(i+1)*bucket, c, draws)
		}
	}
}

func TestNewBackoffRefusesInvalidParameters(t *testing.T) {
	cases := []struct {
		name      string
		base, cap time.Duration
	}{
		{"zero base", 0, time.Minute},
		{"negative base", -time.Second, time.Minute},
		{"zero cap", time.Second, 0},
		{"cap below base", time.Minute, time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewBackoff(1, 1, tc.base, tc.cap); err == nil {
				t.Fatalf("NewBackoff(%s, %s) accepted", tc.base, tc.cap)
			}
		})
	}
}
