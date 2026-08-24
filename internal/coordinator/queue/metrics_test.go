package queue

import (
	"bytes"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/obs"
)

// The tests in this file pin ts:observability-baseline's queue-side metric
// hooks: evictions split by TTL and size cap, and the unlabelled cross-
// recipient depth histogram. Values are read from the registry a scraper
// would gather; eviction is observed via the Swept() event, never by
// sleeping out the watcher.

// evictionValue reads one reason's count from the registry a scraper sees.
func evictionValue(t *testing.T, m *obs.Metrics, reason string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "walkie_queue_evictions_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("reason %q series not exported at all (must be pre-initialised)", reason)
	return 0
}

// depthObservations gathers the walkie_queue_depth histogram's sample count
// and sum — the two aggregates an endpoint observer can legitimately see.
func depthObservations(t *testing.T, m *obs.Metrics) (uint64, float64) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "walkie_queue_depth" {
			continue
		}
		if len(f.GetMetric()) != 1 {
			t.Fatalf("queue depth exported %d series, want exactly one unlabelled series", len(f.GetMetric()))
		}
		h := f.GetMetric()[0].GetHistogram()
		return h.GetSampleCount(), h.GetSampleSum()
	}
	t.Fatal("walkie_queue_depth not exported")
	return 0, 0
}

func TestQueueMetricsEvictionsByTTLAndSizeCap(t *testing.T) {
	m := obs.NewMetrics()
	clk := testClock()
	st := mustOpenStore(t, t.TempDir()+"/coord.db", clk)
	q := mustNewQueue(t, st, clk, &bytes.Buffer{})
	q.WireMetrics(m)

	laptop := "laptop.tail-scale.ts.net."

	// Two messages in, both evicted by TTL when the clock crosses retention.
	enqueueOK(t, q, laptop, directEnv("m-1", "one", clk.Now()))
	enqueueOK(t, q, laptop, directEnv("m-2", "two", clk.Now()))
	if got := evictionValue(t, m, "ttl"); got != 0 {
		t.Fatalf("ttl evictions before sweep = %v, want 0", got)
	}

	clk.Advance(testTTL + time.Minute)
	awaitSweep(t, q)

	if got := evictionValue(t, m, "ttl"); got != 2 {
		t.Fatalf("ttl evictions after sweep = %v, want 2", got)
	}
	// The cap reason must still sit at its pre-initialised zero: a TTL sweep
	// is not a refusal, and the two reasons must never blur.
	if got := evictionValue(t, m, "size_cap"); got != 0 {
		t.Fatalf("size_cap evictions after ttl sweep = %v, want 0", got)
	}

	// Fill laptop's inbox back to the cap (the sweep emptied it), then expect
	// exactly ONE refused admission counting under size_cap.
	for i := 1; i <= testMaxSize; i++ {
		if _, err := q.Enqueue(laptop, directEnv("m-fill", "fill", clk.Now())); err != nil {
			t.Fatalf("enqueue fill %d: %v", i, err)
		}
	}
	_, err := q.Enqueue(laptop, directEnv("m-over", "over", clk.Now()))
	capErr, ok := err.(*CapacityError)
	if !ok {
		t.Fatalf("enqueue past cap: err = %v, want CapacityError", err)
	}
	if capErr.Cap != testMaxSize {
		t.Errorf("CapacityError.Cap = %d, want %d", capErr.Cap, testMaxSize)
	}
	if got := evictionValue(t, m, "size_cap"); got != 1 {
		t.Fatalf("size_cap refusals after overflow enqueue = %v, want 1", got)
	}
	if got := evictionValue(t, m, "ttl"); got != 2 {
		t.Fatalf("ttl evictions after cap refusals = %v, want unchanged 2", got)
	}
}

// Criterion 2 + 6 resolution on the data path: every depth mutation feeds ONE
// unlabelled distribution. Enqueue raises it, Ack lowers it, and no series
// anywhere names or pseudonymises a recipient.
func TestQueueDepthHistogramTracksMutations(t *testing.T) {
	m := obs.NewMetrics()
	clk := testClock()
	st := mustOpenStore(t, t.TempDir()+"/coord.db", clk)
	q := mustNewQueue(t, st, clk, &bytes.Buffer{})
	q.WireMetrics(m)

	laptop := "laptop.tail-scale.ts.net."
	phone := "phone.tail-scale.ts.net."

	enqueueOK(t, q, laptop, directEnv("m-a1", "a1", clk.Now()))
	enqueueOK(t, q, laptop, directEnv("m-a2", "a2", clk.Now()))
	enqueueOK(t, q, phone, directEnv("m-b1", "b1", clk.Now()))

	count, sum := depthObservations(t, m)
	if count != 3 {
		t.Fatalf("depth observations = %d, want 3 (one per enqueue)", count)
	}
	if sum != 4 { // observed post-insert depths 1 + 2 + 1 across recipients
		t.Fatalf("depth observation sum = %v, want 4", sum)
	}

	// Ack drains laptop entirely: one more observation at depth 0.
	if err := q.Ack(laptop, 2); err != nil {
		t.Fatalf("ack: %v", err)
	}
	count, sum = depthObservations(t, m)
	if count != 4 {
		t.Fatalf("depth observations after ack = %d, want 4", count)
	}
	if sum != 4 { // +0 for the drained inbox
		t.Fatalf("depth observation sum after ack = %v, want 4", sum)
	}

	// And the family stays label-free after all mutations — the criterion-6
	// property asserted on live data, not just at construction.
	families, _ := m.Registry().Gather()
	for _, f := range families {
		if f.GetName() != "walkie_queue_depth" {
			continue
		}
		for _, label := range f.GetMetric()[0].GetLabel() {
			t.Errorf("queue depth grew label %q; criterion 6 forbids any label space here", label.GetName())
		}
	}
}

// A nil-wired queue (every pre-observability rig) must run its mutations
// without touching metrics at all.
func TestQueueWithoutMetricsStillWorks(t *testing.T) {
	clk := testClock()
	st := mustOpenStore(t, t.TempDir()+"/coord.db", clk)
	q := mustNewQueue(t, st, clk, &bytes.Buffer{})

	enqueueOK(t, q, "laptop.tail-scale.ts.net.", directEnv("m-1", "one", clk.Now()))
	if err := q.Ack("laptop.tail-scale.ts.net.", 1); err != nil {
		t.Fatalf("ack without metrics wired: %v", err)
	}
}
