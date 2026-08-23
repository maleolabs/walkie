package obs

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// Metric values are asserted through the prometheus test utilities and by
// gathering this package's own registry — never by trusting internal fields.
// That is the same discipline criterion 5 demands for binding: assert what a
// scraper would see.

// gatherFamily pulls one metric family out of the registry, failing the test
// if it is absent. An absent series is exactly the failure mode criterion 3
// guards against, so absence is a hard failure everywhere in this file.
func gatherFamily(t *testing.T, m *Metrics, name string) *dto.MetricFamily {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("metric family %q not exported at all", name)
	return nil
}

// counterValue reads the value of the single metric in a family (labelled
// families must be filtered first).
func counterValue(t *testing.T, f *dto.MetricFamily) float64 {
	t.Helper()
	if len(f.GetMetric()) != 1 {
		t.Fatalf("family %s has %d metrics, want 1", f.GetName(), len(f.GetMetric()))
	}
	return f.GetMetric()[0].GetCounter().GetValue()
}

func TestConnectedDevicesGaugeTracksSetValues(t *testing.T) {
	m := NewMetrics()
	m.SetConnectedDevices(0)
	if got := testutil.ToFloat64(m.connectedDevices); got != 0 {
		t.Fatalf("connected devices = %v, want 0", got)
	}
	m.SetConnectedDevices(3)
	if got := testutil.ToFloat64(m.connectedDevices); got != 3 {
		t.Fatalf("connected devices = %v, want 3", got)
	}
}

// Criterion 2: evictions split by TTL and size cap, both series exported from
// construction even at zero (same exported-at-zero rule as criterion 3).
func TestQueueEvictionsByReason(t *testing.T) {
	m := NewMetrics()

	// Both reasons exist BEFORE anything happened.
	if got := testutil.ToFloat64(m.queueEvictions.WithLabelValues("ttl")); got != 0 {
		t.Fatalf("ttl evictions = %v, want pre-initialised 0", got)
	}
	if got := testutil.ToFloat64(m.queueEvictions.WithLabelValues("size_cap")); got != 0 {
		t.Fatalf("size_cap evictions = %v, want pre-initialised 0", got)
	}

	m.IncQueueEviction("ttl")
	m.IncQueueEviction("ttl")
	m.IncQueueEviction("size_cap")

	if got := testutil.ToFloat64(m.queueEvictions.WithLabelValues("ttl")); got != 2 {
		t.Fatalf("ttl evictions = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.queueEvictions.WithLabelValues("size_cap")); got != 1 {
		t.Fatalf("size_cap evictions = %v, want 1", got)
	}
}

func TestMessagesAuthReconnectCounters(t *testing.T) {
	m := NewMetrics()

	m.IncMessages()
	m.IncMessages()
	m.IncMessages()
	if got := testutil.ToFloat64(m.messagesTotal); got != 3 {
		t.Fatalf("messages_total = %v, want 3", got)
	}

	m.IncAuthRefusal()
	if got := testutil.ToFloat64(m.authRefusals); got != 1 {
		t.Fatalf("auth_refusals_total = %v, want 1", got)
	}

	m.IncReconnect()
	m.IncReconnect()
	if got := testutil.ToFloat64(m.reconnects); got != 2 {
		t.Fatalf("reconnects_total = %v, want 2", got)
	}
}

// Criterion 3: the direct-versus-relay counter exists, exports BOTH modes at
// zero before any traffic, and its help string tells an operator that zero
// direct paths is the MVP's designed state — that string is what
// ts:docs-quickstart-runbook points at.
func TestDataPlanePathsExportedAtZeroWithExpectedZeroHelp(t *testing.T) {
	m := NewMetrics()

	direct := gatherFamily(t, m, "walkie_data_plane_paths_total")
	found := map[string]float64{}
	for _, metric := range direct.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "mode" {
				found[label.GetValue()] = metric.GetCounter().GetValue()
			}
		}
	}
	if v, ok := found["direct"]; !ok || v != 0 {
		t.Fatalf("mode=direct series = (%v present, value %v), want present at 0 from first scrape", ok, v)
	}
	if v, ok := found["relay"]; !ok || v != 0 {
		t.Fatalf("mode=relay series = (%v present, value %v), want present at 0 from first scrape", ok, v)
	}

	help := direct.GetHelp()
	if !strings.Contains(help, "EXPECTED") || !strings.Contains(help, "zero") {
		t.Errorf("help string must tell an operator zero direct paths is expected in the MVP; got %q", help)
	}
	if !strings.Contains(help, "ts:docs-quickstart-runbook") {
		t.Errorf("help string must point at the runbook item for interpretation; got %q", help)
	}

	// The increment path works for relay today and exists for phase 2's
	// direct reporting; nothing may skip it because it "has no data".
	m.ObserveDataPlanePath("relay")
	m.ObserveDataPlanePath("direct")
	after := gatherFamily(t, m, "walkie_data_plane_paths_total")
	for _, metric := range after.GetMetric() {
		for _, label := range metric.GetLabel() {
			want := map[string]float64{"direct": 1, "relay": 1}
			if label.GetName() == "mode" && metric.GetCounter().GetValue() != want[label.GetValue()] {
				t.Errorf("mode=%s = %v, want 1 after one observation each",
					label.GetValue(), metric.GetCounter().GetValue())
			}
		}
	}
}

// Criterion 2 + 6 resolution: queue depth is a histogram with NO labels.
// Observations land in buckets and sum; there is no per-recipient series to
// correlate, which is the whole point — asserted here by checking the family
// carries no label dimensions at all.
func TestQueueDepthHistogramIsUnlabelledDistribution(t *testing.T) {
	m := NewMetrics()

	m.ObserveQueueDepth(1)
	m.ObserveQueueDepth(5)
	m.ObserveQueueDepth(300) // above top bucket: lands in +Inf

	f := gatherFamily(t, m, "walkie_queue_depth")
	if len(f.GetMetric()) != 1 {
		t.Fatalf("queue depth family has %d metrics, want exactly ONE unlabelled series", len(f.GetMetric()))
	}
	metric := f.GetMetric()[0]
	if len(metric.GetLabel()) != 0 {
		t.Fatalf("queue depth carries labels %v; criterion 6 forbids any label space here", metric.GetLabel())
	}
	h := metric.GetHistogram()
	if h.GetSampleCount() != 3 {
		t.Fatalf("histogram count = %d, want 3 observations", h.GetSampleCount())
	}
	if h.GetSampleSum() != 306 {
		t.Fatalf("histogram sum = %v, want 306", h.GetSampleSum())
	}
	// Top legal depth (the default cap, 256) must fall inside a bounded
	// bucket, not only +Inf, or the distribution loses its head exactly when
	// a queue is fullest.
	var topBucket uint64
	for _, b := range h.GetBucket() {
		if b.GetUpperBound() < 256 {
			continue
		}
		topBucket = b.GetCumulativeCount()
		break
	}
	if topBucket < 1 {
		t.Errorf("depth 256 did not land in a bounded bucket; cap-scale backlogs vanish from the histogram")
	}
}

// Nil-receiver tolerance: every method is safe on a nil *Metrics so wiring
// stays optional and call sites stay guard-free.
func TestNilMetricsAreNoOps(t *testing.T) {
	var m *Metrics
	m.SetConnectedDevices(2)
	m.ObserveQueueDepth(4)
	m.IncQueueEviction("ttl")
	m.IncMessages()
	m.IncAuthRefusal()
	m.IncReconnect()
	m.ObserveDataPlanePath("direct")
	if m.Registry() == nil {
		t.Fatal("nil Registry() must still return a usable registry for the handler")
	}
}
