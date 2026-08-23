package coordinator

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	"github.com/maleolabs/walkie/internal/messagehub"
	"github.com/maleolabs/walkie/internal/obs"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// discardLogger swallows server logs for tests that assert on metrics, not
// on output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The tests in this file pin ts:observability-baseline's coordinator-side
// metric hooks. Every value is asserted through the prometheus test utilities
// against the registry a scraper would read — never against internal fields —
// and every wire-level cause is established by causality (an observed frame,
// an observed presence transition), never by sleeping.

// metricValue reads one metric's value out of m's registry the way a scraper
// would: gather, find the family, filter by label values. Fails when the
// series is absent — an absent series is exactly what criterion 3 forbids
// confusing with zero.
func metricValue(t *testing.T, m *obs.Metrics, name string, wantLabels map[string]string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range metric.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			match := true
			for k, v := range wantLabels {
				if labels[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			switch {
			case metric.GetCounter() != nil:
				return metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				return metric.GetGauge().GetValue()
			default:
				t.Fatalf("%s: unsupported metric type for this helper", name)
			}
		}
	}
	t.Fatalf("series %s %v not found in exposition", name, wantLabels)
	return 0
}

// startMetricsServer runs a bare Server (no tracker, no sink) on the
// simulated tailnet address with metrics wired, for gate-level tests that do
// not need the full presence rig.
func startMetricsServer(t *testing.T) (m *obs.Metrics, addr string, cancel func()) {
	t.Helper()

	m = obs.NewMetrics()
	logger := discardLogger()
	clk := clock.NewFake(rigEpoch)

	ln, err := net.Listen("tcp4", simulatedTailnetAddr+":0")
	if err != nil {
		t.Fatalf("bind %s: %v", simulatedTailnetAddr, err)
	}

	srv := NewServer(tsauth.NewStaticResolver(nil), clk, logger, nil, nil)
	srv.WireMetrics(m)

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ctx, ln); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	return m, ln.Addr().String(), func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down within grace")
		}
	}
}

// Criterion 2: authentication refusals are counted — both refusal classes at
// the identity gate (unresolvable identity here; malformed remote addresses
// share the same hook).
func TestAuthRefusalsCounted(t *testing.T) {
	m, addr, cancel := startMetricsServer(t)
	defer cancel()

	ctx, timeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer timeout()

	if got := metricValue(t, m, "walkie_auth_refusals_total", nil); got != 0 {
		t.Fatalf("auth refusals before any dial = %v, want 0", got)
	}

	_, resp, err := websocket.Dial(ctx, "ws://"+addr, nil)
	if err == nil {
		t.Fatal("dial without resolvable identity succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("refusal = (resp %v, err %v), want HTTP 403", resp, err)
	}

	if got := metricValue(t, m, "walkie_auth_refusals_total", nil); got != 1 {
		t.Fatalf("auth refusals after refused dial = %v, want 1", got)
	}
}

// Criterion 2 + 6 together: connected devices is a single unlabelled gauge.
// It follows the ROUTING table — up when a device holds a live route, down
// once its teardown has been OBSERVED (the offline broadcast is the causal
// signal that the handler's deferred unwinding, route removal included, ran).
func TestConnectedDevicesGaugeFollowsRoutes(t *testing.T) {
	rig := startPresenceRig(t)
	m := obs.NewMetrics()
	rig.srv.WireMetrics(m) // before any client connects: no mid-flight wiring

	if got := metricValue(t, m, "walkie_connected_devices", nil); got != 0 {
		t.Fatalf("connected devices before any connection = %v, want 0", got)
	}

	laptop := rig.connectDevice(t, laptopName)
	phone := rig.connectDevice(t, phoneName)
	_ = phone

	if got := metricValue(t, m, "walkie_connected_devices", nil); got != 2 {
		t.Fatalf("connected devices with two routes = %v, want 2", got)
	}

	phone.teardown(t)
	// The OFFLINE update arriving at laptop proves phone's handler fully
	// unwound — removeRoute (which lowers the gauge) runs BEFORE
	// ConnectionLost in the defer chain, so observing the latter implies the
	// former. No sleep, pure causality. Phone's earlier ONLINE update is
	// still queued on laptop's stream, so skip forward to the OFFLINE line.
	for {
		got := laptop.nextPresence(t)
		if got.GetDevice() == phoneName && got.GetState() == walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
			break
		}
		// Anything else (phone's earlier ONLINE line) is legitimate queued
		// chatter; keep reading. nextPresence's own timeout bounds the loop.
	}

	if v := metricValue(t, m, "walkie_connected_devices", nil); v != 1 {
		t.Fatalf("connected devices after observed teardown = %v, want 1", v)
	}
}

// Criterion 2: reconnect rate as a counter. A device's SECOND connection in
// this process's lifetime counts; its first does not. The count is only
// meaningful after the full accept path ran, proven by completing a handshake.
func TestReconnectCounterCountsRepeatConnections(t *testing.T) {
	rig := startPresenceRig(t)
	m := obs.NewMetrics()
	rig.srv.WireMetrics(m)

	first := rig.connectDevice(t, laptopName)
	// connectDevice returns only after the handshake completed, so the full
	// accept path — noteConnection included — has run by this point.
	if got := metricValue(t, m, "walkie_reconnects_total", nil); got != 0 {
		t.Fatalf("reconnects after first-ever connection = %v, want 0", got)
	}

	first.teardown(t)

	second := rig.connectDevice(t, laptopName)
	_ = second
	if got := metricValue(t, m, "walkie_reconnects_total", nil); got != 1 {
		t.Fatalf("reconnects after re-connection = %v, want 1", got)
	}
}

// Criterion 2: messages-per-second rides an exported counter, incremented
// ONCE per accepted message regardless of fanout width — a broadcast to two
// devices is one message, so the scraper's rate means messages/second.
func TestMessagesCounterCountsIngressNotFanout(t *testing.T) {
	rig := startPresenceRig(t)
	m := obs.NewMetrics()
	rig.srv.WireMetrics(m)

	laptop := rig.connectDevice(t, laptopName)
	phone := rig.connectDevice(t, phoneName)
	tablet := rig.connectDevice(t, tabletName)
	_ = tablet

	hub := messagehub.New(laptopName, rig.clk, nil)

	env, err := hub.SendDirect(phoneName, "dm for the counter")
	if err != nil {
		t.Fatalf("compose dm: %v", err)
	}
	laptop.send(t, env)
	if got := nextTextEnvelope(t, phone); got.GetDirectMessage() == nil {
		t.Fatalf("phone got %T, want DirectMessage", got.GetPayload())
	}
	if got := metricValue(t, m, "walkie_messages_total", nil); got != 1 {
		t.Fatalf("messages after one direct = %v, want 1", got)
	}

	benv, err := hub.SendBroadcast("broadcast for the counter")
	if err != nil {
		t.Fatalf("compose broadcast: %v", err)
	}
	laptop.send(t, benv)
	// Both other devices receive it: two delivered copies...
	for _, watcher := range []*testClient{phone, tablet} {
		if got := nextTextEnvelope(t, watcher); got.GetBroadcastMessage() == nil {
			t.Fatalf("%s got %T, want BroadcastMessage", watcher.name, got.GetPayload())
		}
	}
	// ...but still ONE ingressed message each, so two total.
	if got := metricValue(t, m, "walkie_messages_total", nil); got != 2 {
		t.Fatalf("messages after one direct + one broadcast fanned to two devices = %v, want 2 (ingress-counted)", got)
	}
}

// Criterion 3, wiring half: the direct-vs-relay counter is present on a live
// server's registry with both modes at zero before any phase-2 code exists to
// increment them. The increment path itself is pinned in internal/obs.
func TestDataPlanePathsPresentOnLiveCoordinator(t *testing.T) {
	m, _, cancel := startMetricsServer(t)
	defer cancel()

	if got := metricValue(t, m, "walkie_data_plane_paths_total", map[string]string{"mode": "direct"}); got != 0 {
		t.Fatalf("direct paths = %v, want exported-at-zero from MVP onward", got)
	}
	if got := metricValue(t, m, "walkie_data_plane_paths_total", map[string]string{"mode": "relay"}); got != 0 {
		t.Fatalf("relay paths = %v, want exported-at-zero from MVP onward", got)
	}

	// And the family really is there even though nothing ever wrote to it:
	// re-gather and demand exactly two labelled series.
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var series int
	for _, f := range families {
		if f.GetName() == "walkie_data_plane_paths_total" {
			series = len(f.GetMetric())
		}
	}
	if series != 2 {
		t.Fatalf("data plane paths exported %d series, want 2 (direct + relay)", series)
	}
}
