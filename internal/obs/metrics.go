package obs

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the coordinator's metric surface (ts:observability-baseline
// criteria 2, 3 and 6). One struct, one private registry: the registry is
// per-instance rather than prometheus.DefaultRegisterer because global
// registration leaks between tests and makes double-wiring a panic instead of
// a bug someone sees. The HTTP handler for it lives in health.go's mux
// (/metrics); the listener it serves on is injected in server.go and is bound
// tailnet-only — criterion 5, verified by test.
//
// # Every method tolerates a nil receiver
//
// Wiring metrics is optional at the type level (WireMetrics in the coordinator
// and queue packages): tests of other machinery pass nothing and pay nothing.
// A nil *Metrics is a no-op everywhere, so no call site needs an if — the
// guard lives here once.
//
// # Criterion 2 versus criterion 6 — the deliberate resolution
//
// Criterion 2 asks for "queue depth per recipient"; criterion 6 forbids any
// label that turns this endpoint into a presence side channel. Those pull
// against each other, and the resolution chosen here is:
//
//	Queue depth is exported as a HISTOGRAM ACROSS RECIPIENTS, with NO label
//	at all — not a labelled gauge per recipient, and not a gauge keyed by an
//	opaque pseudonym.
//
// Why not the opaque-id alternative: a stable non-reversible id still loses
// to a trivial de-anonymisation attack available to every tailnet peer (the
// metrics endpoint is readable by the whole tailnet, by design). An attacker
// sends a direct message addressed to the victim while the victim is offline;
// exactly one per-recipient series increments; the attacker has now bound the
// opaque id to the victim's name AND learned the victim was offline — the
// side channel criterion 6 exists to forbid, reconstructed through the write
// path. A histogram has no series to bind: probing reveals only that SOME
// bucket moved, indistinguishable from background chatter.
//
// What an endpoint observer CAN infer: the total number of retained messages
// (histogram sum), how many recipients currently hold queued mail (count),
// and the shape of the depth distribution (one hoarder vs even spread).
//
// What an observer CANNOT infer: which device holds mail, how deep ANY named
// or pseudonymous device's queue is, whether a specific device is offline
// (enqueue-probing moves aggregate buckets only), or any per-device time
// series whatsoever. There is no label space to correlate against.
//
// The cost is honest and accepted: an operator cannot answer "is device X's
// queue stuck?" from metrics alone — they answer it from logs (which carry
// device names and positions, content-free) or the control socket. Per-
// recipient precision was traded for structural privacy, not lost to sloppiness.

// Metrics carries one registry and every counter/gauge/histogram the
// coordinator exports. Construct with NewMetrics; wire with WireMetrics on
// coordinator.Server and queue.Queue.
type Metrics struct {
	registry *prometheus.Registry

	connectedDevices prometheus.Gauge
	queueDepth       prometheus.Histogram
	queueEvictions   *prometheus.CounterVec
	messagesTotal    prometheus.Counter
	authRefusals     prometheus.Counter
	reconnects       prometheus.Counter
	dataPlanePaths   *prometheus.CounterVec
}

// NewMetrics builds the metric set on a fresh private registry.
//
// Both labelled counters are PRE-INITIALISED to zero across their label
// values. This is criterion 3 made literal: a Prometheus scrape must see
// walkie_data_plane_paths_total{mode="direct"} 0 from the first MVP scrape,
// not an absent series — an absent series and a zero series look identical in
// some dashboards and opposite in others, and this item exists so nobody ever
// has to wonder which one they are looking at.
func NewMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		connectedDevices: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "walkie_connected_devices",
			Help: "Devices with at least one live routed control connection.",
		}),
		queueDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "walkie_queue_depth",
			Help: "Per-recipient offline-queue depth, observed as a distribution ACROSS recipients (criterion-2-vs-6 resolution: no labels; see the type comment for what an observer can and cannot infer).",
			// Buckets span 1..256 messages: the default size cap is 256
			// (cmd/walkie-coordinator), so the top bucket bounds any legal
			// per-recipient backlog. Exponential from 1 keeps the low end —
			// where human-scale queues live — resolved.
			Buckets: prometheus.ExponentialBuckets(1, 2, 9), // 1,2,4,...,256
		}),
		queueEvictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "walkie_queue_evictions_total",
			Help: "Offline-queue removals by reason. ttl: messages evicted past retention (bounded retention working as designed). size_cap: admissions REFUSED at the cap — counts refusals, not removals; nothing retained was deleted.",
		}, []string{"reason"}),
		messagesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "walkie_messages_total",
			Help: "Valid messages accepted at ingress (direct + broadcast). A rate scraper computes messages/second from this counter; the coordinator keeps no windowed average of its own.",
		}),
		authRefusals: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "walkie_auth_refusals_total",
			Help: "Connections refused at the identity gate (adr:004 WhoIs lookup failed or remote address unusable).",
		}),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "walkie_reconnects_total",
			Help: "Connections accepted from devices ALREADY seen connected during this process's lifetime. A rate scraper computes reconnects/second; the counter resets at process start by Prometheus convention.",
		}),
		dataPlanePaths: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "walkie_data_plane_paths_total",
			Help: "Data-plane paths by transport mode. mode=direct is EXPECTED to read zero throughout the MVP: all traffic relays through the coordinator until phase 2 delivers the direct UDP data plane (adr:001 hybrid topology). Zero direct paths in the MVP is the designed state, NOT a fault — see ts:docs-quickstart-runbook for interpretation, and ses:planning-2026-08-20 for why this counter is mandatory.",
		}, []string{"mode"}),
	}

	m.registry.MustRegister(
		m.connectedDevices,
		m.queueDepth,
		m.queueEvictions,
		m.messagesTotal,
		m.authRefusals,
		m.reconnects,
		m.dataPlanePaths,
	)

	// Pre-initialise both series of each labelled counter (see the NewMetrics
	// comment): exported-at-zero, never absent.
	m.queueEvictions.WithLabelValues("ttl")
	m.queueEvictions.WithLabelValues("size_cap")
	m.dataPlanePaths.WithLabelValues("direct")
	m.dataPlanePaths.WithLabelValues("relay")

	return m
}

// Registry exposes the private registry for the /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return prometheus.NewRegistry()
	}
	return m.registry
}

// SetConnectedDevices publishes the number of devices with at least one live
// routed connection. Set (not incremented): the value is a point-in-time
// count, recomputed from the routing table on every route change.
func (m *Metrics) SetConnectedDevices(n int) {
	if m == nil {
		return
	}
	m.connectedDevices.Set(float64(n))
}

// ObserveQueueDepth records one recipient's current queue depth into the
// cross-recipient distribution. Called after every mutation that can move a
// depth (enqueue, ack deletion, TTL eviction) so the histogram tracks the
// live backlog rather than a stale snapshot.
func (m *Metrics) ObserveQueueDepth(depth int) {
	if m == nil || depth < 0 {
		return
	}
	m.queueDepth.Observe(float64(depth))
}

// IncQueueEviction counts one bounded-retention event. reason is "ttl" for a
// message evicted past retention, "size_cap" for an admission refused at the
// cap (a refusal, not a removal — the help string says so too).
func (m *Metrics) IncQueueEviction(reason string) {
	if m == nil {
		return
	}
	m.queueEvictions.WithLabelValues(reason).Inc()
}

// IncMessages counts one valid message accepted at ingress. Rates are the
// scraper's job (the help string promises this); the coordinator maintains no
// windowed average.
func (m *Metrics) IncMessages() {
	if m == nil {
		return
	}
	m.messagesTotal.Inc()
}

// IncAuthRefusal counts one connection refused at the identity gate.
func (m *Metrics) IncAuthRefusal() {
	if m == nil {
		return
	}
	m.authRefusals.Inc()
}

// IncReconnect counts one accepted connection from a device already seen
// connected in this process's lifetime.
func (m *Metrics) IncReconnect() {
	if m == nil {
		return
	}
	m.reconnects.Inc()
}

// ObserveDataPlanePath counts one data-plane path by transport mode. In the
// MVP nothing calls this with "direct" — the direct UDP plane is phase 2 —
// and the zero reading is the deliverable (criterion 3; ses:planning-2026-08-20).
// The method exists NOW so phase-2 code has exactly one honest place to
// report through, and so this file documents the semantics the phase-2
// implementation must honour: increment per path FORMATION, not per packet.
func (m *Metrics) ObserveDataPlanePath(mode string) {
	if m == nil {
		return
	}
	m.dataPlanePaths.WithLabelValues(mode).Inc()
}
