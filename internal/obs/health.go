package obs

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// This file owns criterion 4 of ts:observability-baseline: a health endpoint
// reporting coordinator readiness and store readiness SEPARATELY.
//
// # Why the split is load-bearing
//
// A single aggregated "ok" hides exactly the case an operator most needs to
// distinguish: the process is up but the store is unavailable. With an
// aggregate, both failures page identically and the first diagnostic step
// (is it the process or the database?) is a guess. Here each component
// reports its own state in every response body, and the HTTP status is
// derived from them — so a scrape, a curl, or a dashboard all see WHICH half
// is unhealthy without logging into the box.
//
// # Liveness and readiness are different questions
//
// Both are exposed, under different paths, because conflating them produces
// crash loops:
//
//   - /livez — LIVENESS. "Is this process alive?" It answers 200 as soon as
//     the handler can answer at all and probes NOTHING. A wedged store must
//     fail READINESS (and get an operator), not liveness — killing a process
//     whose only problem is a slow disk turns a degraded service into a
//     restart storm.
//   - /healthz — READINESS. "Should traffic be sent to me?" It probes each
//     component separately via the injected funcs and reports per-component
//     state. Load balancers and supervisors act on this one.
//
// The probes are injected functions rather than direct store references: obs
// stays dependency-free, tests substitute deterministic fakes, and production
// wiring decides what "store ready" means (a ping) in exactly one place.

// Health serves the coordinator's liveness and readiness endpoints.
type Health struct {
	// CoordinatorReady reports whether the control plane is wired to serve.
	// In production it flips true once the listeners are bound; it exists as
	// a function so readiness has ONE definition the binary owns.
	CoordinatorReady func() bool

	// StoreReady reports whether the persistence layer answers. Production
	// wires a database ping; a nil func reports ready — an absent probe must
	// not manufacture an outage.
	StoreReady func() error
}

// NewHealthHandler returns an http.Handler muxing /livez and /healthz.
func NewHealthHandler(h *Health) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", h.serveLive)
	mux.HandleFunc("/healthz", h.serveReady)
	return mux
}

// NewHandler returns the FULL observability surface production serves:
// /metrics (the Prometheus exposition of m's registry) alongside /livez and
// /healthz on the SAME port. One port because these endpoints share a single
// audience — tailnet operators and their scrapers — and one listener means
// one bind to prove tailnet-only (criterion 5) rather than two.
func NewHandler(m *Metrics, h *Health) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))
	mux.Handle("/livez", http.HandlerFunc(h.serveLive))
	mux.Handle("/healthz", http.HandlerFunc(h.serveReady))
	return mux
}

// healthBody is the /healthz response shape. Component states are spelled as
// short strings ("ready", "not_ready", "unavailable") rather than booleans so
// a human reading a curl output never has to remember which polarity "false"
// meant for which component.
type healthBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// serveReady implements /healthz: per-component readiness, aggregated status,
// 200 only when EVERYTHING is ready. The body always carries both components'
// states — even in failure — because the whole point of criterion 4 is that
// the failing half is nameable from the response alone.
func (h *Health) serveReady(w http.ResponseWriter, _ *http.Request) {
	checks := map[string]string{}

	coordState := "ready"
	if h.CoordinatorReady == nil || !h.CoordinatorReady() {
		coordState = "not_ready"
	}
	checks["coordinator"] = coordState

	storeState := "ready"
	if h.StoreReady != nil {
		if err := h.StoreReady(); err != nil {
			storeState = "unavailable"
			// The error TEXT is deliberately not echoed: store errors can
			// embed paths and driver internals, and the body's job is to say
			// WHICH component failed, not to leak its diagnostics to every
			// tailnet peer who can reach the endpoint. Logs carry details.
			_ = err
		}
	}
	checks["store"] = storeState

	status := "ok"
	code := http.StatusOK
	if coordState != "ready" || storeState != "ready" {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(healthBody{Status: status, Checks: checks})
}

// serveLive implements /livez: liveness only, probing nothing (see the type
// comment for why liveness must not probe the store).
func (h *Health) serveLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
