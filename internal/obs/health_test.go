package obs

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Criterion 4: coordinator and store readiness are reported SEPARATELY, and
// the process-up-but-store-down case is distinguishable from the response
// alone — status code AND body, asserted here against the real handler.

func getBody(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func TestHealthzAllReady(t *testing.T) {
	h := NewHandler(NewMetrics(), &Health{
		CoordinatorReady: func() bool { return true },
		StoreReady:       func() error { return nil },
	})

	code, body := getBody(t, h, "/healthz")
	if code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200 when everything is ready", code)
	}
	var parsed healthBody
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("/healthz body not JSON: %v (%q)", err, body)
	}
	if parsed.Status != "ok" {
		t.Errorf("status = %q, want ok", parsed.Status)
	}
	if parsed.Checks["coordinator"] != "ready" || parsed.Checks["store"] != "ready" {
		t.Errorf("checks = %v, want both ready", parsed.Checks)
	}
}

// The case criterion 4 exists for: process up, store down. The response must
// name the failing component and only that component.
func TestHealthzStoreDownDistinguishableFromCoordinatorDown(t *testing.T) {
	storeDown := &Health{
		CoordinatorReady: func() bool { return true },
		StoreReady:       func() error { return errors.New("disk on fire") },
	}
	code, body := getBody(t, NewHandler(NewMetrics(), storeDown), "/healthz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("store-down /healthz = %d, want 503", code)
	}
	var parsed healthBody
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("body not JSON: %v (%q)", err, body)
	}
	if parsed.Checks["store"] != "unavailable" {
		t.Errorf("store check = %q, want unavailable", parsed.Checks["store"])
	}
	if parsed.Checks["coordinator"] != "ready" {
		t.Errorf("coordinator check = %q, want ready: a store failure must not smear onto the control plane's state", parsed.Checks["coordinator"])
	}

	// The mirror case: coordinator down, store fine. Same split, other half.
	coordDown := &Health{
		CoordinatorReady: func() bool { return false },
		StoreReady:       func() error { return nil },
	}
	code, body = getBody(t, NewHandler(NewMetrics(), coordDown), "/healthz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("coordinator-down /healthz = %d, want 503", code)
	}
	if !strings.Contains(body, `"coordinator":"not_ready"`) || !strings.Contains(body, `"store":"ready"`) {
		t.Errorf("coordinator-down body = %s, want coordinator not_ready with store still ready", body)
	}
}

// Store error TEXT must not reach the response body: every tailnet peer can
// read this endpoint, and driver errors can embed paths and internals. The
// body says WHICH half failed; logs say why.
func TestHealthzDoesNotEchoStoreErrorText(t *testing.T) {
	h := &Health{
		CoordinatorReady: func() bool { return true },
		StoreReady:       func() error { return errors.New("secret-ish detail: /srv/walkie/state/coordinator.db locked") },
	}
	_, body := getBody(t, NewHandler(NewMetrics(), h), "/healthz")
	if strings.Contains(body, "secret-ish") || strings.Contains(body, "coordinator.db") {
		t.Errorf("health body leaked store error text: %s", body)
	}
}

// Liveness (/livez) answers 200 while the process can answer at all and
// probes nothing — even with the store down. That asymmetry IS the design:
// readiness fails, liveness does not crash-loop the process.
func TestLivezProbesNothing(t *testing.T) {
	h := NewHandler(NewMetrics(), &Health{
		CoordinatorReady: func() bool { return false },
		StoreReady:       func() error { return errors.New("down") },
	})
	code, body := getBody(t, h, "/livez")
	if code != http.StatusOK {
		t.Fatalf("/livez = %d, want 200 regardless of component state", code)
	}
	if !strings.HasPrefix(body, "ok") {
		t.Errorf("/livez body = %q, want ok", body)
	}
}
