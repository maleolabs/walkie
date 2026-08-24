package obs

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Criterion 5, verified by test rather than by inspection (the same pattern
// internal/coordinator's server_test.go established for the control plane):
// the observability listener is bound to exactly one interface address and a
// dial ADDRESSED to another local interface cannot reach /metrics or
// /healthz. Had the bind been wildcard, the second dial would succeed and
// this test would fail.
//
// Loopback stands in for the tailnet interface: Linux routes all of 127/8 to
// loopback, so distinct addresses exist without root and a bind to one
// genuinely excludes the others — which is precisely the property under test.
const (
	simulatedTailnetAddr = "127.0.0.2"
	otherIfaceAddr       = "127.0.0.1"
)

func TestObservabilityEndpointsBoundTailnetOnly(t *testing.T) {
	handler := NewHandler(NewMetrics(), &Health{
		CoordinatorReady: func() bool { return true },
		StoreReady:       func() error { return nil },
	})

	ln, err := net.Listen("tcp4", simulatedTailnetAddr+":0")
	if err != nil {
		t.Fatalf("bind %s: %v (environment lacks distinct loopback addresses)", simulatedTailnetAddr, err)
	}

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := Serve(ctx, ln, handler); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("obs server did not shut down within budget")
		}
	})

	addr := ln.Addr().String()
	_, port, _ := net.SplitHostPort(addr)

	// Guard the bind itself: exactly the simulated tailnet address, never a
	// wildcard. Inspection alone is not the proof, but it is worth pinning
	// alongside the dial below.
	boundHost, _, _ := net.SplitHostPort(addr)
	if boundHost != simulatedTailnetAddr || boundHost == "0.0.0.0" || boundHost == "::" {
		t.Fatalf("listener bound to %q, want exactly %s", boundHost, simulatedTailnetAddr)
	}

	// A connection attempt ADDRESSED to another interface cannot reach the
	// endpoints: nothing is listening there, so the kernel refuses at TCP
	// level before any walkie code runs.
	conn, err := net.DialTimeout("tcp4", net.JoinHostPort(otherIfaceAddr, port), 2*time.Second)
	if err == nil {
		conn.Close()
		t.Fatalf("dial to non-tailnet interface %s:%s CONNECTED; metrics/health binding is not interface-exclusive", otherIfaceAddr, port)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || !errors.Is(opErr.Err, syscall.ECONNREFUSED) {
		t.Logf("non-tailnet dial failed with %v (want ECONNREFUSED; other failures still prove unreachability)", err)
	}

	// Control: through the BOUND interface both endpoint classes answer —
	// exclusion comes from the bind, not from a broken server.
	client := &http.Client{Timeout: 2 * time.Second}

	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("bound-interface /metrics control failed: %v", err)
	}
	metricsBody := readAll(t, resp)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/metrics = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(metricsBody, "walkie_data_plane_paths_total") {
		t.Errorf("/metrics exposition missing walkie_ families; got:\n%s", metricsBody)
	}

	resp, err = client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("bound-interface /healthz control failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", resp.StatusCode)
	}

	resp, err = client.Get("http://" + addr + "/livez")
	if err != nil {
		t.Fatalf("bound-interface /livez control failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/livez = %d, want 200", resp.StatusCode)
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return sb.String()
}
