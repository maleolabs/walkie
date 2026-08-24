package obs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Serve accepts connections on ln until ctx is cancelled, serving the given
// handler (production: the health mux with /metrics mounted — see
// cmd/walkie-coordinator).
//
// # The listener is injected, not opened here
//
// Criterion 5 of ts:observability-baseline: metrics and health bind to the
// tailnet interface only, NEVER 0.0.0.0, verified by test rather than by
// inspection. This package therefore never calls net.Listen — production
// wiring passes a tsnet listener, which binds only the node's tailnet IP,
// making the property structural exactly as internal/coordinator does for the
// control plane. binding_test.go proves it over real sockets: a dial addressed
// to another local interface cannot reach these endpoints.
//
// # Plain http.Server.Shutdown is enough here
//
// Unlike the control plane (whose WebSocket hijacking forces an explicit
// connection registry and drain window), every handler this package serves is
// ordinary request/response: Shutdown waits for them cleanly. A short fixed
// budget covers stragglers without inventing a second grace knob to tune.
func Serve(ctx context.Context, ln net.Listener, h http.Handler) error {
	hs := &http.Server{
		Handler: h,
		// Same transport hygiene as the control plane: a peer that opens a
		// socket and sends nothing must not hold a slot forever.
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- hs.Serve(ln) }()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("obs: serve %s: %w", ln.Addr(), err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := hs.Shutdown(shutdownCtx); err != nil {
			// In-flight scrapes are read-only and idempotent; a timeout here
			// costs nothing durable. Report honestly anyway.
			return fmt.Errorf("obs: shutdown: %w", err)
		}
		return nil
	}
}
