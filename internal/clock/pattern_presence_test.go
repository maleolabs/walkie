package clock

import (
	"testing"
	"time"
)

// Pattern demonstration for ts:test-harness criterion 6: presence expiry.
//
// Presence is owned by sto:device-presence, which does not exist yet; this file
// proves the substrate carries the weight — a liveness TTL refreshed by
// activity, expiring exactly when Advance crosses the deadline once refreshes
// stop, driven entirely by the fake clock. The owning item inherits the
// production version, including its defining test: SIGKILL a client and assert
// it goes offline within the TTL. A killed device cannot send a goodbye, so
// expiry must come from the clock alone — which is why this pattern is worth a
// demonstration before any presence code exists.
func TestPatternPresenceTTLExpiresExactlyWhenRefreshStops(t *testing.T) {
	const (
		ttl        = 45 * time.Second
		heartbeat  = 30 * time.Second
		heartbeats = 3
	)

	f := NewFake(epoch)

	// First heartbeat arms the deadline; every later one moves it with Reset.
	// This is the refresh shape req:device-presence needs, and the reason the
	// Clock seam grew a cancellable Timer: "move the deadline" must be atomic,
	// not stop-drain-restart bookkeeping re-derived per call site.
	peer := f.NewTimer(ttl)

	for beat := 1; beat <= heartbeats; beat++ {
		f.Advance(heartbeat)
		select {
		case <-peer.C():
			t.Fatalf("presence expired at beat %d despite continuous refresh", beat)
		default:
		}
		if !peer.Reset(ttl) {
			t.Fatalf("refresh at beat %d reported false — the previous deadline should still have been pending", beat)
		}
		if n := f.Waiters(); n != 1 {
			t.Fatalf("Waiters() = %d after beat %d, want exactly one outstanding deadline (a leaked deadline would fire a ghost offline)", n, beat)
		}
	}

	lastBeat := f.Now()

	// Activity stops — the SIGKILL case. No goodbye is sent; nothing in the
	// test nudges the timer again. Expiry must arrive from the clock alone.
	f.Advance(ttl - time.Second)
	select {
	case v := <-peer.C():
		t.Fatalf("presence expired at %v, one second before its deadline", v)
	default:
	}

	f.Advance(time.Second)
	select {
	case v := <-peer.C():
		if want := lastBeat.Add(ttl); !v.Equal(want) {
			t.Errorf("expired at %v, want exactly %v — expiry must land on the deadline Advance crosses, not near it", v, want)
		}
	default:
		t.Fatal("presence did not expire when Advance crossed the deadline")
	}

	// A dead peer stays dead: resetting after expiry reports false — the
	// signal production presence code uses to notice that the entry already
	// lapsed instead of blindly refreshing it. (Like *time.Timer.Reset, the
	// seam's Reset does rearm regardless; refusing to refresh a lapsed entry
	// is sto:device-presence policy, built on this false return.)
	if peer.Reset(ttl) {
		t.Error("Reset after expiry reported true; production code relies on false to detect the lapse")
	}
	// Tear down the rearm the Reset performed, so the test leaves no waiter.
	peer.Stop()
	if n := f.Waiters(); n != 0 {
		t.Errorf("Waiters() = %d after Stop, want 0", n)
	}
}
