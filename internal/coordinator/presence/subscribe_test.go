package presence

import (
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// TestSubscribeDeliversTransitionsAndUnsubscribeStops pins the seam slice 2's
// broadcast hub consumes: every transition arrives exactly once, in order,
// and after Close the subscription goes silent — a closed subscriber must
// never receive anything, including changes caused before its Close landed.
func TestSubscribeDeliversTransitionsAndUnsubscribeStops(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)

	sub := tr.Subscribe()

	tr.ObserveHeartbeat("phone")
	if err := tr.SetStatus("phone", "on a call"); err != nil {
		t.Fatalf("set status: %v", err)
	}
	f.Advance(testTTL + time.Second) // expiry is async; the receive below syncs

	want := []Change{
		{Device: "phone", Online: true, LastSeen: epoch, Status: ""},
		{Device: "phone", Online: true, LastSeen: epoch, Status: "on a call"},
		{Device: "phone", Online: false, LastSeen: epoch, Status: "on a call"},
	}
	for i, w := range want {
		got := <-sub.C()
		if got.Device != w.Device || got.Online != w.Online || got.Status != w.Status || !got.LastSeen.Equal(w.LastSeen) {
			t.Fatalf("change %d = %+v, want %+v", i, got, w)
		}
	}

	sub.Close()

	// After Close nothing more may be delivered — and since every notify
	// happens synchronously inside the causing call, this check is
	// deterministic rather than timing-dependent.
	tr.ObserveHeartbeat("phone")
	assertNoChange(t, sub)
}

// TestSlowSubscriberDropsWithoutBlocking pins the drop policy: delivery is
// non-blocking, so one stalled consumer can neither stall the tracker (whose
// notify runs under the same mutex as every observation) nor grow memory
// without bound. Presence events are idempotent statements of current state
// — the next heartbeat or TTL event re-states everything — so a dropped
// change self-heals within one TTL; that argument lives on Subscribe and
// this test holds the non-blocking half of it honest.
func TestSlowSubscriberDropsWithoutBlocking(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)

	sub := tr.Subscribe() // buffer of subscriptionBuffer
	defer sub.Close()

	// Far more transitions than the buffer holds. If any send ever blocked,
	// this loop would deadlock the test against itself — the failure mode
	// is loud, not flaky.
	for i := 0; i < subscriptionBuffer*4; i++ {
		status := ""
		if i%2 == 0 {
			status = "x"
		}
		if err := tr.SetStatus("phone", status); err != nil {
			t.Fatalf("set status %d: %v", i, err)
		}
	}

	// The tracker survived; the buffered prefix was delivered; the rest was
	// dropped rather than queued without bound.
	delivered := 0
drain:
	for {
		select {
		case <-sub.C():
			delivered++
		default:
			break drain
		}
	}
	if delivered == 0 || delivered > subscriptionBuffer {
		t.Fatalf("delivered %d changes, want between 1 and the %d-byte buffer", delivered, subscriptionBuffer)
	}

	// And the view stays correct regardless of what was dropped: state, not
	// history, is the contract.
	if e := entryOf(t, tr.Snapshot(), "phone"); e.Status != "" || e.Online {
		t.Fatalf("snapshot after drops = %+v, want cleared offline label state", e)
	}
}

// TestSnapshotIsSortedByDevice keeps roster rendering deterministic for
// clients and tests alike.
func TestSnapshotIsSortedByDevice(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)

	for _, d := range []string{"zeta", "alpha", "mid"} {
		tr.ConnectionEstablished(d)
		f.Advance(time.Second) // stagger observations; ordering must not care
	}

	snap := tr.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot has %d entries, want 3", len(snap))
	}
	for i := 1; i < len(snap); i++ {
		if snap[i-1].Device >= snap[i].Device {
			t.Fatalf("snapshot not sorted by device: %q before %q", snap[i-1].Device, snap[i].Device)
		}
	}
}
