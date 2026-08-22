package presence

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// TestStatusSurvivesReconnect is criterion 4: the label a user set must be
// exactly where they left it when their device reconnects — through both a
// disconnect/reconnect cycle and, separately, a full tracker restart (the
// restart case has its own test in tracker_test.go; this one pins the
// reconnect half without conflating the two).
func TestStatusSurvivesReconnect(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)

	tr.ConnectionEstablished("phone")
	if err := tr.SetStatus("phone", "out climbing"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	// Device drops and comes back.
	tr.ConnectionLost("phone")
	f.Advance(2 * time.Minute) // well past the TTL: it even expired meanwhile
	tr.ConnectionEstablished("phone")

	e := entryOf(t, tr.Snapshot(), "phone")
	if !e.Online {
		t.Fatal("reconnected device should be online")
	}
	if e.Status != "out climbing" {
		t.Fatalf("status after reconnect = %q, want %q — criterion 4 broken", e.Status, "out climbing")
	}
}

// TestSetStatusBoundaries enforces the receiver-side bound from
// ts:protocol-schema-v1's PresenceStatusChange: at most 256 bytes of UTF-8,
// REJECTED rather than truncated, because truncation would publish text the
// user did not write.
func TestSetStatusBoundaries(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)

	exactly256 := strings.Repeat("ä", 128) // 128 runes × 2 bytes = 256 bytes
	if err := tr.SetStatus("phone", exactly256); err != nil {
		t.Fatalf("256-byte status rejected: %v", err)
	}

	tooLong := strings.Repeat("ä", 129) // 258 bytes
	if err := tr.SetStatus("phone", tooLong); !errors.Is(err, ErrStatusTooLong) {
		t.Fatalf("258-byte status: err = %v, want ErrStatusTooLong", err)
	}

	if err := tr.SetStatus("phone", "\xff\xfe invalid"); !errors.Is(err, ErrStatusNotUTF8) {
		t.Fatalf("invalid UTF-8: err = %v, want ErrStatusNotUTF8", err)
	}

	// A rejected status must not have disturbed the stored one.
	if e := entryOf(t, tr.Snapshot(), "phone"); e.Status != exactly256 {
		t.Fatalf("status after rejections = %q, want untouched %q", e.Status, exactly256)
	}

	// Empty clears.
	if err := tr.SetStatus("phone", ""); err != nil {
		t.Fatalf("clear status: %v", err)
	}
	if e := entryOf(t, tr.Snapshot(), "phone"); e.Status != "" {
		t.Fatalf("status after clear = %q, want empty", e.Status)
	}
}

// TestSetStatusDoesNotTouchLiveness keeps the label side in its lane: a
// status change on an offline device leaves it offline (and must never
// fabricate an online transition), and the emitted change carries the
// CURRENT fact alongside the new label so clients can re-render the whole
// roster line from one event.
func TestSetStatusDoesNotTouchLiveness(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)
	sub := tr.Subscribe()
	defer sub.Close()

	// Offline device, known only by its label.
	if err := tr.SetStatus("tablet", "charging"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	ch := <-sub.C()
	if ch.Online {
		t.Fatalf("status change reported online=%v for a device that was never observed alive", ch.Online)
	}
	if ch.Status != "charging" {
		t.Fatalf("status change carried %q, want %q", ch.Status, "charging")
	}

	e := entryOf(t, tr.Snapshot(), "tablet")
	if e.Online {
		t.Fatal("setting a status made an offline device online — label leaking into fact")
	}

	// And liveness transitions carry the label back the other way: the
	// roster line renders from one event either direction.
	tr.ObserveHeartbeat("tablet")
	ch = <-sub.C()
	if !ch.Online || ch.Status != "charging" {
		t.Fatalf("online change = %+v, want online with preserved label", ch)
	}
}
