package presenceview

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These tests are pure state-machine checks: no clock, no network, no sleeps.
// The view is fed envelopes exactly as the control plane will hand them over.

func onlineUpdate(device string) *walkiev1.PresenceUpdate {
	return &walkiev1.PresenceUpdate{Device: device, State: walkiev1.PresenceState_PRESENCE_STATE_ONLINE}
}

// TestApplyOnlineThenOfflineCarriesLastSeen walks one device's whole visible
// life: online (no last-seen on the wire), then offline WITH last-seen — the
// shape criterion 2 and 3 broadcasts take. The offline update must land with
// the coordinator's timestamp verbatim; the online update must not have
// clobbered or invented timestamps.
func TestApplyOnlineThenOfflineCarriesLastSeen(t *testing.T) {
	v := New(nil)

	v.Apply(onlineUpdate("phone"))

	snap := v.Snapshot()
	if len(snap) != 1 || snap[0].Device != "phone" || !snap[0].Online {
		t.Fatalf("after online apply: %+v, want phone online", snap)
	}
	if snap[0].LastSeen != 0 {
		t.Fatalf("online entry carries last-seen %d; wire omits it while online", snap[0].LastSeen)
	}

	seenAt := timestamppb.New(time.Unix(1000, 0))
	v.Apply(&walkiev1.PresenceUpdate{
		Device:   "phone",
		State:    walkiev1.PresenceState_PRESENCE_STATE_OFFLINE,
		LastSeen: seenAt,
	})

	snap = v.Snapshot()
	if snap[0].Online {
		t.Fatal("offline apply left device online")
	}
	if want := time.Unix(1000, 0).UnixNano(); snap[0].LastSeen != want {
		t.Fatalf("last-seen = %d, want coordinator's %d", snap[0].LastSeen, want)
	}
}

// TestApplyKeepsLastSeenAcrossReconnect pins the ONLINE rule: an online
// update omits last_seen on the wire, and the view must KEEP whatever
// last-seen it had rather than zeroing history that still belongs to the
// device's record.
func TestApplyKeepsLastSeenAcrossReconnect(t *testing.T) {
	v := New(nil)

	v.Apply(&walkiev1.PresenceUpdate{
		Device:   "phone",
		State:    walkiev1.PresenceState_PRESENCE_STATE_OFFLINE,
		LastSeen: timestamppb.New(time.Unix(500, 0)),
	})
	v.Apply(onlineUpdate("phone"))

	snap := v.Snapshot()
	if !snap[0].Online {
		t.Fatal("device should be online after reconnect broadcast")
	}
	if want := time.Unix(500, 0).UnixNano(); snap[0].LastSeen != want {
		t.Fatalf("last-seen = %d, want preserved %d", snap[0].LastSeen, want)
	}
}

// TestStatusTravelsWithUpdates: the status label rides every PresenceUpdate;
// Apply must store it whether the device is going online or offline.
func TestStatusTravelsWithUpdates(t *testing.T) {
	v := New(nil)

	up := onlineUpdate("tablet")
	up.Status = "out climbing"
	v.Apply(up)

	if got := v.Snapshot()[0].Status; got != "out climbing" {
		t.Fatalf("status = %q, want %q", got, "out climbing")
	}
}

// TestUnknownStateIgnoredKeepsPriorVerdict covers version skew: a state value
// this build does not know must degrade to no-op on liveness (adr:003), never
// to a guessed verdict. The status label still applies — its meaning does not
// depend on the enum's evolution.
func TestUnknownStateIgnoredKeepsPriorVerdict(t *testing.T) {
	v := New(nil)
	v.Apply(onlineUpdate("phone"))

	v.Apply(&walkiev1.PresenceUpdate{
		Device: "phone",
		State:  walkiev1.PresenceState(99), // a future build's new state
		Status: "roaming",
	})

	snap := v.Snapshot()
	if !snap[0].Online {
		t.Fatal("unknown state flipped liveness; must be ignored")
	}
	if snap[0].Status != "roaming" {
		t.Fatalf("status = %q, want label applied despite unknown state", snap[0].Status)
	}
}

// TestSnapshotSortedAndIdempotent: roster renders deterministically, and
// re-applying the same update changes nothing (the property that makes
// transport-level duplicates harmless).
func TestSnapshotSortedAndIdempotent(t *testing.T) {
	v := New(nil)
	for _, name := range []string{"zeta", "alpha", "mid"} {
		v.Apply(onlineUpdate(name))
	}

	snap := v.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot has %d entries, want 3", len(snap))
	}
	for i, want := range []string{"alpha", "mid", "zeta"} {
		if snap[i].Device != want {
			t.Fatalf("snapshot[%d] = %s, want %s", i, snap[i].Device, want)
		}
	}

	before := v.Snapshot()
	v.Apply(onlineUpdate("mid")) // duplicate of current state
	after := v.Snapshot()
	if len(before) != len(after) || before[1] != after[1] {
		t.Fatalf("re-apply changed the view:\nbefore %+v\nafter  %+v", before, after)
	}
}

// TestSubscriptionDeliversChangesUntilClose: the seam sto:terminal-ui will
// consume — subscribe, receive per change, close stops delivery.
func TestSubscriptionDeliversChangesUntilClose(t *testing.T) {
	v := New(nil)
	sub := v.Subscribe()

	v.Apply(onlineUpdate("phone"))
	got := <-sub.C()
	if got.Device != "phone" || !got.Online {
		t.Fatalf("first notification = %+v, want phone online", got)
	}

	sub.Close()
	v.Apply(&walkiev1.PresenceUpdate{
		Device: "phone",
		State:  walkiev1.PresenceState_PRESENCE_STATE_OFFLINE,
	})
	// A closed subscription's channel yields only zero values; receiving one
	// with ok==false proves silence, same contract as the tracker side.
	if d, ok := <-sub.C(); ok {
		t.Fatalf("notification after Close: %+v", d)
	}
}

// TestDroppedNotificationIsLoggedNotFatal: a full subscriber buffer drops
// quietly-except-for-the-log, naming the device only — no content, matching
// the house log-hygiene rule.
func TestDroppedNotificationIsLoggedNotFatal(t *testing.T) {
	logs := &bytes.Buffer{}
	v := New(slog.New(slog.NewTextHandler(logs, nil)))

	slow := v.Subscribe() // nobody drains it

	// Alternate states so every Apply is a real change and really notifies —
	// re-applying identical state is suppressed as a no-op by design.
	for i := 0; i <= subscriptionBuffer+1; i++ {
		if i%2 == 0 {
			v.Apply(onlineUpdate("phone"))
		} else {
			v.Apply(&walkiev1.PresenceUpdate{Device: "phone", State: walkiev1.PresenceState_PRESENCE_STATE_OFFLINE})
		}
	}

	out := logs.String()
	if !bytes.Contains([]byte(out), []byte("subscriber buffer full")) {
		t.Fatalf("drop not logged; output:\n%s", out)
	}
	if bytes.Contains([]byte(out), []byte("status=")) && bytes.Contains([]byte(out), []byte("out climbing")) {
		t.Fatal("log carried status content")
	}
	slow.Close()
}
