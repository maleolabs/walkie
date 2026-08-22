package presence

import (
	"errors"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/store"
)

// The tests in this file are driven entirely by *clock.Fake: no test sleeps
// in real time (ts:test-harness). The one asynchronous transition — TTL
// expiry, which fires on a watcher goroutine — is synchronized through a
// Subscription receive: the change event is emitted inside the same mutex
// critical section that mutates tracker state, so receiving it proves the
// expiry has been applied. A missing or wrong expiry therefore shows up as a
// blocking receive and a test timeout, not as a flaky pass.

var epoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// testTTL mirrors the liveness interval discussed in req:device-presence's
// orbit; its exact value is arbitrary, only its relationship to Advance
// calls matters.
const testTTL = 45 * time.Second

// newTestTracker returns a Tracker over a fresh temporary store, failing the
// test on any setup error.
func newTestTracker(t *testing.T, clk clock.Clock) (*Tracker, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir()+"/coord.db", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	tr, err := NewTracker(st, clk, testTTL, nil)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr, st
}

// entryOf returns device's roster entry from snap, failing if absent.
func entryOf(t *testing.T, snap []Entry, device string) Entry {
	t.Helper()
	for _, e := range snap {
		if e.Device == device {
			return e
		}
	}
	t.Fatalf("device %q absent from snapshot of %d entries", device, len(snap))
	return Entry{}
}

// assertNoChange fails the test if any change is already pending on sub —
// the deterministic way to assert "nothing happened", because every notify
// happens synchronously inside the API call that caused it. A CLOSED
// subscription counts as silent: its channel yields only the zero value.
func assertNoChange(t *testing.T, sub *Subscription) {
	t.Helper()
	select {
	case ch, ok := <-sub.C():
		if !ok {
			return // closed subscription: silent by contract
		}
		t.Fatalf("unexpected change delivered: %+v", ch)
	default:
	}
}

// TestHeartbeatGoesOnlineAndExpiresWithinTTLWithLastSeen is THE DEFINING TEST
// for sto:device-presence (criterion 3): after the last heartbeat the device
// announces NOTHING — no goodbye call, no ConnectionLost, nothing — and must
// still be offline within the TTL, with last-seen pointing at the moment it
// was last real. A killed process cannot announce; this silence IS the kill,
// modeled at the layer this slice owns. The end-to-end variant (severing a
// real connection through internal/testnet) lands with slice 2's wiring into
// the accept loop.
func TestHeartbeatGoesOnlineAndExpiresWithinTTLWithLastSeen(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)

	// Subscribe BEFORE the heartbeat so the async expiry below has a
	// synchronization point.
	sub := tr.Subscribe()
	defer sub.Close()

	beatAt := f.Now()
	tr.ObserveHeartbeat("phone")

	ch := <-sub.C()
	if ch.Device != "phone" || !ch.Online {
		t.Fatalf("first change = %+v, want phone going online", ch)
	}

	e := entryOf(t, tr.Snapshot(), "phone")
	if !e.Online || !e.LastSeen.Equal(beatAt) {
		t.Fatalf("after heartbeat: online=%v lastSeen=%v, want online at %v", e.Online, e.LastSeen, beatAt)
	}
	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d after heartbeat, want exactly the one liveness lease", n)
	}

	// One second before the deadline: still online, nothing announced.
	f.Advance(testTTL - time.Second)
	assertNoChange(t, sub)
	if e := entryOf(t, tr.Snapshot(), "phone"); !e.Online {
		t.Fatal("device expired one second before its deadline")
	}

	// Cross the deadline. The device says nothing; the clock alone ends it.
	f.Advance(time.Second)

	ch = <-sub.C()
	if ch.Online {
		t.Fatalf("expiry change = %+v, want offline", ch)
	}
	if !ch.LastSeen.Equal(beatAt) {
		t.Fatalf("expiry last-seen = %v, want the last heartbeat %v — not the expiry moment", ch.LastSeen, beatAt)
	}

	e = entryOf(t, tr.Snapshot(), "phone")
	if e.Online {
		t.Fatal("snapshot still shows online after TTL expiry")
	}
	if !e.LastSeen.Equal(beatAt) {
		t.Fatalf("snapshot last-seen = %v, want %v", e.LastSeen, beatAt)
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d after expiry, want 0 (a leaked lease would fire a ghost offline)", n)
	}
}

// TestHeartbeatRefreshExtendsDeadline pins criterion 3's other half: activity
// keeps a device alive. Each heartbeat moves the deadline; expiry lands
// exactly one TTL after the LAST observation, not the first.
func TestHeartbeatRefreshExtendsDeadline(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)
	sub := tr.Subscribe()
	defer sub.Close()

	tr.ObserveHeartbeat("phone")
	<-sub.C() // online

	for beat := 2; beat <= 4; beat++ {
		f.Advance(testTTL - time.Second)
		assertNoChange(t, sub) // survived to within 1s of the old deadline

		beatAt := f.Now()
		tr.ObserveHeartbeat("phone") // refresh: deadline moves forward

		if n := f.Waiters(); n != 1 {
			t.Fatalf("Waiters() = %d after beat %d, want exactly 1 (rotation must not leak leases)", n, beat)
		}
		if e := entryOf(t, tr.Snapshot(), "phone"); !e.LastSeen.Equal(beatAt) {
			t.Fatalf("last-seen after beat %d = %v, want %v", beat, e.LastSeen, beatAt)
		}
	}

	// Activity stops. Expiry must land one full TTL after the final beat.
	lastBeat := f.Now()
	f.Advance(testTTL - time.Second)
	assertNoChange(t, sub)
	f.Advance(time.Second)

	ch := <-sub.C()
	if ch.Online || !ch.LastSeen.Equal(lastBeat) {
		t.Fatalf("expiry change = %+v, want offline with last-seen %v", ch, lastBeat)
	}
}

// TestConnectMarksOnlineAndCleanDisconnectOfflineImmediately covers
// criteria 1 and 2: connecting appears online at once; a clean disconnect
// goes offline immediately — before any clock movement — with last-seen set
// to the disconnect moment, because a close proves presence up to that
// moment.
func TestConnectMarksOnlineAndCleanDisconnectOfflineImmediately(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)
	sub := tr.Subscribe()
	defer sub.Close()

	tr.ConnectionEstablished("laptop")

	ch := <-sub.C()
	if ch.Device != "laptop" || !ch.Online {
		t.Fatalf("first change = %+v, want laptop online on connect", ch)
	}
	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d after connect, want the liveness lease armed", n)
	}

	// Some connected lifetime passes — well inside the TTL, so nothing
	// fires and no change may appear.
	f.Advance(10 * time.Second)
	assertNoChange(t, sub)

	// The clean disconnect must be immediate — a transition on the calling
	// goroutine, not something scheduled on the clock.
	lostAt := f.Now()
	tr.ConnectionLost("laptop")

	select {
	case ch := <-sub.C():
		if ch.Online {
			t.Fatalf("disconnect change = %+v, want offline", ch)
		}
		if !ch.LastSeen.Equal(lostAt) {
			t.Fatalf("disconnect last-seen = %v, want the disconnect moment %v", ch.LastSeen, lostAt)
		}
	default:
		t.Fatal("clean disconnect produced no immediate change")
	}

	e := entryOf(t, tr.Snapshot(), "laptop")
	if e.Online {
		t.Fatal("snapshot shows online after clean disconnect")
	}
	if !e.LastSeen.Equal(lostAt) {
		t.Fatalf("snapshot last-seen = %v, want %v", e.LastSeen, lostAt)
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d after disconnect, want 0", n)
	}
}

// TestSecondConnectionHoldsDeviceOnlineUntilLastLoss pins the refcount:
// handlers overlap during reconnect (the new connection's establish can run
// before the old one's loss callback), and that ordering must never flicker
// a continuously-alive device offline.
func TestSecondConnectionHoldsDeviceOnlineUntilLastLoss(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)
	sub := tr.Subscribe()
	defer sub.Close()

	tr.ConnectionEstablished("laptop") // conn A
	<-sub.C()                          // online
	tr.ConnectionEstablished("laptop") // conn B joins while A still winds down

	tr.ConnectionLost("laptop") // A exits
	assertNoChange(t, sub)      // B holds the device up — no flicker

	if e := entryOf(t, tr.Snapshot(), "laptop"); !e.Online {
		t.Fatal("device went offline while a second connection was still live")
	}

	tr.ConnectionLost("laptop") // B exits: really offline now
	ch := <-sub.C()
	if ch.Online {
		t.Fatalf("final loss change = %+v, want offline", ch)
	}
}

// TestRestartLoadsEverythingOfflineWithoutResurrection is the restart-
// semantics contract: whatever the previous process believed, those beliefs
// died with it. After reopening — both a fresh Tracker over the live store
// and a full close-and-reopen of the database file itself — every device is
// OFFLINE until observed alive again, while last-seen facts and status
// labels survive intact. The schema makes resurrection unrepresentable (no
// "online" column exists); this test holds the behaviour honest anyway.
func TestRestartLoadsEverythingOfflineWithoutResurrection(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/coord.db"
	f := clock.NewFake(epoch)

	st1, err := store.Open(path, f)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tr1, err := NewTracker(st1, f, testTTL, nil)
	if err != nil {
		t.Fatalf("tracker 1: %v", err)
	}

	// d1: was ONLINE at shutdown, with a status and an observed last-seen.
	tr1.ConnectionEstablished("phone")
	f.Advance(5 * time.Second)
	tr1.ObserveHeartbeat("phone")
	if err := tr1.SetStatus("phone", "out climbing"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	// d2: known ONLY by its label — status set without any observation yet.
	if err := tr1.SetStatus("tablet", "charging"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	before := map[string]Entry{}
	for _, e := range tr1.Snapshot() {
		before[e.Device] = e
	}
	if len(before) != 2 {
		t.Fatalf("expected two known devices before restart, got %d", len(before))
	}
	if !before["phone"].Online {
		t.Fatal("setup failed: phone should be online before restart")
	}

	tr1.Close()
	st1.Close()

	// Full restart: new Store handle on the same file, new Tracker.
	f2 := clock.NewFake(epoch.Add(time.Hour)) // later process, later clock
	st2, err := store.Open(path, f2)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()
	tr2, err := NewTracker(st2, f2, testTTL, nil)
	if err != nil {
		t.Fatalf("tracker 2: %v", err)
	}
	defer tr2.Close()

	snap := tr2.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("restart lost devices: %d entries, want 2", len(snap))
	}
	for _, e := range snap {
		if e.Online {
			t.Errorf("%s resurrected ONLINE after restart — stale rows must never come back online", e.Device)
		}
	}

	gotPhone := entryOf(t, snap, "phone")
	if !gotPhone.LastSeen.Equal(before["phone"].LastSeen) {
		t.Errorf("phone last-seen after restart = %v, want preserved %v", gotPhone.LastSeen, before["phone"].LastSeen)
	}
	if gotPhone.Status != "out climbing" {
		t.Errorf("phone status after restart = %q, want preserved %q", gotPhone.Status, "out climbing")
	}

	gotTablet := entryOf(t, snap, "tablet")
	if !gotTablet.LastSeen.IsZero() {
		t.Errorf("tablet last-seen = %v, want zero (never observed alive)", gotTablet.LastSeen)
	}
	if gotTablet.Status != "charging" {
		t.Errorf("tablet status after restart = %q, want preserved %q", gotTablet.Status, "charging")
	}

	// And the restored device comes back only by being observed again.
	sub := tr2.Subscribe()
	defer sub.Close()
	tr2.ObserveHeartbeat("phone")
	ch := <-sub.C()
	if !ch.Online {
		t.Fatalf("post-restart heartbeat change = %+v, want online", ch)
	}
}

// TestCloseStopsWatchers pins teardown: Close retires every outstanding
// lease, so advancing the clock afterwards delivers no ghost expiry.
func TestCloseStopsWatchers(t *testing.T) {
	f := clock.NewFake(epoch)
	tr, _ := newTestTracker(t, f)
	sub := tr.Subscribe()
	defer sub.Close()

	tr.ObserveHeartbeat("phone")
	<-sub.C() // online

	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d after Close, want 0", n)
	}

	f.Advance(10 * testTTL)
	assertNoChange(t, sub) // no watcher left to fire one
}

// TestNewTrackerRejectsBadWiring: a non-positive TTL would expire devices
// the instant they were observed — a wiring bug, refused rather than guessed.
func TestNewTrackerRejectsBadWiring(t *testing.T) {
	f := clock.NewFake(epoch)
	st, err := store.Open(t.TempDir()+"/coord.db", f)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if _, err := NewTracker(st, f, 0, nil); !errors.Is(err, errTTLNotPositive) {
		t.Errorf("zero ttl: err = %v, want errTTLNotPositive", err)
	}
	if _, err := NewTracker(st, f, -time.Second, nil); !errors.Is(err, errTTLNotPositive) {
		t.Errorf("negative ttl: err = %v, want errTTLNotPositive", err)
	}
	if _, err := NewTracker(nil, f, testTTL, nil); err == nil {
		t.Error("nil store: want error")
	}
	if _, err := NewTracker(st, nil, testTTL, nil); err == nil {
		t.Error("nil clock: want error")
	}
}
