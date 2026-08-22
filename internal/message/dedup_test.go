package message

import "testing"

// These tests pin the Dedup contract that criterion 3 leans on: an ID is
// first-seen exactly once, the window is bounded, and eviction is the
// documented tradeoff rather than a surprise.

func TestFirstReportsTrueOncePerID(t *testing.T) {
	d := NewDedup(8)

	if !d.First("01ARZ3NDEKTSV4RRFFQ69G5FAV") {
		t.Fatal("first sighting of a fresh ID reported duplicate")
	}
	if d.First("01ARZ3NDEKTSV4RRFFQ69G5FAV") {
		t.Fatal("second sighting of the same ID reported new — criterion 3 would display twice")
	}
	if got := d.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}

func TestDistinctIDsAreAllFirstEvenWithIdenticalContent(t *testing.T) {
	// Dedup keys on identity, never on content: two genuinely distinct
	// messages may legally carry the same text ("ok", "+1"), and suppressing
	// either would lose a real message.
	d := NewDedup(8)
	for _, id := range []string{"id-1", "id-2", "id-3"} {
		if !d.First(id) {
			t.Fatalf("distinct ID %q reported duplicate", id)
		}
	}
}

func TestWindowEvictsOldestAndReleasesItsID(t *testing.T) {
	d := NewDedup(3)
	for _, id := range []string{"a", "b", "c"} {
		d.First(id)
	}

	if !d.First("d") {
		t.Fatal("fresh ID beyond capacity reported duplicate")
	}
	if got := d.Len(); got != 3 {
		t.Fatalf("Len = %d after overflow insert, want 3 (bound must hold)", got)
	}
	// The evicted ID is the OLDEST. Its slot — and its membership — are gone,
	// so a redelivery of it displays again: the documented cost of bounded
	// retention, acceptable because redelivery latency is far shorter than
	// any window this size affords.
	if !d.First("a") {
		t.Fatal("evicted oldest ID still suppressed; window did not release it")
	}
	// Survivors stay suppressed.
	if d.First("c") {
		t.Fatal("retained ID reported new after unrelated eviction")
	}
}

func TestNewDedupRefusesNonPositiveCapacity(t *testing.T) {
	// A zero-capacity dedup would silently disable criterion 3; construction
	// must be loud instead (same posture as presence.NewTracker's TTL check).
	for _, cap := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewDedup(%d): expected panic, got none", cap)
				}
			}()
			NewDedup(cap)
		}()
	}
}
