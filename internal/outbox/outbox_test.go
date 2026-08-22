package outbox

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/store"
)

// The tests in this file pin the outbox contract on a real store and a fake
// clock: composed-offline messages are HELD durably, drained in composition
// order, kept on failed transmit, and survive a full store reopen. No test
// sleeps; every claim is made by direct observation of the seam.

var epoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

func testClock() *clock.Fake { return clock.NewFake(epoch) }

func mustOpenStore(t *testing.T, path string) *store.Store {
	t.Helper()
	st, err := store.Open(path, testClock())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return st
}

func mustNewOutbox(t *testing.T, st *store.Store) *Outbox {
	t.Helper()
	ob, err := New(st, testClock(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	return ob
}

func env(id string) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId: id,
		SentAt:    timestamppb.New(epoch),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: "phone.tail-scale.ts.net.",
			Body:      "body of " + id,
		}},
	}
}

func pendingIDs(t *testing.T, ob *Outbox) []string {
	t.Helper()
	got, err := ob.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	ids := make([]string, len(got))
	for i, e := range got {
		ids[i] = e.GetMessageId()
	}
	return ids
}

func assertIDs(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("pending = %v, want %v (count mismatch)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pending = %v, want %v (order is FIFO)", got, want)
		}
	}
}

// TestComposedOfflineHeldThenDrainedInOrder is criterion 3's client half:
// envelopes composed while disconnected are held, and Pending serves them in
// composition order for transmission on reconnect.
func TestComposedOfflineHeldThenDrainedInOrder(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/client.db")
	defer st.Close()
	ob := mustNewOutbox(t, st)

	assertIDs(t, pendingIDs(t, ob)) // fresh outbox: nothing held

	for _, id := range []string{"m-1", "m-2", "m-3"} {
		if err := ob.Enqueue(env(id)); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	assertIDs(t, pendingIDs(t, ob), "m-1", "m-2", "m-3")

	n, err := ob.Count()
	if err != nil || n != 3 {
		t.Fatalf("count = %d err %v, want 3 nil", n, err)
	}

	// Successful transmit: remove each as the transport accepts it.
	for _, id := range []string{"m-1", "m-2", "m-3"} {
		if err := ob.Remove(id); err != nil {
			t.Fatalf("remove %s: %v", id, err)
		}
	}
	assertIDs(t, pendingIDs(t, ob))
}

// TestFailedSendStaysQueued: a transmit that did not succeed leaves the
// message — and everything behind it — queued. Retrying later drains in the
// SAME order; nothing skipped ahead while the wire was down.
func TestFailedSendStaysQueued(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/client.db")
	defer st.Close()
	ob := mustNewOutbox(t, st)

	for _, id := range []string{"m-1", "m-2", "m-3"} {
		if err := ob.Enqueue(env(id)); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// The reconnect drain: m-1 transmits, m-2 FAILS. m-2 and m-3 stay.
	if err := ob.Remove("m-1"); err != nil {
		t.Fatalf("remove transmitted: %v", err)
	}
	// (no Remove for the failed m-2 — that IS the failure path)

	assertIDs(t, pendingIDs(t, ob), "m-2", "m-3")

	// Next successful attempt finishes in order.
	if err := ob.Remove("m-2"); err != nil {
		t.Fatalf("remove retry: %v", err)
	}
	if err := ob.Remove("m-3"); err != nil {
		t.Fatalf("remove retry: %v", err)
	}
	assertIDs(t, pendingIDs(t, ob))
}

// TestSurvivesRestartWhileDisconnected is the durability decision made
// concrete: enqueue, close the store entirely, reopen, and every held
// envelope is still pending in order — a killed client loses nothing it
// composed offline.
func TestSurvivesRestartWhileDisconnected(t *testing.T) {
	path := t.TempDir() + "/client.db"

	st1 := mustOpenStore(t, path)
	ob1 := mustNewOutbox(t, st1)
	for _, id := range []string{"m-1", "m-2"} {
		if err := ob1.Enqueue(env(id)); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	if err := st1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2 := mustOpenStore(t, path)
	defer st2.Close()
	ob2 := mustNewOutbox(t, st2)

	assertIDs(t, pendingIDs(t, ob2), "m-1", "m-2")

	// Round-trip fidelity: what comes back decodes to what went in.
	pending, err := ob2.Pending()
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending: %d envelopes err %v", len(pending), err)
	}
	if got := pending[0].GetDirectMessage().GetBody(); got != "body of m-1" {
		t.Errorf("restarted body = %q, want original intact", got)
	}
	if !pending[0].GetSentAt().AsTime().Equal(epoch) {
		t.Errorf("restarted sent_at = %v, want compose instant %v", pending[0].GetSentAt().AsTime(), epoch)
	}
}

// TestDuplicateMessageIDRefused: two rows under one ULID would corrupt the
// removal path (which removes by identity), so a duplicate enqueue is an
// error, not a silent second row.
func TestDuplicateMessageIDRefused(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/client.db")
	defer st.Close()
	ob := mustNewOutbox(t, st)

	if err := ob.Enqueue(env("m-dup")); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := ob.Enqueue(env("m-dup")); err == nil {
		t.Fatal("duplicate message_id accepted; identity collision must be refused")
	}
	assertIDs(t, pendingIDs(t, ob), "m-dup")
}

// TestRemoveUnknownIDIsNoOp: idempotent removal keeps retry-after-crash
// honest — re-removing something already gone reports success, not error.
func TestRemoveUnknownIDIsNoOp(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/client.db")
	defer st.Close()
	ob := mustNewOutbox(t, st)

	if err := ob.Remove("never-existed"); err != nil {
		t.Fatalf("remove unknown id errored: %v", err)
	}
}

// TestConstructorRefusesWiringErrors: missing store or clock is a wiring bug,
// refused at construction per the house rule.
func TestConstructorRefusesWiringErrors(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/client.db")

	if _, err := New(nil, testClock(), nil); err == nil {
		t.Error("nil store accepted")
	}
	if _, err := New(st, nil, nil); err == nil {
		t.Error("nil clock accepted")
	}
}
