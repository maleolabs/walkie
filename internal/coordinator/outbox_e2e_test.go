package coordinator

import (
	"bytes"
	"log/slog"
	"testing"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/outbox"
	"github.com/maleolabs/walkie/internal/store"
)

// The tests in this file drive sto:offline-queue's CLIENT half END-TO-END
// through the real server: a real [outbox.Outbox] over its own store (one
// store per process, as in production), envelopes composed while the sender
// has NO connection at all, then transmitted on reconnect and routed by the
// real dispatch switch. req:offline-delivery criterion 3 is the point: a
// message composed offline is not lost — it reaches its recipient after the
// sender reconnects, attributed and identified exactly as a live-composed
// message would be. No test sleeps; causality closes every measurement.

// TestComposedOfflineDeliveredAfterSenderReconnects is criterion 3 end to
// end: laptop composes two messages while DISCONNECTED (they exist only as
// durable outbox rows), the client process is fully restarted while still
// disconnected, laptop then reconnects, drains the outbox over the live
// connection, and phone receives both — right ULIDs, right bodies, sender
// attributed by the coordinator from the resolved identity, position zero
// (live traffic, not queue replay).
func TestComposedOfflineDeliveredAfterSenderReconnects(t *testing.T) {
	rig := startPresenceRig(t)

	// The recipient is online throughout; this criterion is about the
	// SENDER's half of offline composition, so the delivery leg stays the
	// ordinary live-routing path (the queue holds the recipient-offline case,
	// covered by the queue e2e suite).
	phone := rig.connectDevice(t, phoneName)

	// Compose while disconnected. The envelopes are built the way a
	// composing client builds them — identity fields only, no sender (the
	// coordinator owns attribution) — and persisted BEFORE any wire byte.
	path := t.TempDir() + "/laptop-client.db"
	st1, err := store.Open(path, rig.clk)
	if err != nil {
		t.Fatalf("open client store: %v", err)
	}
	ob1, err := outbox.New(st1, rig.clk, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}

	var composedIDs []string
	bodies := []string{"typed offline one", "typed offline two"}
	for _, body := range bodies {
		id := message.NewID(rig.clk.Now())
		composedIDs = append(composedIDs, id)
		if err := ob1.Enqueue(directEnvelope(id, phoneName, body, rigEpoch)); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	if n, err := ob1.Count(); err != nil || n != 2 {
		t.Fatalf("outbox count = %d err %v, want 2 nil", n, err)
	}

	// The client process DIES and comes back, still disconnected: store
	// closed entirely, reopened fresh. Durability is the outbox's contract —
	// the reopened instance must serve both envelopes in composition order.
	if err := st1.Close(); err != nil {
		t.Fatalf("close client store: %v", err)
	}
	st2, err := store.Open(path, rig.clk)
	if err != nil {
		t.Fatalf("reopen client store: %v", err)
	}
	defer st2.Close()
	ob2, err := outbox.New(st2, rig.clk, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatalf("reopened outbox: %v", err)
	}
	pending, err := ob2.Pending()
	if err != nil {
		t.Fatalf("pending after restart: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending after restart = %d, want 2 (nothing lost while disconnected)", len(pending))
	}

	// Reconnect and drain: transmit each held envelope, removing it ONLY
	// after the transport accepted the frame (the outbox's at-least-once
	// rule). The test client's send fails loudly on any write error, so a
	// returned send IS an accepted frame.
	laptop := rig.connectDevice(t, laptopName)
	for _, env := range pending {
		laptop.send(t, env)
		if err := ob2.Remove(env.GetMessageId()); err != nil {
			t.Fatalf("remove %s after transmit: %v", env.GetMessageId(), err)
		}
	}
	if n, err := ob2.Count(); err != nil || n != 0 {
		t.Fatalf("outbox count after drain = %d err %v, want 0 nil", n, err)
	}

	// Causal sync: the probe's ack proves the server dispatched (and routed)
	// everything sent above before phone reads a single frame.
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)

	// Both compositions arrive, in order, indistinguishable from live-sent
	// traffic apart from their compose-time origins: same ULIDs (dedup keys
	// minted at composition survive the hold), same bodies, coordinator
	// attribution, ingress-stamped, position zero.
	for i, wantID := range composedIDs {
		got := nextTextEnvelope(t, phone)
		assertDirectDelivery(t, got, wantID, bodies[i], rigEpoch, rigEpoch)
	}
}
