package coordinator

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/coordinator/queue"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
)

// The tests in this file drive sto:offline-queue END-TO-END through the real
// server on the presence rig: a real queue over the rig's own store wired as
// the OfflineSink, real WebSocket handshakes carrying last_acked_position,
// real QueueAck frames. Every delivery claim is counted OFF THE WIRE — the
// reader goroutine sees every frame the server emits, so asserting exact
// counts here is criterion 4's measurement discipline (slice 2 hardens it
// into the instrumented-transport form). No test sleeps.

// wireQueue builds a queue over the rig's store and wires it as the server's
// offline sink. Wiring happens before any client connects, so no connection
// handler exists to race the field write (see the presenceRig comment).
func wireQueue(t *testing.T, rig *presenceRig, ttl time.Duration, maxSize int) *queue.Queue {
	t.Helper()
	q, err := queue.New(rig.st, rig.clk, ttl, maxSize, slog.New(slog.NewTextHandler(rig.logs, nil)))
	if err != nil {
		t.Fatalf("wire queue: %v", err)
	}
	rig.srv.offline = q
	t.Cleanup(q.Close)
	return q
}

// nextQueuedDelivery returns the next DirectMessage envelope, failing if the
// next frame is anything else — on a freshly resumed connection the drain is
// deterministic: HelloAck first, then exactly the promised suffix.
func nextQueuedDelivery(t *testing.T, c *testClient) *walkiev1.Envelope {
	t.Helper()
	env := c.nextEnvelope(t)
	if env.GetDirectMessage() == nil {
		t.Fatalf("%s: expected queued DirectMessage, got %T", c.name, env.GetPayload())
	}
	return env
}

// TestOfflineDeliveryResumesByPositionCountedOnWire is criteria 1 and 2 end
// to end, measured the way req:offline-delivery demands: by counting frames.
//
// Phone disconnects; three messages are held. Phone reconnects reporting
// NOTHING acked and receives EXACTLY three deliveries (positions 1..3) after
// a HelloAck promising three. It acks position 2, disconnects, reconnects
// again — and receives EXACTLY ONE message (position 3), not a replay of the
// queue. A full-replay resumption would send four DMs across these two
// reconnects; the tight bound this test asserts is three-plus-one.
//
// Criterion 4's identity half is asserted too, not just the count: after each
// reconnect the connection's complete wire record must hold EXACTLY the
// expected ULIDs in order — a server that replayed already-acknowledged
// messages would fail on both count and identity, even though dedup would
// still make the user's screen look correct.
func TestOfflineDeliveryResumesByPositionCountedOnWire(t *testing.T) {
	rig := startPresenceRig(t)
	wireQueue(t, rig, time.Hour, 64)

	laptop := rig.connectDevice(t, laptopName)
	laptop.assertSilent(t)

	phone := rig.connectDevice(t, phoneName)
	if got := laptop.nextPresence(t); got.GetDevice() != phoneName {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}
	phone.teardown(t)
	if got := laptop.nextPresence(t); got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("setup: laptop saw %+v, want phone OFFLINE", got)
	}

	// Three messages for the offline phone, identities kept for the wire
	// assertions below — criterion 4 counts AND names what crosses. Dispatch
	// is sequential in the sender's read loop, so the ack of the follow-up
	// Hello proves all three holds (and their log lines) have landed.
	var heldIDs []string
	for _, body := range []string{"held one", "held two", "held three"} {
		id := message.NewID(rig.clk.Now())
		heldIDs = append(heldIDs, id)
		laptop.send(t, directEnvelope(id, phoneName, body, rigEpoch))
	}
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)
	if out := rig.logs.String(); strings.Count(out, "handed to offline sink") != 3 {
		t.Fatalf("expected 3 hold log lines, log:\n%s", out)
	}
	laptop.assertSilent(t) // holding answers nothing on the sender's wire

	// Reconnect #1: report zero acked. The handshake's HelloAck promises 3
	// (connectDevice captures it as helloAck), then EXACTLY 3 deliveries
	// cross, positions stamped 1..3, oldest first.
	phone2 := rig.connectDevice(t, phoneName)
	if got := phone2.helloAck.GetPendingCount(); got != 3 {
		t.Fatalf("first reconnect: pending_count = %d, want 3", got)
	}
	for i, wantPos := range []uint64{1, 2, 3} {
		env := nextQueuedDelivery(t, phone2)
		if got := env.GetPosition(); got != wantPos {
			t.Fatalf("delivery %d: position = %d, want %d", i+1, got, wantPos)
		}
	}
	phone2.assertSilent(t) // the suffix ends where the count said it would

	// Causal close of the measurement window: dispatch is sequential per
	// connection, so this probe's ack proves every frame the server was ever
	// going to write for the drain has landed in the wire record. Only now
	// may the exact-count assertion run — earlier, a slow replay would not
	// have arrived yet and a full-replay bug could slip the count.
	phone2.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	phone2.awaitHelloAck(t)
	assertWireDeliveries(t, phone2, wireDelivery{id: heldIDs[0], position: 1},
		wireDelivery{id: heldIDs[1], position: 2},
		wireDelivery{id: heldIDs[2], position: 3})

	// Ack position 2: everything ≤ 2 leaves retention.
	phone2.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_QueueAck{
		QueueAck: &walkiev1.QueueAck{AcknowledgedPosition: 2},
	}})
	// Causal sync: the ack-of-nothing needs a probe to prove processing.
	phone2.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	phone2.awaitHelloAck(t)

	// Reconnect #2: same device reports last_acked_position=2. The wire must
	// carry EXACTLY ONE message — position 3 — after a pending_count of 1.
	// Criterion 2: each pending message arrives once per reconnect; criterion
	// 4's bound forbids replaying what was already acknowledged, and the
	// identity assertion below is what makes a full-replay implementation
	// FAIL here rather than merely look fine through dedup.
	phone3 := rig.connectDevice(t, phoneName, 2)
	if got := phone3.helloAck.GetPendingCount(); got != 1 {
		t.Fatalf("second reconnect: pending_count = %d, want 1", got)
	}
	env := nextQueuedDelivery(t, phone3)
	if env.GetPosition() != 3 || env.GetDirectMessage().GetBody() != "held three" {
		t.Fatalf("second reconnect delivered position %d body %q, want 3 %q",
			env.GetPosition(), env.GetDirectMessage().GetBody(), "held three")
	}
	phone3.assertSilent(t)

	// Same causal close, then the tight bound: one frame, ONE ulid — the
	// third message's own, never its already-acked siblings'.
	phone3.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	phone3.awaitHelloAck(t)
	assertWireDeliveries(t, phone3, wireDelivery{id: heldIDs[2], position: 3})

	// The held envelopes kept their ingress identity: dedup keys survive.
	// (Positions are the ONLY field replay adds.)
	if got := env.GetReceivedAt().AsTime(); !got.Equal(rigEpoch) {
		t.Errorf("replayed received_at = %v, want original ingress stamp %v", got, rigEpoch)
	}
}

// wireDelivery is one expected DirectMessage crossing the wire: the ULID that
// identifies it (the receiver-side dedup key) and the position the replay
// stamps it with.
type wireDelivery struct {
	id       string
	position uint64
}

// assertWireDeliveries fails unless c's complete wire record holds EXACTLY
// the named direct messages, in order, each under its expected ULID and
// position. This is criterion 4's measurement made into a helper: count AND
// identity, off the wire record — not off what the user's display folded.
func assertWireDeliveries(t *testing.T, c *testClient, want ...wireDelivery) {
	t.Helper()
	got := c.wireDirectMessages()
	if len(got) != len(want) {
		t.Fatalf("%s: %d direct messages crossed the wire, want exactly %d",
			c.name, len(got), len(want))
	}
	for i, w := range want {
		env := got[i]
		if env.GetMessageId() != w.id {
			t.Fatalf("%s: wire delivery %d has message_id %q, want %q",
				c.name, i+1, env.GetMessageId(), w.id)
		}
		if env.GetPosition() != w.position {
			t.Fatalf("%s: wire delivery %d (%s) has position %d, want %d",
				c.name, i+1, w.id, env.GetPosition(), w.position)
		}
	}
}

// TestCapRefusalReachesSenderAndLog is criterion 6 end to end: with a
// one-message inbox, the second held message produces QueueRefused{SIZE_CAP}
// ON THE SENDER'S WIRE, a refusal line in the log, and an inbox that still
// holds exactly the first message — no growth past the cap, no silence.
func TestCapRefusalReachesSenderAndLog(t *testing.T) {
	rig := startPresenceRig(t)
	wireQueue(t, rig, time.Hour, 1)

	laptop := rig.connectDevice(t, laptopName)
	phone := rig.connectDevice(t, phoneName)
	if got := laptop.nextPresence(t); got.GetDevice() != phoneName {
		t.Fatalf("setup: laptop saw %+v, want phone", got)
	}
	phone.teardown(t)
	if got := laptop.nextPresence(t); got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("setup: laptop saw %+v, want phone OFFLINE", got)
	}

	// First message: accepted into the one-slot inbox.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "fits", rigEpoch))
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)

	// Second message: REFUSED, explicitly, to the sender.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "overflows", rigEpoch))
	var refused *walkiev1.QueueRefused
	for {
		env := laptop.nextEnvelope(t)
		if qr := env.GetQueueRefused(); qr != nil {
			refused = qr
			break
		}
	}
	if refused.GetReason() != walkiev1.QueueRefusalReason_QUEUE_REFUSAL_REASON_SIZE_CAP {
		t.Fatalf("refusal reason = %v, want SIZE_CAP", refused.GetReason())
	}
	if !strings.Contains(refused.GetDetail(), "size cap") {
		t.Errorf("refusal detail %q should name the cap for the terminal user", refused.GetDetail())
	}

	// The log half of criterion 6, synced by the causal Hello probe.
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)
	out := rig.logs.String()
	if !strings.Contains(out, "inbox at size cap") {
		t.Errorf("cap refusal not logged; log:\n%s", out)
	}
	if strings.Contains(out, "overflows") || strings.Contains(out, "fits") {
		t.Errorf("log carries message BODY — no-content rule violated; log:\n%s", out)
	}

	// No growth past the cap: the inbox still holds exactly the first
	// message. Counted IN THE STORE, not merely inferred from a resume's
	// promise — criterion 6 says the coordinator does not grow past the cap,
	// and the row count is that claim's direct evidence.
	var inboxRows int
	if err := rig.st.DB().QueryRow(
		`SELECT COUNT(*) FROM queue_inbox WHERE recipient = ?`, phoneName,
	).Scan(&inboxRows); err != nil {
		t.Fatalf("count inbox rows: %v", err)
	}
	if inboxRows != 1 {
		t.Fatalf("queue_inbox holds %d rows for %s, want exactly 1 (nothing stored past the cap)",
			inboxRows, phoneName)
	}
	phone2 := rig.connectDevice(t, phoneName)
	if got := phone2.helloAck.GetPendingCount(); got != 1 {
		t.Fatalf("post-refusal resume: pending_count = %d, want 1 (inbox unchanged)", got)
	}
	if env := nextQueuedDelivery(t, phone2); env.GetDirectMessage().GetBody() != "fits" {
		t.Fatalf("post-refusal inbox delivered %q, want the retained message", env.GetDirectMessage().GetBody())
	}

	// The connection survived the refusal: laptop can still talk to the now-
	// online phone directly.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "live again", rigEpoch))
	if got := phone2.nextEnvelope(t); got.GetDirectMessage().GetBody() != "live again" {
		t.Fatalf("post-refusal live delivery = %T, want the direct message (connection must survive refusal)",
			got.GetPayload())
	}
}

// TestAckForUnknownDeviceIgnoredQuietly: a QueueAck from a device the queue
// never saw is absorbed (cursor created, nothing else) rather than erroring
// the connection — at-least-once clients ack what they hold, even when the
// coordinator restarted empty.
func TestAckForUnknownDeviceIgnoredQuietly(t *testing.T) {
	rig := startPresenceRig(t)
	wireQueue(t, rig, time.Hour, 8)

	tablet := rig.connectDevice(t, tabletName)
	tablet.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_QueueAck{
		QueueAck: &walkiev1.QueueAck{AcknowledgedPosition: 7},
	}})

	// Causal proof the frame was processed without harm: the connection still
	// completes a handshake round trip afterwards.
	tablet.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	tablet.awaitHelloAck(t)
	tablet.assertSilent(t)
}
