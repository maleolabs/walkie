package coordinator

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
)

// The tests in this file drive sto:text-messaging's routing END-TO-END
// through the real server on the presence rig: real WebSocket handshakes,
// real envelope bytes, the real dispatch switch. Every claim is made
// deterministic by causality — a blocking receive whose failure mode is a
// loud timeout, never a sleep — and every timestamp comes from the rig's
// fake clock.

const (
	laptopName = "laptop.tail-scale.ts.net."
	phoneName  = "phone.tail-scale.ts.net."
	tabletName = "tablet.tail-scale.ts.net."
)

// recordingSink is the OfflineSink double: it records deliveries on channels
// so tests receive them event-driven instead of polling. Buffered so a slow
// test cannot stall the server's read loop.
type recordingSink struct {
	recipients chan string
	envs       chan *walkiev1.Envelope
}

func newRecordingSink() *recordingSink {
	return &recordingSink{
		recipients: make(chan string, 8),
		envs:       make(chan *walkiev1.Envelope, 8),
	}
}

func (r *recordingSink) Deliver(recipient string, env *walkiev1.Envelope) error {
	r.recipients <- recipient
	r.envs <- env
	return nil // retention always accepted: the double holds everything
}

// connectAll connects the named devices in order, draining each earlier
// device's ONLINE chatter as it goes, and leaves every pipe silent.
//
// The drain is deterministic, not best-effort: connecting device N queues
// exactly one ONLINE update per already-connected device (subject-exclusion
// keeps N's own event off its own stream), so consuming exactly that many
// events and then asserting silence proves the pipes are clean before any
// message traffic starts.
func connectAll(t *testing.T, rig *presenceRig, names ...string) []*testClient {
	t.Helper()
	var out []*testClient
	for _, name := range names {
		c := rig.connectDevice(t, name)
		for _, prev := range out {
			if got := prev.nextPresence(t); got.GetDevice() != name {
				t.Fatalf("setup: %s saw %+v, want %s ONLINE", prev.name, got, name)
			}
		}
		out = append(out, c)
	}
	for _, c := range out {
		c.assertSilent(t)
	}
	return out
}

func directEnvelope(id, recipient, body string, sentAt time.Time) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId: id,
		SentAt:    timestamppb.New(sentAt),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: recipient,
			Body:      body,
		}},
	}
}

func broadcastEnvelope(id, body string, sentAt time.Time) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId: id,
		SentAt:    timestamppb.New(sentAt),
		Payload: &walkiev1.Envelope_BroadcastMessage{BroadcastMessage: &walkiev1.BroadcastMessage{
			Body: body,
		}},
	}
}

// assertDirectDelivery checks the full envelope contract of a routed direct
// message: payload intact, identity fields carried over verbatim, received_at
// stamped from the coordinator clock, position zero (live traffic).
func assertDirectDelivery(t *testing.T, got *walkiev1.Envelope, wantID, wantBody string, wantSentAt, wantReceivedAt time.Time) {
	t.Helper()
	dm := got.GetDirectMessage()
	if dm == nil {
		t.Fatalf("delivered payload = %T, want DirectMessage", got.GetPayload())
	}
	if dm.GetBody() != wantBody {
		t.Errorf("body = %q, want %q", dm.GetBody(), wantBody)
	}
	if got.GetMessageId() != wantID {
		t.Errorf("message_id = %q, want %q (dedup keys on it surviving transit)", got.GetMessageId(), wantID)
	}
	if !got.GetSentAt().AsTime().Equal(wantSentAt) {
		t.Errorf("sent_at = %v, want %v (sender half must not be rewritten)", got.GetSentAt().AsTime(), wantSentAt)
	}
	if !got.GetReceivedAt().AsTime().Equal(wantReceivedAt) {
		t.Errorf("received_at = %v, want %v (coordinator stamp)", got.GetReceivedAt().AsTime(), wantReceivedAt)
	}
	if got.GetPosition() != 0 {
		t.Errorf("position = %d, want 0 (zero is live traffic; queue replays are sto:offline-queue's)", got.GetPosition())
	}
}

// TestDirectMessageReachesExactlyRecipient is criterion 1's routing half:
// A→B arrives on B with its identity fields intact — and on NO other device.
// Laptop's non-receipt is proven by causality: tablet's later message must be
// the FIRST frame on laptop's wire, so nothing (least of all an echo of
// laptop's own outgoing message) can have preceded it.
func TestDirectMessageReachesExactlyRecipient(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName, tabletName)
	laptop, phone, tablet := clients[0], clients[1], clients[2]

	id := message.NewID(rig.clk.Now())
	laptop.send(t, directEnvelope(id, phoneName, "hello phone", rigEpoch))

	got := phone.nextEnvelope(t)
	assertDirectDelivery(t, got, id, "hello phone", rigEpoch, rigEpoch)

	// Exactly the recipient: tablet holds nothing...
	tablet.assertSilent(t)
	// ...and laptop holds nothing — proven next by causality, since
	// assertSilence alone cannot see frames still in flight.
	tablet.send(t, directEnvelope(message.NewID(rig.clk.Now()), laptopName, "probe", rigEpoch))
	if probe := laptop.nextEnvelope(t); probe.GetDirectMessage().GetBody() != "probe" {
		t.Fatalf("laptop's first inbound frame = %T %q, want the probe (an echo or stray frame preceded it)",
			probe.GetPayload(), probe.GetDirectMessage().GetBody())
	}
}

// TestBroadcastReachesEveryOtherDeviceNotSender is criterion 2's routing
// half: one broadcast from A lands on every OTHER connected device, and A
// itself stays silent — subject exclusion for text, same rule the presence
// pump applies to roster events.
func TestBroadcastReachesEveryOtherDeviceNotSender(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName, tabletName)
	laptop, phone, tablet := clients[0], clients[1], clients[2]

	id := message.NewID(rig.clk.Now())
	laptop.send(t, broadcastEnvelope(id, "standup in 5", rigEpoch))

	for _, watcher := range []*testClient{phone, tablet} {
		got := watcher.nextEnvelope(t)
		bm := got.GetBroadcastMessage()
		if bm == nil {
			t.Fatalf("%s got %T, want BroadcastMessage", watcher.name, got.GetPayload())
		}
		if bm.GetBody() != "standup in 5" || got.GetMessageId() != id {
			t.Fatalf("%s got body=%q id=%q, want standup/id match", watcher.name, bm.GetBody(), got.GetMessageId())
		}
		if !got.GetReceivedAt().AsTime().Equal(rigEpoch) || got.GetPosition() != 0 {
			t.Fatalf("%s got received_at=%v position=%d, want fake-clock stamp and zero position",
				watcher.name, got.GetReceivedAt().AsTime(), got.GetPosition())
		}
	}

	// The sender is NOT in its own audience; causal backstop below proves
	// nothing preceded the probe on laptop's wire.
	laptop.assertSilent(t)
	phone.send(t, directEnvelope(message.NewID(rig.clk.Now()), laptopName, "backstop", rigEpoch))
	if probe := laptop.nextEnvelope(t); probe.GetDirectMessage().GetBody() != "backstop" {
		t.Fatalf("laptop's first inbound frame was %T, want the backstop DM (a self-echo preceded it)",
			probe.GetPayload())
	}
}

// TestReceivedAtStampedFromInjectedClockOncePerIngress pins the ingress rule:
// the stamp comes from the INJECTED clock (never the wall), changes as the
// clock moves, leaves sent_at untouched, and — when the SAME ULID arrives
// twice, as at-least-once transport redelivery guarantees it eventually will
// — each arrival carries its OWN ingress stamp. The wire does not prevent the
// duplicate; the receiver's core makes it harmless (message package tests).
func TestReceivedAtStampedFromInjectedClockOncePerIngress(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	rig.clk.Advance(5 * time.Second)
	firstIngress := rig.clk.Now()

	id := message.NewID(firstIngress)
	laptop.send(t, directEnvelope(id, phoneName, "once", rigEpoch)) // sent_at deliberately skewed

	got := phone.nextEnvelope(t)
	assertDirectDelivery(t, got, id, "once", rigEpoch, firstIngress)

	rig.clk.Advance(7 * time.Second)
	secondIngress := rig.clk.Now()

	// Redelivery of the SAME ULID — what a reconnect replay looks like.
	laptop.send(t, directEnvelope(id, phoneName, "once", rigEpoch))

	redelivered := phone.nextEnvelope(t)
	assertDirectDelivery(t, redelivered, id, "once", rigEpoch, secondIngress)
	if redelivered.GetReceivedAt().AsTime().Equal(firstIngress) {
		t.Fatal("second ingress reused the first stamp; each arrival must be stamped at ITS ingress")
	}
}

// TestOfflineRecipientGoesToSinkExtensionPoint exercises the documented seam:
// a message for a KNOWN but disconnected device is handed to the OfflineSink
// with the stamped envelope intact — not dropped silently, not answered with
// an error, and not written into any built-in queue (there is none to write
// into; the sink IS the extension point). The unknown-recipient refusal at
// the end doubles as proof that no stray frame preceded it on the sender's
// wire after the offline hold.
func TestOfflineRecipientGoesToSinkExtensionPoint(t *testing.T) {
	sink := newRecordingSink()
	rig := startPresenceRig(t, sink)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	// Phone disconnects cleanly; laptop observes OFFLINE. That observation is
	// also the ordering proof: serveConn unregisters the route BEFORE
	// ConnectionLost fires, so "offline observed" means "already unroutable".
	phone.teardown(t)
	if got := laptop.nextPresence(t); got.GetDevice() != phoneName ||
		got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("setup: laptop saw %+v, want phone OFFLINE", got)
	}
	laptop.assertSilent(t)

	rig.clk.Advance(3 * time.Second)
	heldAt := rig.clk.Now()
	id := message.NewID(heldAt)
	laptop.send(t, directEnvelope(id, phoneName, "hold this", rigEpoch))

	select {
	case recipient := <-sink.recipients:
		if recipient != phoneName {
			t.Fatalf("sink got recipient %q, want %q", recipient, phoneName)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("offline delivery never reached the sink extension point")
	}
	select {
	case env := <-sink.envs:
		assertDirectDelivery(t, env, id, "hold this", rigEpoch, heldAt)
	case <-time.After(5 * time.Second):
		t.Fatal("sink got a recipient but no envelope")
	}

	// No error frame went back to the sender: holding for an offline peer is
	// normal operation, not a refusal (and read receipts are out of scope).
	laptop.assertSilent(t)

	// Causal close: an UNKNOWN recipient is refused in band. Receiving THAT
	// error as laptop's first frame proves the offline hold emitted nothing
	// spurious before it.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), "ghost.tail-scale.ts.net.", "?", rigEpoch))
	refused := laptop.nextEnvelope(t)
	pe := refused.GetProtocolError()
	if pe == nil || pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_DEVICE_UNKNOWN {
		t.Fatalf("unknown recipient got %T code=%v, want DEVICE_UNKNOWN", refused.GetPayload(), pe.GetCode())
	}
	if !strings.Contains(pe.GetDetail(), "ghost") {
		t.Errorf("refusal detail %q should name the unknown device (typo diagnosis)", pe.GetDetail())
	}
}

// TestUnknownRecipientRefusedDeviceUnknown pins the typo case without a sink:
// a name this tailnet has never resolved gets the structured DEVICE_UNKNOWN
// error, the connection survives, and the intended recipient (never seen by
// the tracker) naturally receives nothing.
func TestUnknownRecipientRefusedDeviceUnknown(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), "typo.tail-scale.ts.net.", "hello?", rigEpoch))

	got := laptop.nextEnvelope(t)
	pe := got.GetProtocolError()
	if pe == nil {
		t.Fatalf("reply = %T, want ProtocolError", got.GetPayload())
	}
	if pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_DEVICE_UNKNOWN {
		t.Fatalf("code = %v, want DEVICE_UNKNOWN", pe.GetCode())
	}

	// Correctable mistake, not a broken peer: the connection still works.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "for you", rigEpoch))
	if got := phone.nextEnvelope(t); got.GetDirectMessage().GetBody() != "for you" {
		t.Fatalf("post-refusal delivery = %T, want the follow-up DM (connection must survive refusal)",
			got.GetPayload())
	}
}

// TestOfflineDropLoudWhenNoQueueWired documents today's DEFAULT shape: with
// no sink wired, a message for a known-but-offline device is dropped — loudly,
// typed, content-free — and the sender hears nothing. This test exists so the
// drop can never become SILENT: the log line is part of the contract until
// sto:offline-queue replaces the nil.
func TestOfflineDropLoudWhenNoQueueWired(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	phone.teardown(t)
	if got := laptop.nextPresence(t); got.GetState() != walkiev1.PresenceState_PRESENCE_STATE_OFFLINE {
		t.Fatalf("setup: laptop saw %+v, want phone OFFLINE", got)
	}

	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "into the void", rigEpoch))

	// Causal sync before reading logs: dispatch runs sequentially in the
	// sender's read loop, so receiving THIS Hello's ack proves the drop (and
	// its log line) already happened. Reading logs straight after send races
	// the server's goroutine.
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)

	laptop.assertSilent(t) // no ack, no error: silence on the wire...
	if out := rig.logs.String(); !strings.Contains(out, "no queue wired") {
		t.Errorf("drop not logged loudly; log output:\n%s", out)
	}
}

// TestOversizedBodyRejectedNotTruncated: the receiver-enforced 4096-byte
// bound REJECTS with a structured error naming the bound rather than
// truncating (a truncated message reads as delivered-but-wrong), nothing
// reaches the recipient, exactly-at-bound delivers whole, and invalid UTF-8
// is rejected too.
func TestOversizedBodyRejectedNotTruncated(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	tooBig := strings.Repeat("x", message.MaxBodyBytes+1)
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, tooBig, rigEpoch))

	var pe *walkiev1.ProtocolError
	for {
		env := laptop.nextEnvelope(t)
		if e := env.GetProtocolError(); e != nil {
			pe = e
			break
		}
	}
	if pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED {
		t.Fatalf("rejection code = %v, want MALFORMED", pe.GetCode())
	}
	if !strings.Contains(pe.GetDetail(), "4096") {
		t.Fatalf("rejection detail %q should name the bound", pe.GetDetail())
	}
	phone.assertSilent(t) // rejected means never routed

	exact := strings.Repeat("y", message.MaxBodyBytes)
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, exact, rigEpoch))
	got := phone.nextEnvelope(t)
	if len(got.GetDirectMessage().GetBody()) != message.MaxBodyBytes {
		t.Fatalf("boundary body delivered at %d bytes, want %d untruncated",
			len(got.GetDirectMessage().GetBody()), message.MaxBodyBytes)
	}
}

// TestValidateBodyRejectsInvalidUTF8 covers the encoding half of the body
// rule at unit level. It CANNOT be tested over the wire: proto3 string
// fields are UTF-8 by definition and both marshal and unmarshal refuse
// anything else, so an invalid-UTF-8 body is unrepresentable in a frame that
// decodes. The check stays in ValidateBody as defence-in-depth for any future
// non-proto ingress (the control socket), matching the status label's
// validation shape in the presence tracker.
func TestValidateBodyRejectsInvalidUTF8(t *testing.T) {
	if err := message.ValidateBody("\xff\xfe not text"); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

// TestEmptyRecipientRejectedMalformed covers the degenerate addressing case:
// a direct message naming nobody is malformed, refused in band, connection
// kept.
func TestEmptyRecipientRejectedMalformed(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), "", "to whom?", rigEpoch))

	got := laptop.nextEnvelope(t)
	pe := got.GetProtocolError()
	if pe == nil || pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED {
		t.Fatalf("empty recipient got %T code=%v, want MALFORMED", got.GetPayload(), pe.GetCode())
	}
	if !strings.Contains(pe.GetDetail(), "recipient") {
		t.Errorf("detail %q should name the offending field", pe.GetDetail())
	}

	// Connection survives; a well-addressed message flows afterwards.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "still here", rigEpoch))
	if got := phone.nextEnvelope(t); got.GetDirectMessage().GetBody() != "still here" {
		t.Fatalf("post-refusal delivery = %T, want the follow-up DM", got.GetPayload())
	}
}

// TestBroadcastToEmptyRoomSucceedsSilently: broadcasting with no other
// connected device is a normal state of a small tailnet, not an error — no
// refusal, no echo, and the connection demonstrably healthy right after.
func TestBroadcastToEmptyRoomSucceedsSilently(t *testing.T) {
	rig := startPresenceRig(t)
	laptop := rig.connectDevice(t, laptopName)
	laptop.assertSilent(t)

	laptop.send(t, broadcastEnvelope(message.NewID(rig.clk.Now()), "anyone there?", rigEpoch))
	laptop.assertSilent(t)

	// Causality probe: phone's arrival produces laptop's ONLINE event, which
	// can only arrive if the connection stayed open and dispatching.
	phone := rig.connectDevice(t, phoneName)
	if got := laptop.nextPresence(t); got.GetDevice() != phoneName {
		t.Fatalf("probe saw %+v, want phone ONLINE (broadcast must not have closed or wedged the connection)",
			got)
	}
	_ = phone
}

// TestSenderIdentityComesFromTailnetNotPayload pins the attribution rule:
// routing decisions cite the RESOLVED identity. There is no sender field on
// the wire to forge, and the log lines must carry the tailnet-resolved name —
// asserted against captured logs, which are the operator's view of who sent
// what (names and byte counts only, never bodies).
func TestSenderIdentityComesFromTailnetNotPayload(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]

	laptop.send(t, broadcastEnvelope(message.NewID(rig.clk.Now()), "hi all", rigEpoch))
	phone.nextEnvelope(t) // consume the delivery

	// Causal sync before reading logs: the fan-out WRITES frames before it
	// LOGS, so consuming a delivery does not prove the log line exists yet.
	// The Hello/ack round trip proves laptop's read loop finished the whole
	// dispatch, logging included.
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)

	out := rig.logs.String()
	if !strings.Contains(out, "sender="+laptopName) {
		t.Errorf("fan-out log missing resolved sender %q; log output:\n%s", laptopName, out)
	}
	if strings.Contains(out, "hi all") {
		t.Errorf("log output contains message BODY — the no-content rule is violated; log:\n%s", out)
	}
}

// Compile-time interface check: the recording sink satisfies the seam it
// doubles for.
var _ OfflineSink = (*recordingSink)(nil)
