package messagehub

import (
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Unit tests for the client seam: composition, local filing and the Apply
// fold, all on a fake clock. The wire-level behaviour — what the coordinator
// does to these envelopes, what comes back — is proven end-to-end in
// internal/coordinator's messaging suite; these tests pin the seam's OWN
// contracts.

const (
	localName = "laptop.tail-scale.ts.net."
	peerName  = "phone.tail-scale.ts.net."
)

var epoch = time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)

func newTestHub(t *testing.T) (*Hub, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(epoch)
	return New(localName, clk, nil), clk
}

// deliveredEnvelope builds the envelope a coordinator routes back: identity
// fields carried over, sender filled from the resolved identity, received_at
// stamped at ingress.
func deliveredEnvelope(id string, sentAt time.Time, receivedAt time.Time, dm *walkiev1.DirectMessage) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId:  id,
		SentAt:     timestamppb.New(sentAt),
		ReceivedAt: timestamppb.New(receivedAt),
		Payload:    &walkiev1.Envelope_DirectMessage{DirectMessage: dm},
	}
}

func TestSendDirectComposesAndFilesLocally(t *testing.T) {
	hub, _ := newTestHub(t)

	env, err := hub.SendDirect(peerName, "hello phone")
	if err != nil {
		t.Fatalf("SendDirect: %v", err)
	}

	dm := env.GetDirectMessage()
	if dm == nil {
		t.Fatalf("payload = %T, want DirectMessage", env.GetPayload())
	}
	if dm.GetRecipient() != peerName || dm.GetBody() != "hello phone" {
		t.Errorf("recipient/body = %q/%q, want peer/hello phone", dm.GetRecipient(), dm.GetBody())
	}
	if env.GetMessageId() == "" {
		t.Error("message_id empty: the ULID is the whole dedup mechanism, it must exist")
	}
	if !env.GetSentAt().AsTime().Equal(epoch) {
		t.Errorf("sent_at = %v, want injected-clock now %v", env.GetSentAt().AsTime(), epoch)
	}
	// Attribution is the coordinator's job: the composed payload must NOT
	// carry a client-asserted sender (see the field comment in SendDirect).
	if dm.GetSender() != "" {
		t.Errorf("composed sender = %q, want unset (attribution is server-authoritative)", dm.GetSender())
	}

	// Local filing: the user sees their own side of the conversation, with
	// the honest unstamped ReceivedAt.
	filed := hub.Conversation(message.ConversationKey(peerName))
	if len(filed) != 1 {
		t.Fatalf("local conversation has %d messages, want 1", len(filed))
	}
	m := filed[0]
	if m.Sender != localName || m.Recipient != peerName || m.Body != "hello phone" || m.ID != env.GetMessageId() {
		t.Errorf("filed = %+v, want own message matching the envelope", m)
	}
	if m.SentAt.IsZero() || !m.SentAt.Equal(epoch) {
		t.Errorf("filed SentAt = %v, want %v", m.SentAt, epoch)
	}
	if !m.ReceivedAt.IsZero() {
		t.Errorf("filed ReceivedAt = %v, want zero (no ack exists to carry ingress time back)", m.ReceivedAt)
	}
}

func TestSendBroadcastFilesIntoBroadcastConversation(t *testing.T) {
	hub, _ := newTestHub(t)

	env, err := hub.SendBroadcast("standup in 5")
	if err != nil {
		t.Fatalf("SendBroadcast: %v", err)
	}
	bm := env.GetBroadcastMessage()
	if bm == nil || bm.GetBody() != "standup in 5" {
		t.Fatalf("payload = %+v, want broadcast with body", env.GetPayload())
	}
	if bm.GetSender() != "" {
		t.Errorf("composed sender = %q, want unset", bm.GetSender())
	}

	filed := hub.Conversation(message.BroadcastConversation)
	if len(filed) != 1 || filed[0].Sender != localName || !filed[0].IsBroadcast() {
		t.Fatalf("broadcast filing = %+v, want own broadcast in %q", filed, message.BroadcastConversation)
	}
}

func TestSendRefusesOversizedDraftBeforeWire(t *testing.T) {
	hub, _ := newTestHub(t)

	tooBig := strings.Repeat("x", message.MaxBodyBytes+1)
	if _, err := hub.SendDirect(peerName, tooBig); err == nil {
		t.Fatal("oversized draft accepted locally; validation must run before any wire byte")
	}
	if _, err := hub.SendBroadcast(tooBig); err == nil {
		t.Fatal("oversized broadcast accepted locally")
	}
	if _, err := hub.SendDirect("", "to whom?"); err == nil {
		t.Fatal("recipient-less DM accepted locally")
	}

	// A refused draft files nothing: refusal means nothing was said.
	for _, key := range hub.Conversations() {
		t.Errorf("refused draft filed into %q", key)
	}
}

func TestApplyAttributesAndFilesInboundDirect(t *testing.T) {
	hub, _ := newTestHub(t)

	sentAt := epoch.Add(-90 * time.Second) // skewed sender clock
	receivedAt := epoch
	env := deliveredEnvelope("01J8Z9P3Q7V6M4T8J2WXYR5N6C", sentAt, receivedAt,
		&walkiev1.DirectMessage{Recipient: localName, Body: "hi", Sender: peerName})

	msg, displayed := hub.Apply(env)
	if !displayed {
		t.Fatal("first delivery not displayed")
	}
	if msg.Sender != peerName {
		t.Errorf("sender = %q, want coordinator-attributed %q", msg.Sender, peerName)
	}
	if !msg.SentAt.Equal(sentAt) || !msg.ReceivedAt.Equal(receivedAt) {
		t.Errorf("timestamps = %v/%v, want both preserved (%v/%v)", msg.SentAt, msg.ReceivedAt, sentAt, receivedAt)
	}
	if got := hub.Conversation(message.ConversationKey(peerName)); len(got) != 1 || got[0].ID != msg.ID {
		t.Errorf("conversation = %+v, want the one inbound message", got)
	}
}

func TestApplyDuplicateULIDDisplayedOnce(t *testing.T) {
	hub, _ := newTestHub(t)

	env := deliveredEnvelope("01J8Z9P3Q7V6M4T8J2WXYR5N6C", epoch, epoch.Add(time.Second),
		&walkiev1.DirectMessage{Recipient: localName, Body: "once", Sender: peerName})

	if _, first := hub.Apply(env); !first {
		t.Fatal("first delivery reported duplicate")
	}
	_, second := hub.Apply(env)
	if second {
		t.Fatal("same ULID displayed twice; dedup gate did not hold at the display decision")
	}
	if got := hub.Conversation(message.ConversationKey(peerName)); len(got) != 1 {
		t.Fatalf("conversation holds %d messages after redelivery, want exactly 1", len(got))
	}
}

func TestApplyBroadcastLandsInBroadcastConversation(t *testing.T) {
	hub, _ := newTestHub(t)

	env := &walkiev1.Envelope{
		MessageId:  "01J8Z9P3Q7V6M4T8J2WXYR5N6C",
		SentAt:     timestamppb.New(epoch),
		ReceivedAt: timestamppb.New(epoch),
		Payload: &walkiev1.Envelope_BroadcastMessage{BroadcastMessage: &walkiev1.BroadcastMessage{
			Body: "maintenance", Sender: peerName,
		}},
	}

	msg, displayed := hub.Apply(env)
	if !displayed || msg.Sender != peerName || !msg.IsBroadcast() {
		t.Fatalf("apply = (%+v, %v), want attributed broadcast displayed", msg, displayed)
	}
	if got := hub.Conversation(message.BroadcastConversation); len(got) != 1 {
		t.Fatalf("broadcast conversation holds %d messages, want 1", len(got))
	}
}

func TestApplyIgnoresNonTextPayloads(t *testing.T) {
	hub, _ := newTestHub(t)

	others := []*walkiev1.Envelope{
		{Payload: &walkiev1.Envelope_HelloAck{HelloAck: &walkiev1.HelloAck{Device: localName}}},
		{Payload: &walkiev1.Envelope_PresenceUpdate{PresenceUpdate: &walkiev1.PresenceUpdate{
			Device: peerName, State: walkiev1.PresenceState_PRESENCE_STATE_ONLINE,
		}}},
		{Payload: &walkiev1.Envelope_ProtocolError{ProtocolError: &walkiev1.ProtocolError{
			Code: walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
		}}},
		nil,
	}
	for i, env := range others {
		if _, displayed := hub.Apply(env); displayed {
			t.Errorf("case %d: non-text payload displayed", i)
		}
	}
	if keys := hub.Conversations(); len(keys) != 0 {
		t.Errorf("ignored payloads created conversations %v", keys)
	}
}

// Mixed fleet (adr:003): an old coordinator that never fills the sender field
// still delivers displayable text — attribution degrades to absent, never to
// wrong, and the message files under the only honest reading.
func TestApplyToleratesMissingSenderFromOldCoordinator(t *testing.T) {
	hub, _ := newTestHub(t)

	env := deliveredEnvelope("01J8Z9P3Q7V6M4T8J2WXYR5N6C", epoch, epoch,
		&walkiev1.DirectMessage{Recipient: localName, Body: "from the past"})

	msg, displayed := hub.Apply(env)
	if !displayed {
		t.Fatal("pre-sender delivery dropped; mixed-fleet tolerance broken")
	}
	if msg.Sender != "" {
		t.Errorf("sender = %q, want empty", msg.Sender)
	}
	if got := hub.Conversation(message.ConversationKey("")); len(got) != 1 {
		t.Errorf("unattributed message not filed (got %d), want filed under its unnamed sender", len(got))
	}
}
