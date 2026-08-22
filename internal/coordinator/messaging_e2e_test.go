package coordinator

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/messagehub"
)

// The tests in this file drive sto:text-messaging END-TO-END through the real
// server AND the real client seam: a [messagehub.Hub] on each device composes
// and folds, the presence rig carries the bytes, and every claim is made
// deterministic by causality and the fake clock — never a sleep. Slice 1's
// routing_test.go pins what the coordinator does to envelopes; these pin what
// arrives at the consumer the UI will be.

// nextTextEnvelope returns the next DIRECT- or BROADCAST-message envelope,
// skipping presence chatter that may legitimately be interleaved with it.
//
// Why the skip exists: any test that jumps the fake clock across the rig's
// presence TTL produces liveness events (expiry, and resurrection if devices
// re-lease), and those events can land on a device's stream either side of
// the text frame under test — the presence pump and the routing path write
// to the same socket through one mutex, which serializes but does not order
// them. Skipping is deterministic anyway: the chatter is finite and already
// queued once the synced heartbeats' acks returned, so the text frame is
// guaranteed to be next-or-later, and the timeout fails loudly if it never
// comes.
func nextTextEnvelope(t *testing.T, c *testClient) *walkiev1.Envelope {
	t.Helper()
	for {
		env := c.nextEnvelope(t)
		if env.GetDirectMessage() != nil || env.GetBroadcastMessage() != nil {
			return env
		}
	}
}

// TestEndToEndDirectMessageAttributedAndStamped is criteria 1 and 4 through
// the whole path: A composes via its Hub, B's Hub displays exactly ONE
// message, attributed to A by the coordinator (not by anything A claimed on
// the wire), with BOTH timestamps intact — the sender's skewed sent_at and
// the coordinator's later ingress stamp — exposed on the seam for rendering.
func TestEndToEndDirectMessageAttributedAndStamped(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]
	laptopHub := messagehub.New(laptopName, rig.clk, nil)
	phoneHub := messagehub.New(phoneName, rig.clk, nil)

	env, err := laptopHub.SendDirect(phoneName, "hello phone")
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	sentAt := rig.clk.Now()

	// Skew the two clocks apart BEFORE the wire crossing, so the assertions
	// can tell which timestamp came from whom: sent_at stays at the compose
	// instant, received_at is stamped 90 seconds later at ingress.
	//
	// The jump crosses the rig's presence TTL, so both devices re-lease with
	// synced heartbeats immediately after it — otherwise the expiry machinery
	// would mark them offline mid-test. Any liveness chatter the jump and the
	// resurrections produced is finite and queued by the time both acks
	// return; nextTextEnvelope skips it below.
	rig.clk.Advance(90 * time.Second)
	ingress := rig.clk.Now()
	laptop.heartbeat(t)
	phone.heartbeat(t)

	laptop.send(t, env)
	got := nextTextEnvelope(t, phone)

	msg, displayed := phoneHub.Apply(got)
	if !displayed {
		t.Fatalf("criterion 1: first delivery not displayed; got payload %T", got.GetPayload())
	}
	if msg.Sender != laptopName {
		t.Errorf("sender = %q, want coordinator-attributed %q", msg.Sender, laptopName)
	}
	if msg.Body != "hello phone" || msg.ID != env.GetMessageId() {
		t.Errorf("body/id = %q/%q, want composed values intact", msg.Body, msg.ID)
	}
	if !msg.SentAt.Equal(sentAt) {
		t.Errorf("sent_at = %v, want sender's compose instant %v (skewed half preserved)", msg.SentAt, sentAt)
	}
	if !msg.ReceivedAt.Equal(ingress) {
		t.Errorf("received_at = %v, want coordinator ingress %v", msg.ReceivedAt, ingress)
	}

	// Criterion 4, query half: the same two timestamps come back through the
	// conversation snapshot the UI will actually read.
	filed := phoneHub.Conversation(message.ConversationKey(laptopName))
	if len(filed) != 1 {
		t.Fatalf("criterion 1: B's conversation holds %d messages, want exactly 1", len(filed))
	}
	if !filed[0].SentAt.Equal(sentAt) || !filed[0].ReceivedAt.Equal(ingress) {
		t.Errorf("snapshot timestamps = %v/%v, want both halves surviving to the query surface",
			filed[0].SentAt, filed[0].ReceivedAt)
	}
	if keys := phoneHub.Conversations(); len(keys) != 1 || keys[0] != message.ConversationKey(laptopName) {
		t.Fatalf("conversations = %v, want only the laptop DM thread", keys)
	}

	// A's own side: the locally filed copy sits in the same peer conversation
	// from A's perspective, so the thread interleaves both directions.
	own := laptopHub.Conversation(message.ConversationKey(phoneName))
	if len(own) != 1 || own[0].ID != env.GetMessageId() {
		t.Fatalf("sender's own filing = %+v, want the composed message", own)
	}
}

// TestEndToEndBroadcastReachesEveryOtherHub is criterion 2 through the seam:
// one broadcast from A lands displayed on every OTHER connected device,
// attributed to A, filed in the shared broadcast conversation — and A itself
// receives nothing (no echo; its own copy came from the local filing).
func TestEndToEndBroadcastReachesEveryOtherHub(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName, tabletName)
	laptop, phone, tablet := clients[0], clients[1], clients[2]
	hubs := map[string]*messagehub.Hub{
		laptopName: messagehub.New(laptopName, rig.clk, nil),
		phoneName:  messagehub.New(phoneName, rig.clk, nil),
		tabletName: messagehub.New(tabletName, rig.clk, nil),
	}

	env, err := hubs[laptopName].SendBroadcast("standup in 5")
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	laptop.send(t, env)

	for _, watcher := range []*testClient{phone, tablet} {
		got := watcher.nextEnvelope(t)
		msg, displayed := hubs[watcher.name].Apply(got)
		if !displayed {
			t.Fatalf("%s: broadcast not displayed", watcher.name)
		}
		if msg.Sender != laptopName {
			t.Errorf("%s: sender = %q, want %q", watcher.name, msg.Sender, laptopName)
		}
		if msg.Body != "standup in 5" {
			t.Errorf("%s: body = %q, want composed body intact", watcher.name, msg.Body)
		}
		filed := hubs[watcher.name].Conversation(message.BroadcastConversation)
		if len(filed) != 1 {
			t.Fatalf("%s: broadcast conversation holds %d messages, want 1", watcher.name, len(filed))
		}
	}

	// The sender is not in its own audience; the causal backstop proves
	// nothing preceded the probe on laptop's wire.
	laptop.assertSilent(t)
	phone.send(t, directEnvelope(message.NewID(rig.clk.Now()), laptopName, "backstop", rigEpoch))
	if probe := laptop.nextEnvelope(t); probe.GetDirectMessage().GetBody() != "backstop" {
		t.Fatalf("laptop's first inbound frame was %T, want the backstop DM (a self-echo preceded it)",
			probe.GetPayload())
	}
}

// TestEndToEndDuplicateULIDDisplayedOnce is criterion 3 through the FULL
// receive path — wire, server redelivery, Hub fold — not just the unit-level
// dedup: the same ULID crosses twice (what reconnect replay looks like), the
// receiver's Hub displays it once, and the conversation holds one message.
func TestEndToEndDuplicateULIDDisplayedOnce(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]
	phoneHub := messagehub.New(phoneName, rig.clk, nil)

	id := message.NewID(rig.clk.Now())
	env := directEnvelope(id, phoneName, "once", rigEpoch)

	laptop.send(t, env)
	first, firstDisplayed := phoneHub.Apply(phone.nextEnvelope(t))
	if !firstDisplayed {
		t.Fatal("first delivery reported duplicate")
	}

	// Redelivery of the SAME ULID — the server routes it again (at-least-once
	// on the wire is the contract; the receiver makes it harmless).
	laptop.send(t, env)
	_, secondDisplayed := phoneHub.Apply(phone.nextEnvelope(t))
	if secondDisplayed {
		t.Fatal("criterion 3: duplicate ULID displayed a second time")
	}

	filed := phoneHub.Conversation(message.ConversationKey(laptopName))
	if len(filed) != 1 || filed[0].ID != first.ID {
		t.Fatalf("conversation holds %+v, want exactly the one displayed message", filed)
	}
}

// TestClientSetSenderOverwrittenByCoordinator pins the attribution rule at
// the seam: a client that forges DirectMessage.sender (what a hostile or
// buggy build would put on the wire) cannot make its message appear under
// another device's name — the coordinator fills sender from the connection's
// resolved identity, and the forged value never survives transit.
func TestClientSetSenderOverwrittenByCoordinator(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]
	phoneHub := messagehub.New(phoneName, rig.clk, nil)

	// Hand-built envelope with a FORGED sender field — deliberately not via
	// Hub.SendDirect, which refuses to set one.
	forged := &walkiev1.Envelope{
		MessageId: message.NewID(rig.clk.Now()),
		SentAt:    timestamppb.New(rigEpoch),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: phoneName,
			Body:      "signed, ghost",
			Sender:    "ghost.tail-scale.ts.net.",
		}},
	}
	laptop.send(t, forged)

	msg, displayed := phoneHub.Apply(phone.nextEnvelope(t))
	if !displayed {
		t.Fatal("forged-sender delivery dropped entirely; must arrive re-attributed instead")
	}
	if msg.Sender != laptopName {
		t.Fatalf("sender = %q, want the RESOLVED identity %q (forged value must not survive transit)",
			msg.Sender, laptopName)
	}
	if msg.Body != "signed, ghost" {
		t.Errorf("body = %q, want untouched — only attribution is rewritten", msg.Body)
	}
}

// TestHubLocalRefusalKeepsSeamHealthy covers the client half of the refusal
// posture: an oversized draft is refused by the Hub BEFORE any wire byte
// exists (nothing reaches the recipient), and the connection demonstrably
// still works right after — matching the server-side keep-open behaviour the
// routing suite pins for in-band refusals.
func TestHubLocalRefusalKeepsSeamHealthy(t *testing.T) {
	rig := startPresenceRig(t)
	clients := connectAll(t, rig, laptopName, phoneName)
	laptop, phone := clients[0], clients[1]
	laptopHub := messagehub.New(laptopName, rig.clk, nil)
	phoneHub := messagehub.New(phoneName, rig.clk, nil)

	tooBig := strings.Repeat("x", message.MaxBodyBytes+1)
	if _, err := laptopHub.SendDirect(phoneName, tooBig); err == nil {
		t.Fatal("oversized draft accepted by the Hub; validation must run before any wire byte")
	}
	phone.assertSilent(t) // refused locally means nothing was ever sent

	// The connection is untouched by the local refusal: a well-formed message
	// flows immediately afterwards, end to end.
	env, err := laptopHub.SendDirect(phoneName, "still here")
	if err != nil {
		t.Fatalf("post-refusal compose: %v", err)
	}
	laptop.send(t, env)
	got := phone.nextEnvelope(t)
	if _, displayed := phoneHub.Apply(got); !displayed {
		t.Fatal("post-refusal delivery not displayed; seam did not stay healthy")
	}
}
