package main

import (
	"bytes"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/history"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/outbox"
	"github.com/maleolabs/walkie/internal/presenceview"
	"github.com/maleolabs/walkie/internal/store"
)

// The assembly duties, driven against fakes and real local stores: the ack
// discipline for sealed deliveries, the outbox drain contract, the key
// announce shape, and the composition path's hold-on-failure policy. No
// network, no sleeps — control.Client itself is proven in its own package;
// this file proves what cmd/walkie ADDS on top of it.

var testEpoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// fakeClient records what the duties do to the connection. failAt makes the
// Nth Send fail (1-based), which is how the drain tests draw a mid-drain
// transport failure deterministically.
type fakeClient struct {
	sent     []*walkiev1.Envelope
	acks     []uint64
	sendErr  error
	failAt   int
	identity string
}

func (f *fakeClient) Send(env *walkiev1.Envelope) error {
	if f.failAt == len(f.sent)+1 {
		return errors.New("write failed mid-stream")
	}
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, env)
	return nil
}

func (f *fakeClient) Acknowledge(position uint64) { f.acks = append(f.acks, position) }

func (f *fakeClient) ConnectedAs() string { return f.identity }

type fixture struct {
	app      *app
	client   *fakeClient
	box      *outbox.Outbox
	hist     *history.Store
	presence *presenceview.View
	key      *crypto.IdentityKey
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clk := clock.NewFake(testEpoch)
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	fc := &fakeClient{}

	// Real stores over temp files: the durability contracts under test
	// (outbox hold, history append) are store-backed, so the tests exercise
	// the real seams rather than stand-ins.
	stPath := filepath.Join(t.TempDir(), "state.db")
	hist, err := history.Open(stPath, clk, history.Options{
		TTL:         time.Hour,
		MaxMessages: 100,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("open history: %v", err)
	}
	box, err := outbox.New(mustStore(t, stPath, clk), clk, logger)
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	key, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	presence := presenceview.New(logger)

	t.Cleanup(func() { hist.Close() })

	f := &fixture{
		client:   fc,
		box:      box,
		hist:     hist,
		presence: presence,
		key:      key,
	}
	f.app = newApp(clk, logger, fc, box, hist, key, presence)
	return f
}

func mustStore(t *testing.T, path string, clk clock.Clock) *store.Store {
	t.Helper()
	st, err := store.Open(path, clk)
	if err != nil {
		t.Fatalf("open store %s: %v", path, err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func directEnvelopeFrom(sender, recipient, body string) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId:  message.NewID(testEpoch),
		SentAt:     timestamppb.New(testEpoch),
		ReceivedAt: timestamppb.New(testEpoch),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Sender:    sender,
			Recipient: recipient,
			Body:      body,
		}},
	}
}

func presenceEnvelope(device string) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PresenceUpdate{PresenceUpdate: &walkiev1.PresenceUpdate{
			Device: device,
			State:  walkiev1.PresenceState_PRESENCE_STATE_ONLINE,
		}},
	}
}

// -- sealed delivery: ack regardless of verdict ------------------------------

func TestSealedDeliveryAckedRegardlessOfVerdict(t *testing.T) {
	f := newFixture(t)
	f.client.identity = "laptop"

	// A box sealed to a DIFFERENT key: Apply cannot open it, the verdict is
	// false, and the frame must STILL be acked at its outer position —
	// withholding the ack would freeze the queue behind an unreadable frame
	// (the messagehub contract this assembly implements).
	otherKey, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	inner, err := proto.Marshal(directEnvelopeFrom("alice", "laptop", "sealed body"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := crypto.Seal(otherKey.PublicKey(), inner)
	if err != nil {
		t.Fatal(err)
	}
	env := &walkiev1.Envelope{
		Position: 7,
		Payload: &walkiev1.Envelope_SealedDelivery{SealedDelivery: &walkiev1.SealedDelivery{
			Ciphertext: sealed,
		}},
	}

	f.app.OnEnvelope(env)

	if len(f.client.acks) != 1 || f.client.acks[0] != 7 {
		t.Errorf("acks = %v, want [7]: an unreadable box is still terminal", f.client.acks)
	}
	select {
	case <-f.app.inbound:
		t.Error("an unreadable box must not notify the UI")
	default:
	}
}

func TestSealedDeliveryOpenedAndFiledWhenKeyMatches(t *testing.T) {
	f := newFixture(t)
	f.client.identity = "laptop"

	inner, err := proto.Marshal(directEnvelopeFrom("alice", "laptop", "queued while offline"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := crypto.Seal(f.key.PublicKey(), inner)
	if err != nil {
		t.Fatal(err)
	}
	env := &walkiev1.Envelope{
		Position: 3,
		Payload: &walkiev1.Envelope_SealedDelivery{SealedDelivery: &walkiev1.SealedDelivery{
			Ciphertext: sealed,
		}},
	}

	f.app.OnEnvelope(env)

	if len(f.client.acks) != 1 || f.client.acks[0] != 3 {
		t.Fatalf("acks = %v, want [3]", f.client.acks)
	}
	rows, err := f.hist.Query(history.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Message.Body != "queued while offline" {
		t.Errorf("opened delivery must land in history, got %+v", rows)
	}
	select {
	case got := <-f.app.inbound:
		if got.Body != "queued while offline" {
			t.Errorf("ui notified with %q", got.Body)
		}
	default:
		t.Error("opened delivery must notify the UI")
	}
}

// -- outbox drain ------------------------------------------------------------

func TestDrainSendsInOrderAndRemovesOnlyOnSuccess(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"01", "02", "03"} {
		if err := f.box.Enqueue(directEnvelopeFrom("me", "alice", "body "+id)); err != nil {
			t.Fatal(err)
		}
	}
	// The second write fails: drain must stop there with only the first
	// removed. Head-of-line blocking preserves per-conversation order — the
	// outbox package's documented contract.
	f.client.failAt = 2

	f.app.drainOutbox()

	if n, _ := f.box.Count(); n != 2 {
		t.Errorf("outbox holds %d messages, want 2 (first was transmitted and removed)", n)
	}
	if len(f.client.sent) != 1 {
		t.Errorf("%d frames transmitted, want exactly 1 before the failure", len(f.client.sent))
	}
}

func TestDrainKeepsEverythingWhenOffline(t *testing.T) {
	f := newFixture(t)
	if err := f.box.Enqueue(directEnvelopeFrom("me", "alice", "held")); err != nil {
		t.Fatal(err)
	}
	f.client.sendErr = control.ErrOffline

	f.app.drainOutbox()

	if n, _ := f.box.Count(); n != 1 {
		t.Errorf("offline drain removed the held message; count = %d, want 1", n)
	}
}

// -- announce duty -----------------------------------------------------------

func TestAnnouncePublishesOwnPublicKey(t *testing.T) {
	f := newFixture(t)

	f.app.announceKey()

	if len(f.client.sent) != 1 {
		t.Fatalf("announce sent %d frames, want 1", len(f.client.sent))
	}
	ann := f.client.sent[0].GetPublicKeyAnnounce()
	if ann == nil {
		t.Fatalf("payload is %T, want PublicKeyAnnounce", f.client.sent[0].GetPayload())
	}
	pub, err := crypto.ParsePublicKey(ann.GetPublicKey())
	if err != nil {
		t.Fatalf("announced key malformed: %v", err)
	}
	if pub != f.key.PublicKey() {
		t.Error("announced key differs from this device's identity")
	}
}

// -- hub lifecycle + composition path ----------------------------------------

func TestHubBuiltLazilyFromHelloAckIdentity(t *testing.T) {
	f := newFixture(t)
	if f.app.hubSource() != nil {
		t.Fatal("hub must not exist before the coordinator echoed an identity")
	}

	f.client.identity = "laptop.tail-scale.ts.net."
	f.app.OnEnvelope(directEnvelopeFrom("alice", "laptop", "hello"))

	src := f.app.hubSource()
	if src == nil {
		t.Fatal("hub must exist after the first post-handshake envelope")
	}
	if got := src.Conversations(); len(got) == 0 {
		t.Error("delivered message should have created a conversation")
	}
}

func TestSendFromUIHoldsMessageOnTransmitFailure(t *testing.T) {
	f := newFixture(t)
	f.client.identity = "laptop"
	f.app.OnEnvelope(presenceEnvelope("alice")) // create the dm conversation target
	f.client.sendErr = errors.New("socket died mid-write")

	err := f.app.SendFromUI(message.ConversationKey("alice"), "important")
	if err == nil {
		t.Fatal("expected the failure to surface to the user")
	}

	// Held durably despite the failure: a write error leaves the frame's fate
	// unknown, and holding is always safe under receiver-side ULID dedup.
	if n, _ := f.box.Count(); n != 1 {
		t.Errorf("outbox holds %d messages, want 1", n)
	}
	// And the user still sees their own side immediately.
	select {
	case got := <-f.app.inbound:
		if got.Body != "important" {
			t.Errorf("notified body %q", got.Body)
		}
	default:
		t.Error("composed message must appear in the pane even when held")
	}
	// History recorded the composition fact.
	rows, err := f.hist.Query(history.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Message.Body != "important" {
		t.Errorf("history missing the composed message: %+v", rows)
	}
}

func TestSendFromUIRefusedBeforeFirstHandshake(t *testing.T) {
	f := newFixture(t) // no identity yet: hub does not exist

	err := f.app.SendFromUI(message.BroadcastConversation, "too early")
	if err == nil {
		t.Fatal("composition before any handshake must be refused loudly, not silently misfiled")
	}
	if n, _ := f.box.Count(); n != 0 {
		t.Errorf("refused draft leaked into the outbox (%d rows)", n)
	}
}

func TestOversizedDraftRefusedBeforeAnyWireByte(t *testing.T) {
	f := newFixture(t)
	f.client.identity = "laptop"

	big := make([]byte, message.MaxBodyBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	if err := f.app.SendFromUI(message.BroadcastConversation, string(big)); err == nil {
		t.Fatal("oversized draft must be refused locally")
	}
	if len(f.client.sent) != 0 {
		t.Errorf("%d frames sent for a refused draft", len(f.client.sent))
	}
	if n, _ := f.box.Count(); n != 0 {
		t.Errorf("refused draft leaked into the outbox (%d rows)", n)
	}
}
