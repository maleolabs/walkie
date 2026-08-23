package messagehub

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
)

// Tests for the sealed-delivery half of the seam (ts:queue-sealed-box): the
// recipient's open-and-fold path, its loud whole-frame drops, and the dedup
// key riding INSIDE the box. The coordinator side — sealing, storage bytes,
// wire framing — is proven in internal/coordinator and internal/coordinator/
// queue; these tests pin what the recipient's client does with a frame.

// sealedFrame seals inner into a SealedDelivery envelope addressed to id's
// holder, exactly as the coordinator's queue drain emits it: outer position
// set, everything else about the message inside the box.
func sealedFrame(t *testing.T, id *crypto.IdentityKey, position uint64, inner *walkiev1.Envelope) *walkiev1.Envelope {
	t.Helper()
	plain, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal inner envelope: %v", err)
	}
	box, err := crypto.Seal(id.PublicKey(), plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	return &walkiev1.Envelope{
		Position: position,
		Payload: &walkiev1.Envelope_SealedDelivery{SealedDelivery: &walkiev1.SealedDelivery{
			Ciphertext: box,
		}},
	}
}

// queuedDirect builds the stamped envelope that rides INSIDE a sealed box:
// identity fields, both timestamps, sender attribution — everything except
// the position, which lives on the outer frame.
func queuedDirect(id, body string, at time.Time) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId:  id,
		SentAt:     timestamppb.New(at),
		ReceivedAt: timestamppb.New(at),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: localName,
			Sender:    peerName,
			Body:      body,
		}},
	}
}

func newIdentityHub(t *testing.T) (*Hub, *crypto.IdentityKey, *bytes.Buffer) {
	t.Helper()
	hub, _ := newTestHub(t)
	id, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	t.Cleanup(id.Zero)
	buf := &bytes.Buffer{}
	hub.logger = slog.New(slog.NewTextHandler(buf, nil))
	hub.UseIdentity(id)
	return hub, id, buf
}

// TestApplyOpensSealedDeliveryAndFilesInner is the happy path: the box opens
// with this device's key, the INNER envelope files exactly as live text
// would — same ULID, sender attribution, both timestamps — and the outer
// frame contributes nothing but the ack handle.
func TestApplyOpensSealedDeliveryAndFilesInner(t *testing.T) {
	hub, id, _ := newIdentityHub(t)

	frame := sealedFrame(t, id, 3, queuedDirect("01SEALED0001", "queued while offline", epoch))
	msg, show := hub.Apply(frame)
	if !show {
		t.Fatal("first delivery of a sealed message reported duplicate")
	}
	if msg.ID != "01SEALED0001" || msg.Body != "queued while offline" || msg.Sender != peerName || msg.Recipient != localName {
		t.Fatalf("filed message drifted: %+v", msg)
	}
	if msg.SentAt.IsZero() || msg.ReceivedAt.IsZero() {
		t.Fatalf("timestamps lost across seal/open: sent=%v received=%v", msg.SentAt, msg.ReceivedAt)
	}
	if got := hub.Conversation(message.ConversationKey(peerName)); len(got) != 1 || got[0].Body != "queued while offline" {
		t.Fatalf("conversation = %+v, want the one opened message", got)
	}
}

// TestApplyDedupsSealedRedeliveryOnInnerULID pins where the dedup key lives:
// at-least-once redelivery of the SAME box (and of a re-sealed box carrying
// the same inner message) displays once — keyed on the inner ULID, not on
// anything the outer frame could vary.
func TestApplyDedupsSealedRedeliveryOnInnerULID(t *testing.T) {
	hub, id, _ := newIdentityHub(t)

	first := sealedFrame(t, id, 3, queuedDirect("01SEALED0002", "once only", epoch))
	if _, show := hub.Apply(first); !show {
		t.Fatal("first delivery reported duplicate")
	}
	// Redelivered verbatim (at-least-once), and again under a different
	// outer position after a reconnect.
	if _, show := hub.Apply(sealedFrame(t, id, 3, queuedDirect("01SEALED0002", "once only", epoch))); show {
		t.Fatal("verbatim redelivery displayed twice")
	}
	if _, show := hub.Apply(sealedFrame(t, id, 9, queuedDirect("01SEALED0002", "once only", epoch))); show {
		t.Fatal("re-wrapped redelivery displayed twice — dedup must key on the inner ULID")
	}
}

// TestApplyDropsTamperedBoxWholeLoudly is criterion 4 at the recipient: a
// bit-flipped box fails authentication, files NOTHING, and the loud error is
// content-free — no ciphertext, no body, no key material in the log.
func TestApplyDropsTamperedBoxWholeLoudly(t *testing.T) {
	hub, id, logs := newIdentityHub(t)

	frame := sealedFrame(t, id, 5, queuedDirect("01SEALED0003", "secret body", epoch))
	sd := frame.GetSealedDelivery()
	tampered := append([]byte(nil), sd.GetCiphertext()...)
	tampered[len(tampered)-1] ^= 0x01

	msg, show := hub.Apply(&walkiev1.Envelope{
		Position: 5,
		Payload: &walkiev1.Envelope_SealedDelivery{SealedDelivery: &walkiev1.SealedDelivery{
			Ciphertext: tampered,
		}},
	})
	if show || msg != (message.Message{}) {
		t.Fatalf("tampered box produced output %+v (show=%v) — must drop WHOLE", msg, show)
	}
	if got := hub.Conversation(peerName); len(got) != 0 {
		t.Fatalf("tampered box filed %d messages, want 0 (never partial)", len(got))
	}
	out := logs.String()
	if !strings.Contains(out, "UNREADABLE") || !strings.Contains(out, `position=5`) {
		t.Fatalf("drop not logged loudly with its position; log:\n%s", out)
	}
	for _, forbidden := range []string{"secret body", string(tampered)} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("log carries forbidden material (%q…); log:\n%s", forbidden[:12], out)
		}
	}
}

// TestApplyDropsLoudlyWithoutIdentityWired: a hub with no identity key can
// open nothing; every sealed frame is dropped with a loud, content-free
// error rather than silently vanishing.
func TestApplyDropsLoudlyWithoutIdentityWired(t *testing.T) {
	hub, id, hubLogs := newIdentityHub(t)
	other, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer other.Zero()

	// Simulate "no crypto wired": a fresh hub without UseIdentity.
	bare := New(localName, clock.NewFake(epoch), nil)
	buf := &bytes.Buffer{}
	bare.logger = slog.New(slog.NewTextHandler(buf, nil))

	frame := sealedFrame(t, id, 2, queuedDirect("01SEALED0004", "unreadable", epoch))
	if _, show := bare.Apply(frame); show {
		t.Fatal("identity-less hub displayed a sealed delivery")
	}
	if !strings.Contains(buf.String(), "no identity key wired") {
		t.Fatalf("identity-less drop not logged loudly; log:\n%s", buf.String())
	}

	// And a box sealed to someone ELSE's key fails authentication here too —
	// wrong-recipient and tamper are indistinguishable by design.
	if _, show := hub.Apply(sealedFrame(t, other, 4, queuedDirect("01SEALED0005", "not yours", epoch))); show {
		t.Fatal("box sealed to another key was opened")
	}
	if !strings.Contains(hubLogs.String(), "UNREADABLE") {
		t.Fatalf("wrong-key drop not logged loudly; log:\n%s", hubLogs.String())
	}
}

// TestUseIdentityRefusesNil: forgetting the key must fail at wiring time,
// not surface later as silently vanishing mail.
func TestUseIdentityRefusesNil(t *testing.T) {
	hub, _ := newTestHub(t)
	defer func() {
		if recover() == nil {
			t.Fatal("UseIdentity(nil) accepted — would drop every sealed delivery quietly-configured")
		}
	}()
	hub.UseIdentity(nil)
}
