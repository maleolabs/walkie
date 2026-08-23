// Package messagehub is the client-side text-messaging seam: one type that
// composes outgoing text onto the control plane and folds incoming text into
// the display model.
//
// Owning work item:
//
//	eka get walkie/sto:text-messaging
//
// # Why this is a sibling of internal/message, not part of it
//
// internal/message owns message identity, validation and the receiver-side
// display model, and by its own rule its consumers never touch protobuf types.
// This package is the one place that necessarily does touch them — it is the
// adapter between the wire (walkiev1 envelopes) and that core — so it lives
// here rather than dragging genproto into the core. The split keeps
// sto:message-history able to persist [message.Message] values without
// importing generated code.
//
// # The seam contract
//
// Everything a text consumer needs passes through four calls:
//
//   - [Hub.SendDirect] / [Hub.SendBroadcast] compose an envelope (ULID via
//     message.NewID, body validated BEFORE any wire byte exists) and file the
//     message into the local log, so the user sees their own side of the
//     conversation. The caller — the future ts:reconnect-resume connection,
//     or a test — writes the returned envelope to the transport.
//   - [Hub.Apply] folds one received envelope into the log through
//     [message.Log.Append], which is where dedup-by-ULID sits: at the display
//     decision, not at the socket (criterion 3). Its bool is the display
//     verdict — false means "already shown", wherever the duplicate came
//     from. Sealed queue deliveries (ts:queue-sealed-box) come through here
//     too: with an identity wired ([Hub.UseIdentity]) Apply opens the box
//     with this device's key and folds the INNER envelope in exactly as if
//     it had arrived live; a box that fails authentication is dropped WHOLE
//     with a loud, content-free error — AEAD failure means there is no
//     partial plaintext to act on, and acting on none is the only honest
//     outcome.
//   - [Hub.Conversation] / [Hub.Conversations] expose the log's query surface.
//
// Ack discipline for sealed deliveries, stated where the opener lives: a
// caller acknowledging queue positions should ack a SealedDelivery frame's
// OUTER position REGARDLESS of Apply's verdict — displayed and forfeited are
// both terminal, and withholding the ack would freeze the high-water mark
// behind an unreadable frame, forcing eternal redelivery of everything
// after it. Key loss forfeits that device's queued messages (adr:004
// consequence); the ack records the forfeiture, it does not cause it.
//
// There is deliberately no transport here: no dialing, no read loop, no
// reconnect. internal/control owns the connection lifecycle (ts:reconnect-
// resume); this package works against whatever feeds it envelopes, which is
// exactly what makes a headless client, sto:terminal-ui and ts:control-socket
// consumers of the same seam with no rendering code in the middle.
//
// # Timestamps on the seam (criterion 4)
//
// Every [message.Message] this package produces carries both SentAt and
// ReceivedAt, unmerged and unhidden — skew stays legible. Two honest gaps,
// stated rather than papered over:
//
//   - A LOCALLY FILED sent message has a zero ReceivedAt: the protocol has no
//     ack (an ack would be a read receipt, out of scope), so the sender never
//     learns the coordinator's ingress time for its own message. Renderers
//     should show that as "unstamped", not fake a value.
//   - A message from a pre-sender-field coordinator (adr:003 mixed fleet)
//     arrives with an empty Sender. It still files — under the only honest
//     reading, an unnamed peer — so attribution degrades to absent, never to
//     wrong.
//
// # Ordering, restated at the seam
//
// What [message.Log] offers — per-conversation arrival order — is all this
// package offers. No global order across conversations exists or may be
// assumed; see the message package comment before building on one.
package messagehub

import (
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
)

// Hub is one device's messaging session: the client-side half of
// req:text-messaging. Safe for concurrent use — Apply comes from the
// control-plane read loop while sends and queries come from user-facing
// goroutines; message.Log carries its own locking.
type Hub struct {
	local  string // this device's tailnet name (from HelloAck), used for conversation orientation
	clk    clock.Clock
	logger *slog.Logger
	log    *message.Log

	// identity is this device's X25519 keypair (ts:queue-sealed-box): the
	// private half that opens SealedDelivery frames queued for this device
	// while it was offline. Nil — the default — means sealed frames are
	// dropped loudly rather than opened; wire one with UseIdentity before
	// traffic flows. The key itself is owned by the caller (generated once,
	// persisted owner-only by crypto.LoadOrCreateIdentity); the hub only
	// borrows it for Open, never serializes it, never logs it.
	identity *crypto.IdentityKey
}

// New returns a Hub speaking as the device named local (the resolved identity
// echoed in HelloAck — learned, never asserted). A nil logger falls back to
// slog's default.
func New(local string, clk clock.Clock, logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		local:  local,
		clk:    clk,
		logger: logger,
		log:    message.NewLog(local, 0, 0),
	}
}

// UseIdentity wires this device's X25519 identity key for opening sealed
// queue deliveries (ts:queue-sealed-box). Call once at assembly, before any
// traffic flows — the same set-before-Start discipline as control.Client's
// OnEnvelope — so no read loop can observe the wiring mid-flight. A nil key
// is refused: "forgot to wire crypto" must fail loudly here, not surface
// later as every queued message silently vanishing.
func (h *Hub) UseIdentity(id *crypto.IdentityKey) {
	if id == nil {
		panic("messagehub: UseIdentity: identity must not be nil (leave unwired to drop sealed deliveries loudly)")
	}
	h.identity = id
}

// SendDirect composes a direct message to recipient and files it locally.
//
// The returned envelope is the caller's to write to the transport; nothing has
// touched a network by the time this returns. The body is validated first —
// an oversized draft is refused HERE, before a round trip proves it
// undeliverable, and a refused draft files nothing and sends nothing.
//
// The local filing has SentAt from the injected clock and a ZERO ReceivedAt:
// see the package comment for why that absence is honest rather than sloppy.
func (h *Hub) SendDirect(recipient, body string) (*walkiev1.Envelope, error) {
	if recipient == "" {
		return nil, fmt.Errorf("messagehub: direct message needs a recipient")
	}
	if err := message.ValidateBody(body); err != nil {
		return nil, fmt.Errorf("messagehub: %w", err)
	}

	now := h.clk.Now()
	id := message.NewID(now)
	h.fileLocal(message.Message{
		ID:        id,
		Sender:    h.local,
		Recipient: recipient,
		Body:      body,
		SentAt:    now,
		// ReceivedAt deliberately zero: no ack exists to carry it back.
	})

	return &walkiev1.Envelope{
		MessageId: id,
		SentAt:    timestamppb.New(now),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: recipient,
			Body:      body,
			// Sender left unset ON PURPOSE: attribution is the coordinator's
			// job (server-authoritative, same as presence). A client-set
			// value would be discarded in transit anyway; leaving it unset
			// keeps the wire honest about who filled what.
		}},
	}, nil
}

// SendBroadcast composes a broadcast to every other connected device and
// files it locally, mirroring SendDirect. The coordinator never echoes a
// broadcast to its sender, so this local filing IS how the sender sees its
// own broadcast — there is no second copy coming back to dedup against.
func (h *Hub) SendBroadcast(body string) (*walkiev1.Envelope, error) {
	if err := message.ValidateBody(body); err != nil {
		return nil, fmt.Errorf("messagehub: %w", err)
	}

	now := h.clk.Now()
	id := message.NewID(now)
	h.fileLocal(message.Message{
		ID:     id,
		Sender: h.local,
		Body:   body,
		SentAt: now,
	})

	return &walkiev1.Envelope{
		MessageId: id,
		SentAt:    timestamppb.New(now),
		Payload: &walkiev1.Envelope_BroadcastMessage{BroadcastMessage: &walkiev1.BroadcastMessage{
			Body: body,
		}},
	}, nil
}

// Apply folds one received envelope into the log and reports the display
// decision: the returned Message plus whether it is NEW (true) or a duplicate
// of something already shown (false — criterion 3's verdict, delivered by
// message.Log.Append's dedup gate).
//
// The two text payloads are this package's business, plus — with an identity
// wired — sealed queue deliveries, which are text that travelled encrypted:
// Apply opens the box and folds the INNER envelope in exactly as if it had
// arrived live, dedup keyed on the inner ULID that rode inside the box. A
// box that fails authentication is dropped WHOLE, loudly and content-free:
// there is no partial plaintext to act on, and a forfeited message must not
// look like a display hiccup. Callers ack such a frame's outer position
// regardless of the verdict — see the ack discipline in the package comment.
//
// Everything else (presence, handshake, errors) is ignored untouched — other
// seams (presenceview, the connection state machine) own those, and
// swallowing them here would hide traffic another consumer is waiting for. A
// nil envelope is likewise a no-op, because a defensive panic helps nobody
// at a read loop.
func (h *Hub) Apply(env *walkiev1.Envelope) (message.Message, bool) {
	if env == nil {
		return message.Message{}, false
	}
	if sd := env.GetSealedDelivery(); sd != nil {
		return h.applySealed(env.GetPosition(), sd)
	}
	return h.applyText(env)
}

// applySealed opens one SealedDelivery payload with this device's identity
// key and folds the inner envelope in through the SAME path live text takes.
//
// Failure modes, all drop-whole and loud, never partial:
//
//   - no identity wired: every sealed frame is unreadable by construction;
//     logged once per frame at Error with the position (the ack handle) and
//     nothing else;
//   - Open fails (crypto.ErrAuthFailed or structural): the box did not
//     authenticate — tampered at rest, corrupted, or sealed to a key this
//     device no longer holds (criterion 7's forfeiture). The error carries
//     reason class only; neither ciphertext nor key material ever reaches a
//     log line;
//   - opened bytes do not decode: authenticated garbage — impossible under
//     a correct AEAD, refused anyway rather than half-trusted.
func (h *Hub) applySealed(position uint64, sd *walkiev1.SealedDelivery) (message.Message, bool) {
	if h.identity == nil {
		h.logger.Error("sealed delivery DROPPED: no identity key wired",
			slog.Uint64("position", position),
		)
		return message.Message{}, false
	}
	opened, err := crypto.Open(h.identity, sd.GetCiphertext())
	if err != nil {
		h.logger.Error("sealed delivery UNREADABLE: dropped whole (key lost or box inauthentic)",
			slog.Uint64("position", position),
			slog.String("reason", err.Error()),
		)
		return message.Message{}, false
	}
	var inner walkiev1.Envelope
	if err := proto.Unmarshal(opened, &inner); err != nil {
		h.logger.Error("sealed delivery opened but did not decode: dropped whole",
			slog.Uint64("position", position),
			slog.String("reason", err.Error()),
		)
		return message.Message{}, false
	}
	return h.applyText(&inner)
}

// applyText folds one plaintext envelope into the log — the shared path for
// live deliveries and opened sealed ones, so the two ingest paths cannot
// drift apart in what they file or how they dedup.
func (h *Hub) applyText(env *walkiev1.Envelope) (message.Message, bool) {
	var msg message.Message
	switch payload := env.GetPayload().(type) {
	case *walkiev1.Envelope_DirectMessage:
		dm := payload.DirectMessage
		msg = message.Message{
			ID:         env.GetMessageId(),
			Sender:     dm.GetSender(),
			Recipient:  dm.GetRecipient(),
			Body:       dm.GetBody(),
			SentAt:     stampOrZero(env.GetSentAt()),
			ReceivedAt: stampOrZero(env.GetReceivedAt()),
		}
	case *walkiev1.Envelope_BroadcastMessage:
		bm := payload.BroadcastMessage
		msg = message.Message{
			ID:         env.GetMessageId(),
			Sender:     bm.GetSender(),
			Body:       bm.GetBody(),
			SentAt:     stampOrZero(env.GetSentAt()),
			ReceivedAt: stampOrZero(env.GetReceivedAt()),
		}
	default:
		return message.Message{}, false
	}

	return msg, h.log.Append(msg)
}

// Conversation returns one conversation's messages in arrival order; key from
// [message.ConversationKey] or [message.BroadcastConversation]. A copy —
// render at your own pace while the read loop keeps filing.
func (h *Hub) Conversation(key string) []message.Message {
	return h.log.Conversation(key)
}

// Conversations lists known conversation keys sorted.
func (h *Hub) Conversations() []string {
	return h.log.Conversations()
}

// fileLocal runs a composed message through the SAME Append gate inbound
// traffic uses, so locally filed messages obey the same scrollback and pass
// the same dedup membership as received ones. Append's return value is
// impossible-false here (the ID was minted microseconds ago) but the gate is
// shared on principle: two ingest paths is how the display model drifts.
func (h *Hub) fileLocal(msg message.Message) {
	if !h.log.Append(msg) {
		h.logger.Warn("messagehub: locally composed message hit the dedup gate",
			slog.String("message_id", msg.ID),
		)
	}
}

// stampOrZero converts a proto timestamp, mapping nil (field absent on the
// wire) to the zero time rather than timestamppb's nil-safe Unix epoch — a
// renderer distinguishes "no stamp" from 1970 by IsZero, and the difference
// is load-bearing for locally filed messages' unstamped ReceivedAt.
func stampOrZero(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
