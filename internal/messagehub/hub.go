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
//     verdict — false means "already shown", wherever the duplicate came from.
//   - [Hub.Conversation] / [Hub.Conversations] expose the log's query surface.
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

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
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
// Only the two text payloads are this package's business; anything else
// (presence, handshake, errors) is ignored untouched — other seams
// (presenceview, the connection state machine) own those, and swallowing them
// here would hide traffic another consumer is waiting for. A nil envelope is
// likewise a no-op, because a defensive panic helps nobody at a read loop.
func (h *Hub) Apply(env *walkiev1.Envelope) (message.Message, bool) {
	if env == nil {
		return message.Message{}, false
	}

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
