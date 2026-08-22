package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
)

// This file is sto:text-messaging's routing half: what the coordinator does
// with a DirectMessage or BroadcastMessage after dispatch decodes it. The
// rules it implements come straight from req:text-messaging and adr:003:
//
//   - A direct message reaches exactly its named recipient's live connection;
//     a broadcast reaches every OTHER connected device, never the sender.
//   - Sender identity comes from the connection's resolved tailnet identity
//     (adr:004's WhoIs gate), never from any payload field — there is no
//     sender field on the wire to distrust, and none may be added client-side.
//   - received_at is stamped ONCE here, at ingress, from the injected clock,
//     and never rewritten on any later path (the schema pins this; queue
//     replay must still say when the coordinator FIRST received a message).
//   - Envelope.position stays zero on everything routed today: zero is the
//     schema's marker for live traffic, distinct from future queue replays.
//   - Logs carry device names, byte lengths and reason classes only — never
//     message bodies (the no-content rule holds from the first line).
//
// What is deliberately NOT here: queueing. An offline recipient is handed to
// [OfflineSink] — an extension point, not an implementation — because bounded
// retention with TTL, size cap and explicit refusal is sto:offline-queue's
// contract, and half-building it here would produce an unbounded queue by
// accident.

// OfflineSink receives an envelope whose recipient has no live connection.
//
// EXTENSION POINT, deliberately minimal. sto:offline-queue will provide the
// production implementation: bounded retention (TTL AND size cap, per
// req:offline-delivery), explicit QueueRefused on cap, position-stamped
// replay. Until that item lands, the server runs with a nil sink and DROPS
// with a loud log line rather than pretending to retain — a silent fake queue
// would make senders believe delivery is pending when nothing is.
//
// Deliver receives the already-stamped envelope verbatim, so a future replay
// path cannot restamp received_at even by accident: the stamp was applied at
// ingress, before this seam ever saw the message.
type OfflineSink interface {
	Deliver(recipient string, env *walkiev1.Envelope)
}

// addRoute registers one live connection under its resolved device name.
// Called from serveConn once presence setup is done — see the registration
// comment there for why teardown order makes this placement load-bearing.
func (s *Server) addRoute(device string, w *connWriter) {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	if s.routes[device] == nil {
		s.routes[device] = make(map[*connWriter]struct{})
	}
	s.routes[device][w] = struct{}{}
}

// removeRoute unregisters one connection. Deferred by serveConn so every exit
// path — clean close, read error, version refusal, shutdown abort, panic —
// stops routing to a writer that will never flush.
func (s *Server) removeRoute(device string, w *connWriter) {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	set := s.routes[device]
	if set == nil {
		return
	}
	delete(set, w)
	if len(set) == 0 {
		delete(s.routes, device)
	}
}

// routeTargets snapshots the live connections of one device. More than one
// can exist across a reconnect overlap (the presence tracker refcounts the
// same situation); delivering to ALL of them is correct under at-least-once —
// the receiver dedups by ULID — whereas picking "the newest" would need
// ordering machinery no one can define honestly.
func (s *Server) routeTargets(device string) []*connWriter {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	out := make([]*connWriter, 0, len(s.routes[device]))
	for w := range s.routes[device] {
		out = append(out, w)
	}
	return out
}

// routeTargetsExcept snapshots every connected device's connections EXCLUDING
// exclude — broadcast's audience. Iteration order is map order, deliberately
// unspecified: delivery order across DIFFERENT devices carries no meaning
// (same stance as the presence pump), and encoding one would manufacture the
// global-order illusion the message package warns against.
func (s *Server) routeTargetsExcept(exclude string) []*connWriter {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()
	var out []*connWriter
	for device, set := range s.routes {
		if device == exclude {
			continue
		}
		for w := range set {
			out = append(out, w)
		}
	}
	return out
}

// handleDirect routes one DirectMessage to its named recipient.
//
// Refusals are structured ProtocolErrors on the SENDER's connection and never
// close it: a typo'd recipient or oversized draft is a correctable mistake
// (same posture as handleStatusChange). Successful routing answers with
// NOTHING on the sender's connection — there is no ack, because an ack is a
// read receipt, and req:text-messaging's scope has none.
func (s *Server) handleDirect(ctx context.Context, out *connWriter, id tsauth.Identity, env *walkiev1.Envelope, dm *walkiev1.DirectMessage) bool {
	sender := deviceOf(id)

	if dm.GetRecipient() == "" {
		s.logger.Warn("direct message rejected",
			slog.String("sender", sender),
			slog.Int("body_bytes", len(dm.GetBody())),
			slog.String("reason", "empty recipient"),
		)
		s.rejectMalformed(ctx, out, "direct_message.recipient must not be empty")
		return true
	}
	if err := message.ValidateBody(dm.GetBody()); err != nil {
		// Receiver-enforced bound: reject, never truncate (the schema's body
		// field doc; a truncated message reads as delivered-but-wrong).
		s.logger.Warn("direct message rejected",
			slog.String("sender", sender),
			slog.String("recipient", dm.GetRecipient()),
			slog.Int("body_bytes", len(dm.GetBody())),
			slog.String("reason", err.Error()),
		)
		s.rejectMalformed(ctx, out, "direct_message.body rejected: "+err.Error())
		return true
	}

	stamped := stampedDelivery(s.clk.Now(), env, &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: dm},
	})

	targets := s.routeTargets(dm.GetRecipient())
	if len(targets) > 0 {
		for _, w := range targets {
			w.write(ctx, stamped)
		}
		s.logger.Info("direct message routed",
			slog.String("sender", sender),
			slog.String("recipient", dm.GetRecipient()),
			slog.Int("body_bytes", len(dm.GetBody())),
			slog.Int("connections", len(targets)),
		)
		return true
	}

	s.deliverUnroutable(ctx, out, sender, dm.GetRecipient(), stamped, len(dm.GetBody()))
	return true
}

// handleBroadcast fans one BroadcastMessage out to every other connected
// device. The sender is excluded by req:text-messaging criterion 2 ("every
// other online device") and by the same subject-exclusion logic the presence
// pump applies: a device knows what it itself said better than any echo can
// tell it.
//
// Broadcasting into an empty room (no other connected device) succeeds
// silently with a zero-count log: it is a normal state of a small tailnet,
// not an error, and inventing a refusal here would just teach clients to
// retry broadcasts nobody asked them to retry.
func (s *Server) handleBroadcast(ctx context.Context, out *connWriter, id tsauth.Identity, env *walkiev1.Envelope, bm *walkiev1.BroadcastMessage) bool {
	sender := deviceOf(id)

	if err := message.ValidateBody(bm.GetBody()); err != nil {
		s.logger.Warn("broadcast rejected",
			slog.String("sender", sender),
			slog.Int("body_bytes", len(bm.GetBody())),
			slog.String("reason", err.Error()),
		)
		s.rejectMalformed(ctx, out, "broadcast_message.body rejected: "+err.Error())
		return true
	}

	stamped := stampedDelivery(s.clk.Now(), env, &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_BroadcastMessage{BroadcastMessage: bm},
	})

	targets := s.routeTargetsExcept(sender)
	for _, w := range targets {
		w.write(ctx, stamped)
	}
	s.logger.Info("broadcast fanned out",
		slog.String("sender", sender),
		slog.Int("recipients", len(targets)),
		slog.Int("body_bytes", len(bm.GetBody())),
	)
	return true
}

// deliverUnroutable handles a direct message whose recipient has no live
// connection. Three cases, in priority order:
//
//   - Unknown device (tracker wired, name never seen): ProtocolError
//     DEVICE_UNKNOWN back to the sender — the schema defines this code for
//     exactly the typo case, and refusing fast beats silence.
//   - Known-but-offline, sink wired: hand the STAMPED envelope to the sink.
//     This is sto:offline-queue's extension point being exercised; the sink
//     decides retention, this code decides nothing about it.
//   - Known-but-offline, no sink (today's default): drop with a loud warn.
//     Honest absence of a queue, logged content-free; NOT a silent fake.
//
// With no tracker wired (skeleton tests), unknown-vs-offline cannot be
// distinguished, so every unroutable message takes the sink-or-drop path:
// claiming DEVICE_UNKNOWN without evidence would lie to a possibly-real peer.
func (s *Server) deliverUnroutable(ctx context.Context, out *connWriter, sender, recipient string, stamped *walkiev1.Envelope, bodyBytes int) {
	if s.presence != nil && !s.presence.Known(recipient) {
		s.logger.Info("direct message refused: recipient unknown",
			slog.String("sender", sender),
			slog.String("recipient", recipient),
			slog.Int("body_bytes", bodyBytes),
		)
		out.write(ctx, &walkiev1.Envelope{
			Payload: &walkiev1.Envelope_ProtocolError{
				ProtocolError: &walkiev1.ProtocolError{
					Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_DEVICE_UNKNOWN,
					Detail: fmt.Sprintf("no device named %q is known to this coordinator", recipient),
				},
			},
		})
		return
	}

	if s.offline != nil {
		s.offline.Deliver(recipient, stamped)
		s.logger.Info("direct message held: recipient offline, handed to offline sink",
			slog.String("sender", sender),
			slog.String("recipient", recipient),
			slog.Int("body_bytes", bodyBytes),
		)
		return
	}

	// Today's honest default: no queue exists yet. The drop is loud, typed
	// (reason class, not prose) and names the owning item so the next reader
	// knows this is a seam awaiting wiring, not a bug.
	s.logger.Warn("direct message dropped: recipient offline, no queue wired",
		slog.String("sender", sender),
		slog.String("recipient", recipient),
		slog.Int("body_bytes", bodyBytes),
		slog.String("reason", "sto:offline-queue not wired"),
	)
}

// rejectMalformed answers the sender with a structured MALFORMED ProtocolError.
// The connection stays open: a rejected frame is a correctable mistake, not a
// broken peer (handleStatusChange sets the precedent).
func (s *Server) rejectMalformed(ctx context.Context, out *connWriter, detail string) {
	out.write(ctx, &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_ProtocolError{
			ProtocolError: &walkiev1.ProtocolError{
				Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
				Detail: detail,
			},
		},
	})
}

// stampedDelivery completes an envelope headed for recipients: the sender's
// message_id and sent_at carried over UNCHANGED (receiver-side dedup keys on
// the ULID surviving transit; skew legibility needs the original sent_at),
// received_at stamped ONCE from the coordinator clock at this ingress, and
// position left zero — the schema's live-traffic marker, distinct from the
// position-stamped envelopes sto:offline-queue will replay later. A replay
// path must reuse the already-stamped envelope, never call this again.
//
// routed arrives with its payload set and nothing else; this function is the
// only place envelope-level delivery fields are decided.
func stampedDelivery(receivedAt time.Time, original, routed *walkiev1.Envelope) *walkiev1.Envelope {
	routed.MessageId = original.GetMessageId()
	routed.SentAt = original.GetSentAt()
	routed.ReceivedAt = timestamppb.New(receivedAt)
	routed.Position = 0
	return routed
}
