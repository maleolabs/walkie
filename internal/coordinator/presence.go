package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/coordinator/presence"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// This file is sto:device-presence's wiring into the accept loop: it turns the
// tracker's read model (presence.Entry / presence.Change) into PresenceUpdate
// envelopes and fans them out to connected devices. The liveness RULES live in
// internal/coordinator/presence — nothing here decides who is online. Wiring
// only ever feeds the tracker events the server itself witnessed and forwards
// the verdicts the tracker emits.

// deviceOf names a connection's device exactly as handleHello echoes it back:
// the tailnet-resolved NodeName, falling back to LoginName for daemons that
// answered WhoIs without a node record. One rule, used everywhere a device is
// named (HelloAck, heartbeat observation, status labels, presence changes), so
// a device can never appear under two names because two call sites guessed
// differently.
func deviceOf(id tsauth.Identity) string {
	if id.NodeName != "" {
		return id.NodeName
	}
	return id.LoginName
}

// connWriter serializes every envelope written to one WebSocket connection.
//
// Why it exists: coder/websocket permits one concurrent writer per
// connection, and this server now has two would-be writers — the read loop,
// which answers handshakes and rejects malformed frames, and the per-
// connection presence pump, which broadcasts roster changes. Without the
// mutex, an interleaved write pair corrupts frames; with it, both writers
// queue and neither can starve the other, because each write is a single
// bounded frame.
type connWriter struct {
	ws     *websocket.Conn
	logger *slog.Logger

	mu sync.Mutex
}

// write marshals env and sends it as one binary frame, bounded by ctx so a
// stuck peer cannot hold shutdown past its grace window. Failures are logged
// and swallowed: a failed broadcast or reply means the connection is dying,
// and its own read loop will surface that and run the cleanup path.
func (w *connWriter) write(ctx context.Context, env *walkiev1.Envelope) {
	data, err := proto.Marshal(env)
	if err != nil {
		// Marshal failure on a structurally valid envelope is a programmer
		// error; log and let the read loop surface the dead connection.
		w.logger.Error("envelope marshal failed", slog.String("reason", err.Error()))
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ws.Write(ctx, websocket.MessageBinary, data); err != nil {
		w.logger.Warn("connection write failed", slog.String("reason", err.Error()))
	}
}

// pumpPresence forwards presence changes to ONE connected device until its
// connection ends: each Change becomes a PresenceUpdate envelope on out.
//
// # The subject-exclusion rule
//
// Changes about the receiving device ITSELF are dropped (self == ch.Device).
// req:device-presence criterion 1 promises a connecting device appears online
// "to every other connected device" — others, deliberately. A device knows
// its own liveness more directly than any broadcast can tell it: it knows
// whether its socket is up and whether it is heartbeating. Echoing its own
// state back would add nothing the device cannot observe, while handing the
// client a channel that LOOKS like the server conferring liveness — exactly
// the fact/label inversion criterion 5 forbids clients from exploiting. The
// one-shot roster snapshot sent at connect time is different: it is a read
// model, not an event, and a roster needs its own line in it.
//
// # Delivery semantics
//
// At-least-a-buffered-send, not at-least-once: the tracker drops changes for
// subscribers whose buffer is full (view.go documents why that is correct for
// self-healing presence data), and a Change is an idempotent statement of
// current state, so duplicates and gaps both converge on the next event.
// Ordering across DIFFERENT devices carries no meaning; per-device ordering
// is preserved by the single subscription channel feeding this loop.
//
// Exit paths: ctx cancelled (handler exit / shutdown abort), sub closed by
// the handler's defer, or a dead peer surfacing as a write error — after
// which CloseNow unblocks the reader so the handler's cleanup runs promptly
// instead of at the next read deadline.
func (s *Server) pumpPresence(ctx context.Context, out *connWriter, sub *presence.Subscription, self string) {
	for {
		select {
		case <-ctx.Done():
			return
		case ch, ok := <-sub.C():
			if !ok {
				return // unsubscribed: connection is being torn down
			}
			if ch.Device == self {
				continue // subject-exclusion rule above
			}
			out.write(ctx, presenceUpdateFromChange(ch))
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// presenceUpdateFromChange maps one tracker change onto the wire message.
// Field-for-field by design: presence.Change was shaped to mirror
// walkiev1.PresenceUpdate so this function stays mechanical.
//
// last_seen follows the schema's contract — set when OFFLINE (the whole point
// of last-seen is judging staleness after a device vanishes), omitted while
// ONLINE (the proto field doc: "unset while online"). A zero LastSeen on an
// offline entry means the device was known without ever being observed alive
// (a status label set before its first connect); there is no honest timestamp
// to send, so none is sent rather than a fabricated zero-time.
func presenceUpdateFromChange(ch presence.Change) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PresenceUpdate{
			PresenceUpdate: presenceUpdateFields(ch.Device, ch.Online, ch.LastSeen, ch.Status),
		},
	}
}

// presenceUpdateEnvelope renders a roster Entry as a PresenceUpdate envelope —
// the snapshot form of presenceUpdateFromChange, used for the one-shot roster
// a newly connected device receives.
func presenceUpdateEnvelope(e presence.Entry) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PresenceUpdate{
			PresenceUpdate: presenceUpdateFields(e.Device, e.Online, e.LastSeen, e.Status),
		},
	}
}

// presenceUpdateFields fills the shared field set of both mapping helpers.
func presenceUpdateFields(device string, online bool, lastSeen time.Time, status string) *walkiev1.PresenceUpdate {
	pu := &walkiev1.PresenceUpdate{
		Device: device,
		Status: status,
		State:  walkiev1.PresenceState_PRESENCE_STATE_OFFLINE,
	}
	if online {
		pu.State = walkiev1.PresenceState_PRESENCE_STATE_ONLINE
	} else if !lastSeen.IsZero() {
		pu.LastSeen = timestamppb.New(lastSeen)
	}
	return pu
}
