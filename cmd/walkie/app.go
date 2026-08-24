package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/history"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/messagehub"
	"github.com/maleolabs/walkie/internal/outbox"
	"github.com/maleolabs/walkie/internal/presenceview"
	"github.com/maleolabs/walkie/internal/tui"
)

// app is the assembled client: the piece that turns the proven seams —
// control.Client (ts:reconnect-resume), messagehub.Hub (sto:text-messaging),
// presenceview.View (sto:device-presence), outbox.Outbox (sto:offline-queue),
// history.Store (sto:message-history) and the crypto identity
// (ts:queue-sealed-box) — into one running program. The wiring here closes the
// duties those packages' comments record for exactly this assembly:
//
//   - the hub gets this device's X25519 identity key at wiring time
//     (UseIdentity), so sealed queue deliveries OPEN on arrival;
//   - a SealedDelivery frame's OUTER position is acked REGARDLESS of Apply's
//     verdict — displayed and forfeited are both terminal, and withholding the
//     ack would freeze the queue behind an unreadable frame;
//   - the public key is announced on reaching online, because peers can only
//     seal queued mail to a PINNED key; announcing on EVERY online transition
//     (not just the first) keeps the plaintext-at-rest bootstrap window one
//     connection wide even across a coordinator that lost its pin store;
//   - the outbox drains on every online transition, head-of-line blocking on
//     the first failure (per-conversation order is the only ordering there is);
//   - every displayed message lands in ~/.walkie/history.db — the SAME file
//     `walkie history` queries — so the live view and the query surface read
//     one store.
//
// Everything below consumes exported seams only; nothing reaches past them
// into coordinator internals.
type app struct {
	clk    clock.Clock
	logger *slog.Logger
	client controlClient
	box    *outbox.Outbox
	hist   *history.Store
	key    *crypto.IdentityKey

	// presence is the roster seam (sto:device-presence): fed by OnEnvelope,
	// rendered by the UI, subscribed to by nobody here — the UI takes its own
	// subscription so change delivery has exactly one consumer per stream.
	presence *presenceview.View

	// inbound carries newly filed messages to the TUI. Buffered + drop-loud:
	// the OnEnvelope handler runs on the control-plane read loop and MUST NOT
	// block (control.Client.OnEnvelope's rule). A dropped notification costs
	// one late repaint — the pane re-reads the hub snapshot on the next event,
	// and the message itself is safe in the hub log and the history store.
	inbound chan message.Message

	// hub is built lazily on the first post-handshake envelope: its local
	// name is the HelloAck echo (learned, never asserted), which does not
	// exist until the supervisor completed a handshake. Guarded by mu;
	// construction happens exactly once.
	mu  sync.Mutex
	hub *messagehub.Hub

	draining atomic.Bool // one drain at a time; overlapping online transitions collapse
}

// sender is the slice of control.Client the duties need, so tests can drive
// the drain and announce paths without a transport.
type sender interface {
	Send(env *walkiev1.Envelope) error
}

// acker is the queue-ack half of the same seam.
type acker interface {
	Acknowledge(position uint64)
}

// controlClient is everything the assembled duties need from the connection.
// *control.Client satisfies it; the interface exists so tests can drive the
// duties with a fake instead of a transport, exactly like control.Session
// does for the supervisor itself.
type controlClient interface {
	sender
	acker
	ConnectedAs() string
}

var (
	_ sender        = (*control.Client)(nil)
	_ acker         = (*control.Client)(nil)
	_ controlClient = (*control.Client)(nil)
)

func newApp(clk clock.Clock, logger *slog.Logger, client controlClient, box *outbox.Outbox, hist *history.Store, key *crypto.IdentityKey, presence *presenceview.View) *app {
	return &app{
		clk:      clk,
		logger:   logger,
		client:   client,
		box:      box,
		hist:     hist,
		key:      key,
		presence: presence,
		inbound:  make(chan message.Message, 64),
	}
}

// OnEnvelope is the control.Client inbound handler. Sealed deliveries are
// acked here REGARDLESS of the display verdict — see the type comment.
func (a *app) OnEnvelope(env *walkiev1.Envelope) {
	// Presence belongs to the roster seam, not the messaging seam: hub.Apply
	// deliberately ignores non-text payloads, so route it here first.
	if pu := env.GetPresenceUpdate(); pu != nil {
		a.presence.Apply(pu)
		return
	}

	h := a.hubRef()
	if h == nil {
		// Unreachable through the supervisor (envelopes flow only after the
		// handshake that publishes the identity), but a silent drop here would
		// be indistinguishable from quiet packet loss, so say it loudly.
		a.logger.Error("envelope arrived before identity known; dropped",
			slog.String("message_id", env.GetMessageId()),
		)
		return
	}

	msg, shown := h.Apply(env)

	if env.GetSealedDelivery() != nil {
		// Ack discipline from the messagehub contract: the OUTER position is
		// terminal whichever way Apply ruled. Withholding it would wedge the
		// high-water mark behind an unreadable frame and force eternal
		// redelivery of everything after it.
		a.client.Acknowledge(env.GetPosition())
	}

	if !shown {
		return // duplicate, non-text payload, or forfeited box: nothing to show
	}
	if err := a.hist.Append(a.client.ConnectedAs(), msg); err != nil {
		// Display survives a history failure; the loss is logged, not hidden.
		a.logger.Error("history append failed",
			slog.String("message_id", msg.ID),
			slog.String("reason", err.Error()),
		)
	}
	a.notify(msg)
}

// notify hands one newly filed message to the TUI, dropping loudly if the UI
// has fallen behind (see the inbound field comment).
func (a *app) notify(msg message.Message) {
	select {
	case a.inbound <- msg:
	default:
		a.logger.Warn("ui notification dropped; pane catches up on next event",
			slog.String("message_id", msg.ID),
		)
	}
}

// hubRef returns the hub once it exists, building it on first call after the
// identity is known. The name comes from ConnectedAs — the coordinator's
// HelloAck echo — because conversation orientation (whose DM is whose) hangs
// on it being the resolved truth rather than a guess.
func (a *app) hubRef() *messagehub.Hub {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hub == nil {
		local := a.client.ConnectedAs()
		if local == "" {
			return nil
		}
		h := messagehub.New(local, a.clk, a.logger)
		// ts:queue-sealed-box wiring duty: without this, every sealed queue
		// delivery is dropped loudly on arrival instead of opened. A nil key
		// here panics by the seam's own contract — "forgot crypto" must fail
		// at wiring, not surface as vanishing mail.
		h.UseIdentity(a.key)
		a.hub = h
	}
	return a.hub
}

// hubSource exposes the hub to the UI through the read-only surface the pane
// needs. It returns an explicit nil until the identity is known — a typed-nil
// *messagehub.Hub wrapped in the interface would look non-nil to the caller
// and panic on first use, which is exactly the kind of lie this seam refuses.
func (a *app) hubSource() tui.ConversationSource {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hub == nil {
		return nil
	}
	return a.hub
}

// WatchConn consumes one Machine subscription and performs the per-online
// duties. It takes its OWN subscription — the TUI gets another — because a
// Machine subscription is single-consumer by shape.
func (a *app) WatchConn(sub *control.Subscription) {
	for change := range sub.C() {
		if change.To != control.StateOnline {
			continue
		}
		a.announceKey()
		go a.drainOutbox()
	}
}

// announceKey publishes this device's X25519 public key. Idempotent
// server-side (a re-announce of the pinned key is a no-op), so announcing on
// every online transition is cheap insurance: it keeps the bootstrap window —
// messages resting PLAINTEXT for a device that has never announced — as short
// as one connection, including after a coordinator that restarted with an
// empty pin store.
func (a *app) announceKey() {
	pub := a.key.PublicKey()
	env := &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PublicKeyAnnounce{PublicKeyAnnounce: &walkiev1.PublicKeyAnnounce{
			PublicKey: pub[:],
		}},
	}
	if err := a.client.Send(env); err != nil && !errors.Is(err, control.ErrOffline) {
		a.logger.Warn("public key announce failed",
			slog.String("reason", err.Error()),
		)
	}
}

// drainOutbox transmits held messages in composition order. Head-of-line
// blocking on the first failure is the outbox's documented contract: skipping
// a stuck message would deliver its successors first, an order no receiver can
// reconstruct. Removal happens ONLY after the transport accepted the frame;
// anything else keeps the entry queued (at-least-once makes that safe).
func (a *app) drainOutbox() {
	if !a.draining.CompareAndSwap(false, true) {
		return // a drain is already running; it sees the same pending set
	}
	defer a.draining.Store(false)

	pending, err := a.box.Pending()
	if err != nil {
		a.logger.Error("outbox drain read failed", slog.String("reason", err.Error()))
		return
	}
	for _, env := range pending {
		if err := a.client.Send(env); err != nil {
			a.logger.Warn("outbox drain stopped; message stays queued",
				slog.String("message_id", env.GetMessageId()),
				slog.String("reason", err.Error()),
			)
			return
		}
		if err := a.box.Remove(env.GetMessageId()); err != nil {
			a.logger.Error("outbox removal failed; stopping drain before duplicating further sends",
				slog.String("message_id", env.GetMessageId()),
				slog.String("reason", err.Error()),
			)
			return
		}
	}
}

// SendFromUI transmits one composed message from the terminal interface. The
// conversation key is the TUI's currency ("broadcast" or "dm:<peer>").
//
// Failure policy: the message is ALREADY filed for display and history by the
// time transmission is attempted, and ANY transmit failure holds the envelope
// in the outbox for the next online transition — including a write error on a
// live connection, where the frame may or may not have reached the
// coordinator. Holding it either way is correct under at-least-once: the
// receiver dedups by ULID, so the worst case is one suppressed duplicate,
// while the alternative (dropping on uncertainty) is silent loss.
func (a *app) SendFromUI(conversation, body string) error {
	h := a.hubRef()
	if h == nil {
		// Cold start before the FIRST-ever handshake: there is no identity to
		// file under yet. Loud refusal beats silently misfiling the message;
		// every later outage composes offline normally (the hub exists by
		// then, and the outbox holds until reconnect).
		return errors.New("not connected yet - wait for [online], then send again")
	}

	var (
		env *walkiev1.Envelope
		err error
	)
	switch conversation {
	case message.BroadcastConversation:
		env, err = h.SendBroadcast(body)
	default:
		env, err = h.SendDirect(strings.TrimPrefix(conversation, "dm:"), body)
	}
	if err != nil {
		return err // validation refused BEFORE any wire byte existed
	}

	local := a.client.ConnectedAs()
	filed := message.Message{
		ID:        env.GetMessageId(),
		Sender:    local,
		Recipient: env.GetDirectMessage().GetRecipient(),
		Body:      body,
		SentAt:    env.GetSentAt().AsTime(),
		// ReceivedAt deliberately zero: no ack exists to carry it back, and
		// the renderer shows that absence as "-" rather than faking a stamp.
	}
	if err := a.hist.Append(local, filed); err != nil {
		a.logger.Error("history append failed for composed message",
			slog.String("message_id", filed.ID),
			slog.String("reason", err.Error()),
		)
	}
	a.notify(filed) // own side of the conversation appears immediately

	if err := a.client.Send(env); err != nil {
		if qerr := a.box.Enqueue(env); qerr != nil {
			return fmt.Errorf("send failed (%s) AND holding failed (%s)", err, qerr)
		}
		return errors.New("offline - message held, sends automatically on reconnect")
	}
	return nil
}

// backoffSeeds draws the two random seeds control.NewClient needs for its
// full-jitter backoff, from crypto/rand — the same source class the identity
// key uses, because predictable retry timing is a (small) herd-correlation
// defect the jitter exists to prevent.
func backoffSeeds() (uint64, uint64) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return binary.BigEndian.Uint64(buf[:8]), binary.BigEndian.Uint64(buf[8:])
}
