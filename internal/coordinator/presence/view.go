package presence

import (
	"log/slog"
	"sort"
	"sync"
	"time"
)

// This file is the VIEW half of the tracker: the read model other components
// consume. Slice 2's broadcast hub (sto:device-presence wiring into the
// coordinator's accept loop) is the intended customer: it takes one
// [Tracker.Subscribe], sends a [Snapshot] to each newly connected client, and
// forwards every [Change] as a PresenceUpdate envelope.
//
// The types here carry both halves of a device's state — Online/LastSeen are
// server-derived FACTS, Status is a user-authored LABEL — because a roster
// line renders from both. The fields stay named and commented so nothing in
// the view can be mistaken for the other kind.

// subscriptionBuffer is how many changes a Subscription buffers before the
// tracker starts dropping. See Subscribe for the drop policy and why it is
// correct for this data.
const subscriptionBuffer = 32

// Entry is one device's rendered roster state at a point in time.
type Entry struct {
	// Device is the tailnet-resolved device name (tsauth.Identity.NodeName,
	// or LoginName when no node record answered).
	Device string

	// Online is the coordinator's liveness verdict — a FACT derived from
	// observed heartbeats, connection lifecycle and TTL expiry. No client
	// input has ever set it (criterion 5).
	Online bool

	// LastSeen is when the device was last observed alive, on the
	// coordinator's clock — a FACT. For an offline device this is what the
	// roster shows so users can judge staleness themselves; it is zero only
	// for a device known without any observation yet (e.g. status set before
	// its first heartbeat).
	LastSeen time.Time

	// Status is the device's custom status message — a LABEL the user wrote.
	// Empty means none is set. It says nothing about liveness.
	Status string
}

// Change is one transition, delivered to subscribers. Its fields mirror the
// PresenceUpdate proto message field-for-field (device, state, status,
// last_seen), so slice 2's mapping is mechanical.
//
// A Change is an idempotent statement of current state, not a delta: two
// Changes for the same device always converge on the same roster line,
// whichever arrives last. That property is what licenses the drop policy on
// Subscribe.
type Change struct {
	Device   string
	Online   bool      // FACT
	LastSeen time.Time // FACT
	Status   string    // LABEL
}

// Snapshot returns the current view of every known device, sorted by name.
// See Tracker.Snapshot.
func (t *Tracker) Snapshot() []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]Entry, 0, len(t.devices))
	for _, d := range t.devices {
		out = append(out, Entry{
			Device:   d.device,
			Online:   d.fact.online,
			LastSeen: d.fact.lastSeen,
			Status:   d.label.text,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Device < out[j].Device })
	return out
}

// Known reports whether the tracker has any record of device: a connection
// observed, a heartbeat seen, or a liveness row loaded from a previous
// process. It says nothing about being online — use Snapshot for that.
//
// Why sto:text-messaging needs it: routing an unroutable DirectMessage must
// distinguish "device this tailnet has never resolved" (a typo — answered
// with ProtocolError DEVICE_UNKNOWN, per the schema's field doc) from
// "known device that is simply not connected right now" (an ordinary state,
// handed to sto:offline-queue's extension point, never an error). Only the
// tracker knows the first fact; nothing else in the server records it.
func (t *Tracker) Known(device string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.devices[device]
	return ok
}

// Subscribe returns a Subscription receiving every subsequent change.
//
// Ordering with Snapshot: subscribe FIRST, then take the Snapshot. A change
// landing between the two calls then appears in both the channel and the
// snapshot — harmless duplication, because Changes are idempotent statements
// of current state. The reverse order could MISS a change outright.
//
// # Drop policy — deliberate
//
// Delivery is non-blocking: a subscriber whose buffer is full has changes
// DROPPED (logged loudly), never blocked and never grown without bound.
// That is correct for this data and would be wrong for messages: presence is
// a continuously refreshed view whose events are self-healing — the next
// heartbeat refresh, disconnect or TTL expiry re-states the device's full
// current state within one TTL. req:offline-delivery's at-least-once rule
// governs MESSAGE delivery, which this seam deliberately does not carry;
// slow-consumer backpressure belongs to slice 2's per-client fan-out, not to
// the tracker's single mutex-guarded loop.
func (t *Tracker) Subscribe() *Subscription {
	s := &Subscription{
		ch:      make(chan Change, subscriptionBuffer),
		tracker: t,
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.checkClosed() {
		close(s.ch)
		return s
	}
	t.subs[s] = struct{}{}
	return s
}

// Subscription receives presence changes until Close. C is a buffered
// channel; see Subscribe for the delivery contract.
type Subscription struct {
	ch      chan Change
	tracker *Tracker

	closeOnce sync.Once
	done      chan struct{}
}

// C receives changes until Close.
func (s *Subscription) C() <-chan Change { return s.ch }

// Close unsubscribes. Idempotent; safe concurrently with change delivery —
// removal happens under the tracker mutex that also guards sends, so no
// send can target a closed channel.
func (s *Subscription) Close() {
	s.closeOnce.Do(func() {
		t := s.tracker
		t.mu.Lock()
		delete(t.subs, s)
		t.mu.Unlock()
		close(s.ch)
	})
}

// notifyLocked fans one change out to every subscriber. Callers hold t.mu,
// which makes send-vs-Close races impossible: a subscription removed by
// Close is never in the map a later notify iterates.
//
// Drops are counted and logged with the device NAME only — payload-free,
// per the no-message-content logging rule that holds from the first line.
func (t *Tracker) notifyLocked(ch Change) {
	for s := range t.subs {
		select {
		case s.ch <- ch:
		default:
			t.logger.Warn("presence: subscriber buffer full, change dropped",
				slog.String("device", ch.Device),
				slog.Int("buffer", subscriptionBuffer),
			)
		}
	}
}
