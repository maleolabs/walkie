package presenceview

import (
	"log/slog"
	"sort"
	"sync"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// View is the client-side roster: every device the coordinator has spoken
// about, with its last known liveness verdict, status label and last-seen
// timestamp. Safe for concurrent use — Apply comes from the control-plane
// read loop while Snapshot/Subscribe come from UI goroutines.
type View struct {
	logger *slog.Logger

	mu      sync.Mutex
	entries map[string]entry
	subs    map[*Subscription]struct{}
}

// entry is one device's rendered state. Field-for-field it mirrors
// walkiev1.PresenceUpdate so Apply stays mechanical; it exists as a named type
// so consumers never import protobuf types to draw a roster line.
type entry struct {
	device   string
	online   bool
	lastSeen int64 // unix nanos; 0 = never observed alive
	status   string
}

// New returns an empty View. A nil logger falls back to slog's default.
func New(logger *slog.Logger) *View {
	if logger == nil {
		logger = slog.Default()
	}
	return &View{
		logger:  logger,
		entries: make(map[string]entry),
		subs:    make(map[*Subscription]struct{}),
	}
}

// Device is one roster line as sto:terminal-ui will consume it.
type Device struct {
	// Device is the tailnet-resolved device name from the PresenceUpdate.
	Device string

	// Online is the coordinator's verdict, not this client's belief. See the
	// package comment for why a client never overrides it locally.
	Online bool

	// LastSeen is when the coordinator last observed the device alive; zero
	// while online (the wire omits it) or when never observed at all.
	LastSeen int64 // unix nanos

	// Status is the device's user-authored status label; empty means none.
	Status string
}

// Snapshot returns the current roster sorted by device name.
func (v *View) Snapshot() []Device {
	v.mu.Lock()
	defer v.mu.Unlock()

	out := make([]Device, 0, len(v.entries))
	for _, e := range v.entries {
		out = append(out, Device{
			Device:   e.device,
			Online:   e.online,
			LastSeen: e.lastSeen,
			Status:   e.status,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Device < out[j].Device })
	return out
}

// Apply folds one received PresenceUpdate into the view and notifies
// subscribers. Idempotent per device: re-applying an update changes nothing,
// which is what makes transport-level duplicates harmless.
//
// Per-state rules, straight from the proto field docs:
//
//   - ONLINE: last_seen is unset on the wire ("unset while online"), so any
//     previously stored last-seen is KEPT — it remains true that the device
//     was seen then, and clearing it would lose history the offline transition
//     has not replaced yet.
//   - OFFLINE: last_seen travels with the update and is stored as given;
//     an update without one leaves the previously known last-seen untouched
//     (the coordinator only omits last_seen for devices never observed,
//     which have no client-side entry yet).
//   - UNSPECIFIED (or any value this build does not know): ignored. Version
//     skew must degrade to no-op here exactly as unknown envelope payloads
//     degrade to ignore on the wire (adr:003); guessing a state from a number
//     we do not know is how rosters end up confidently wrong.
func (v *View) Apply(pu *walkiev1.PresenceUpdate) {
	if pu == nil || pu.GetDevice() == "" {
		return // nothing identifiable to apply
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	e := v.entries[pu.GetDevice()]
	e.device = pu.GetDevice()
	e.status = pu.GetStatus()

	switch pu.GetState() {
	case walkiev1.PresenceState_PRESENCE_STATE_ONLINE:
		e.online = true
	case walkiev1.PresenceState_PRESENCE_STATE_OFFLINE:
		e.online = false
		if ts := pu.GetLastSeen(); ts != nil {
			e.lastSeen = ts.AsTime().UnixNano()
		}
	default:
		// Unknown state: keep prior liveness fields, still take the label.
		// The status half of the update is user-authored text whose meaning
		// does not depend on the enum's evolution.
	}

	changed := v.entries[pu.GetDevice()] != e
	v.entries[pu.GetDevice()] = e

	if changed {
		v.notifyLocked(Device{
			Device:   e.device,
			Online:   e.online,
			LastSeen: e.lastSeen,
			Status:   e.status,
		})
	}
}

// subscriptionBuffer is how many change notifications a Subscription buffers
// before drops begin. Same policy and same justification as the tracker side:
// presence is self-healing — the next heartbeat, disconnect or expiry restates
// full current state within one TTL — so a dropped notification costs a late
// repaint, never stale-forever data.
const subscriptionBuffer = 32

// Subscribe returns a Subscription receiving a Device snapshot after each
// change. Subscribe BEFORE the first Apply of interest; anything applied
// earlier is already visible via [View.Snapshot].
func (v *View) Subscribe() *Subscription {
	s := &Subscription{ch: make(chan Device, subscriptionBuffer), view: v}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.subs[s] = struct{}{}
	return s
}

// Subscription receives roster-change notifications until Close.
type Subscription struct {
	ch        chan Device
	view      *View
	closeOnce sync.Once
}

// C receives one Device per applied change until Close.
func (s *Subscription) C() <-chan Device { return s.ch }

// Close unsubscribes. Idempotent; safe concurrently with delivery because
// removal happens under the same mutex that guards sends.
func (s *Subscription) Close() {
	s.closeOnce.Do(func() {
		v := s.view
		v.mu.Lock()
		delete(v.subs, s)
		v.mu.Unlock()
		close(s.ch)
	})
}

// notifyLocked fans one change out. Non-blocking; drops are logged with the
// device name only. Callers hold v.mu.
func (v *View) notifyLocked(d Device) {
	for s := range v.subs {
		select {
		case s.ch <- d:
		default:
			v.logger.Warn("presenceview: subscriber buffer full, notification dropped",
				slog.String("device", d.Device),
				slog.Int("buffer", subscriptionBuffer),
			)
		}
	}
}
