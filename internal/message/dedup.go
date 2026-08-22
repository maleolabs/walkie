package message

import (
	"fmt"
	"sync"
)

// DefaultDedupWindow is the dedup window a [Log] uses when none is given.
//
// Why this number: it must comfortably exceed any plausible REDLIVERY burst —
// transport retries and reconnect replays re-deliver messages sent seconds to
// minutes earlier, interleaved with at most a handful of newer ones. 4096
// distinct recent IDs covers days of human-scale chat (arc:system-overview's
// under-20-device posture) while costing a few hundred kilobytes. It is a
// live-traffic window, NOT durable retention: sto:offline-queue's replay path
// carries its own position-based resumption and must not lean on this window
// reaching back days.
const DefaultDedupWindow = 4096

// Dedup is the receiver-side duplicate filter: it remembers recently seen
// message IDs and reports whether an ID is being seen for the first time.
//
// Owning work item:
//
//	eka get walkie/sto:text-messaging   (criterion 3)
//
// # Why dedup lives here and not on the wire
//
// req:text-messaging specifies at-least-once delivery with receiver-side
// deduplication; adr:003-wire-protocol makes the client-generated ULID the
// mechanism. Exactly-once is explicitly NOT attempted at the transport layer —
// you cannot get it there without distributed agreement, and you do not need
// it: making a duplicate harmless is cheaper and survives every failure mode
// (reconnect replay, coordinator retry, future queue drain) unchanged.
//
// # The window is bounded, and what that costs
//
// Retention here is bounded by design — unbounded growth in a long-lived
// process is a leak with extra steps. The cost of bounding is honest and
// stated: once capacity distinct IDs have been seen, the OLDEST is evicted,
// and a duplicate arriving after its ID was evicted will be displayed again.
// At-least-once duplicates arrive near their originals in practice, so the
// window only has to outlive redelivery latency, not history. Anything needing
// longer-lived identity semantics (queue positions, history storage) builds on
// its own durable state, not on this filter.
type Dedup struct {
	mu sync.Mutex

	capacity int

	// seen is the membership set; order is the insertion-order ring that
	// decides eviction. They are always in lockstep: len(order) == len(seen)
	// <= capacity, and every element of order is a key of seen.
	seen  map[string]struct{}
	order []string
	head  int // next slot to overwrite once the ring is full
}

// NewDedup returns a Dedup remembering the most recent capacity distinct IDs.
//
// Panics on capacity <= 0, following the house rule for programmer errors at
// construction (presence.NewTracker refuses a non-positive TTL the same way):
// a zero-capacity dedup would silently disable criterion 3, which is worse
// than a loud crash at wiring time.
func NewDedup(capacity int) *Dedup {
	if capacity <= 0 {
		panic(fmt.Sprintf("message: dedup capacity must be positive, got %d", capacity))
	}
	return &Dedup{
		capacity: capacity,
		seen:     make(map[string]struct{}, capacity),
	}
}

// First reports whether id has NOT been seen before, recording it either way.
// This is the display decision's input: true means "new, display it", false
// means "duplicate, discard it" — criterion 3's one displayed message.
func (d *Dedup) First(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, dup := d.seen[id]; dup {
		return false
	}
	if len(d.order) == d.capacity {
		// Evict the oldest ID exactly when its slot is reused. delete before
		// insert keeps seen and order consistent even if id equals the
		// evicted key — impossible today (it was checked above) but cheap to
		// keep true by construction rather than by argument.
		delete(d.seen, d.order[d.head])
		d.order[d.head] = id
		d.head = (d.head + 1) % d.capacity
	} else {
		d.order = append(d.order, id)
	}
	d.seen[id] = struct{}{}
	return true
}

// Len reports how many IDs are currently retained. Read-only; tests use it to
// assert the bound actually holds instead of trusting the eviction code.
func (d *Dedup) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
