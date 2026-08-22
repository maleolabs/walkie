package testnet

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// DatagramLink is the lossy, datagram-shaped half of this package: a UDP-like
// link whose every datagram is independently delivered or dropped.
//
// # Why it exists beside the stream Link
//
// The package comment on testnet.go argues — correctly — that packet loss does
// not belong on [Link]: loss is a datagram concept, and bolting it onto a
// reliable byte stream would produce a knob that models nothing real. That
// argument is about the stream's semantics, not about whether walkie will ever
// need loss injection. It will: adr:001-transport-topology puts real-time
// audio on RTP over UDP in phase 2, and ts:test-harness criterion 1 requires
// that packet loss be injectable per test. The honest resolution is not to
// weaken the stream link but to add the transport shape loss actually belongs
// to. This file is that shape, built now so phase 2 inherits a tested seam
// instead of inventing one under deadline; the MVP control plane keeps using
// the stream Link, whose semantics are unchanged.
//
// # Semantics, chosen to match UDP rather than to be convenient
//
//   - A successful Send means only that the datagram was handed to the link,
//     never that it will arrive. Loss is drawn per datagram from the caller's
//     *rand.Rand — see NewDatagramLink for why the source is injected.
//   - Latency is paid on the link's clock, exactly as on [Link]: on a
//     *clock.Fake a Send parks until the test advances, so no test waits in
//     real time (ts:test-harness criterion 2).
//   - Each endpoint has a bounded receive queue. A datagram arriving at a full
//     queue is dropped and counted, which is what an OS socket does when the
//     application cannot keep up. Unbounded queues in a test harness hide
//     overload instead of surfacing it.
//   - A datagram in flight when the link is partitioned is dropped, not
//     delivered late: a severed network does not hold packets in trust.
//   - No ordering guarantee across concurrent senders, matching UDP. Sends
//     from a single goroutine are delivered in order, by construction.
//
// # Determinism
//
// rand.Rand is not safe for concurrent use, so draws are serialised under the
// link mutex. With a single sending goroutine the drop pattern is a pure
// function of the seed and the send sequence — replay the same seed and you
// get the same losses, which is what ts:test-harness criterion 3 asks for.
// Concurrent senders interleave their draws nondeterministically, so tests
// that assert exact loss counts must serialize their sends; that restriction
// is the price of an honest shared source and is preferable to hiding a lock
// inside a global RNG.

// DefaultQueueDepth is the receive-queue depth used when
// [DatagramConditions.QueueDepth] is zero. Large enough that ordinary tests
// never overflow by accident; small enough that an overflowing test is a bug
// in the code under test, not in the harness.
const DefaultQueueDepth = 64

// ErrClosed reports that the endpoint has been closed.
var ErrClosed = errors.New("testnet: endpoint is closed")

// DatagramConditions describes what a [DatagramLink] does to traffic.
type DatagramConditions struct {
	// Latency is added to each delivery.
	//
	// On a *clock.Fake this parks Send until the test advances the clock,
	// mirroring [Conditions.Latency] on the stream link.
	Latency time.Duration

	// Loss is the probability, in [0,1), that any one datagram is dropped.
	// Zero disables loss.
	Loss float64

	// QueueDepth is each endpoint's receive-queue depth. Zero selects
	// [DefaultQueueDepth].
	QueueDepth int
}

// DatagramStats counts what a link did to traffic. Tests assert on these to
// prove loss happened — and, with a seeded source, to prove exactly how much.
type DatagramStats struct {
	// Sent counts datagrams accepted by Send, dropped or not.
	Sent uint64
	// Dropped counts datagrams destroyed by the link: loss draws, sends
	// severed mid-flight by a partition, and arrivals at a full or closed
	// receive queue.
	Dropped uint64
	// Delivered counts datagrams handed to a receiver by Recv.
	Delivered uint64
}

// DatagramLink is a controllable lossy datagram connection between two
// endpoints. It is safe for concurrent use.
type DatagramLink struct {
	clk clock.Clock
	rng *rand.Rand

	mu          sync.Mutex
	conditions  DatagramConditions
	partitioned bool
	stats       DatagramStats
}

// NewDatagramLink returns a datagram link governed by clk, drawing its loss
// decisions from rng.
//
// The random source is a parameter, not an internal default, because
// ts:test-harness criterion 3 makes determinism a contract: a test constructs
// rand.New(rand.NewPCG(seedA, seedB)) with seeds it controls, and the same
// seeds reproduce the same losses forever. Reaching for the package-global
// functions here would make every loss test a coin flip that happens to be
// reproducible only by luck.
//
// Loss outside [0,1) panics: a silently clamped probability would make a typo
// look like a passing loss test.
func NewDatagramLink(clk clock.Clock, rng *rand.Rand, conditions DatagramConditions) *DatagramLink {
	if conditions.Loss < 0 || conditions.Loss >= 1 {
		panic("testnet: DatagramConditions.Loss must be in [0,1)")
	}
	if conditions.QueueDepth <= 0 {
		conditions.QueueDepth = DefaultQueueDepth
	}
	return &DatagramLink{clk: clk, rng: rng, conditions: conditions}
}

// Endpoints returns the two ends of the link. What either end sends arrives,
// subject to the link's conditions, at the other.
func (l *DatagramLink) Endpoints() (*DatagramEndpoint, *DatagramEndpoint) {
	a := &DatagramEndpoint{
		link:  l,
		inbox: make(chan []byte, l.queueDepth()),
		done:  make(chan struct{}),
	}
	b := &DatagramEndpoint{
		link:  l,
		inbox: make(chan []byte, l.queueDepth()),
		done:  make(chan struct{}),
	}
	a.peer, b.peer = b, a
	return a, b
}

func (l *DatagramLink) queueDepth() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conditions.QueueDepth
}

// Partition severs the link. Sends fail with ErrPartitioned and datagrams
// already in flight are dropped until [DatagramLink.Heal] is called.
func (l *DatagramLink) Partition() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partitioned = true
}

// Heal restores the link.
func (l *DatagramLink) Heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partitioned = false
}

// Partitioned reports whether the link is currently severed.
func (l *DatagramLink) Partitioned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.partitioned
}

// SetLatency changes the per-datagram delay.
func (l *DatagramLink) SetLatency(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conditions.Latency = d
}

// SetLoss changes the drop probability. Panics outside [0,1), for the same
// reason NewDatagramLink does.
func (l *DatagramLink) SetLoss(p float64) {
	if p < 0 || p >= 1 {
		panic("testnet: Loss must be in [0,1)")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conditions.Loss = p
}

// Stats reports the counters. The snapshot is taken atomically with respect
// to concurrent sends and receives.
func (l *DatagramLink) Stats() DatagramStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stats
}

// DatagramEndpoint is one end of a [DatagramLink].
type DatagramEndpoint struct {
	link  *DatagramLink
	peer  *DatagramEndpoint
	inbox chan []byte

	// mu guards closed, which Send reads to make arrivals at a closed
	// endpoint deterministically countable as dropped rather than racing a
	// select between the queue and the done channel.
	mu     sync.Mutex
	closed bool

	closeOnce sync.Once
	done      chan struct{}
}

// Send hands one datagram to the link. It blocks for the configured latency
// on the link's clock, then returns nil whether or not the datagram survives
// — UDP send success says nothing about delivery, and a test that wants to
// know what arrived asserts on Recv or [DatagramLink.Stats], not on Send.
//
// The payload is copied before the latency wait, so callers may reuse their
// buffer immediately.
func (e *DatagramEndpoint) Send(p []byte) error {
	l := e.link

	dg := make([]byte, len(p))
	copy(dg, p)

	l.mu.Lock()
	switch {
	case l.partitioned:
		l.mu.Unlock()
		return ErrPartitioned
	default:
	}
	latency, loss := l.conditions.Latency, l.conditions.Loss
	l.stats.Sent++
	// Serialised with everything else under l.mu because rand.Rand is not
	// safe for concurrent use; see the determinism note above.
	if l.rng.Float64() < loss {
		l.stats.Dropped++
		l.mu.Unlock()
		return nil
	}
	l.mu.Unlock()

	if latency > 0 {
		l.clk.Sleep(latency)
	}

	// Re-check after the wait: the link may have been severed mid-flight, and
	// a partitioned network drops what it was carrying rather than delivering
	// it late.
	l.mu.Lock()
	partitioned := l.partitioned
	if partitioned {
		l.stats.Dropped++
	}
	l.mu.Unlock()
	if partitioned {
		return nil
	}

	// A closed endpoint will never read again, so its arrivals are destroyed
	// rather than queued. Checked before the push so that a Send issued after
	// Close returns is deterministically counted as dropped; an arrival that
	// races the Close itself may still land in the queue, where Recv's
	// drain-priority keeps it deliverable.
	e.peer.mu.Lock()
	closed := e.peer.closed
	e.peer.mu.Unlock()
	if closed {
		l.mu.Lock()
		l.stats.Dropped++
		l.mu.Unlock()
		return nil
	}

	select {
	case e.peer.inbox <- dg:
	default:
		// Queue full: the datagram is destroyed, exactly like an OS socket
		// overrun. Blocking here instead would silently turn UDP into TCP
		// backpressure and hide overload from the test.
		l.mu.Lock()
		l.stats.Dropped++
		l.mu.Unlock()
	}
	return nil
}

// Recv receives one datagram, blocking until one arrives.
//
// After Close, datagrams already queued remain deliverable by pending Recv
// calls; once the queue is observed empty, Recv reports ErrClosed forever.
func (e *DatagramEndpoint) Recv() ([]byte, error) {
	// Drain-priority: prefer queued traffic over reporting closure, so Close
	// never eats datagrams that already arrived.
	select {
	case dg := <-e.inbox:
		e.link.countDelivered()
		return dg, nil
	default:
	}
	select {
	case dg := <-e.inbox:
		e.link.countDelivered()
		return dg, nil
	case <-e.done:
		return nil, ErrClosed
	}
}

// Close closes this endpoint. Pending and future Recv calls return ErrClosed
// once the queue is drained; datagrams arriving afterwards are counted as
// dropped by [DatagramLink.Stats].
func (e *DatagramEndpoint) Close() error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.closeOnce.Do(func() { close(e.done) })
	return nil
}

func (l *DatagramLink) countDelivered() {
	l.mu.Lock()
	l.stats.Delivered++
	l.mu.Unlock()
}
