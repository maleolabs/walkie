// Package testnet provides an in-memory network whose behaviour tests control.
//
// Owning work item:
//
//	eka get walkie/ts:test-harness
//
// # Why
//
// The behaviour that matters most in walkie is what happens when the network
// misbehaves: reconnect with jittered backoff, queue resumption by position,
// presence expiry. Testing that against a real socket produces slow tests that
// pass or fail depending on the machine. ts:test-harness requires the opposite —
// deterministic, reproducible, and with no test sleeping in real time.
//
// Pair this with internal/clock. A [Link] on a *clock.Fake advances only when
// the test says so.
//
// # What this models, and what it does not
//
// Partition and added latency, over a stream connection. That covers the control
// plane, which is a WebSocket over TCP.
//
// It deliberately does NOT model packet loss. Loss is a datagram concept, and
// walkie's only datagram path is the real-time audio data plane — which is phase
// 2 in plan:roadmap-v1, not the MVP. Adding a loss knob to a stream link now
// would be inventing the wrong abstraction and then building on it. The loss
// injection ts:test-harness asks for belongs with the UDP data plane that will
// need it.
//
// Two simplifications worth knowing:
//
//   - A partition surfaces as [ErrPartitioned] rather than as silence followed
//     by a timeout. For the primary use case — the client losing its connection
//     and backing off — an immediate, distinguishable error is what a test wants.
//   - The underlying transport is net.Pipe, which is synchronous and unbuffered:
//     a Write blocks until someone Reads. TCP would buffer. If a test depends on
//     send-side buffering, it is depending on something this link does not have.
package testnet

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// ErrPartitioned reports that the link is currently severed.
var ErrPartitioned = errors.New("testnet: link is partitioned")

// Conditions describe what a link does to traffic passing over it.
type Conditions struct {
	// Latency is added to each write.
	//
	// On a *clock.Fake this blocks until the test advances the clock past the
	// delay. That is correct fake-clock behaviour, and it surprises people: a
	// test that sets a latency and never advances will park on the first write.
	Latency time.Duration
}

// Link is a controllable connection between two endpoints. It is safe for
// concurrent use.
type Link struct {
	clk clock.Clock

	mu          sync.RWMutex
	conditions  Conditions
	partitioned bool
}

// NewLink returns a link governed by clk and starting in the given conditions.
func NewLink(clk clock.Clock, conditions Conditions) *Link {
	return &Link{clk: clk, conditions: conditions}
}

// Pipe returns two connected endpoints subject to the link's conditions.
func (l *Link) Pipe() (net.Conn, net.Conn) {
	a, b := net.Pipe()
	return &linkConn{Conn: a, link: l}, &linkConn{Conn: b, link: l}
}

// Partition severs the link. Reads and writes fail with ErrPartitioned until
// [Link.Heal] is called.
func (l *Link) Partition() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partitioned = true
}

// Heal restores the link.
func (l *Link) Heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partitioned = false
}

// Partitioned reports whether the link is currently severed.
func (l *Link) Partitioned() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.partitioned
}

// SetLatency changes the added per-write delay.
func (l *Link) SetLatency(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conditions.Latency = d
}

func (l *Link) latency() time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.conditions.Latency
}

type linkConn struct {
	net.Conn
	link *Link
}

func (c *linkConn) Read(b []byte) (int, error) {
	if c.link.Partitioned() {
		return 0, ErrPartitioned
	}
	return c.Conn.Read(b)
}

func (c *linkConn) Write(b []byte) (int, error) {
	if c.link.Partitioned() {
		return 0, ErrPartitioned
	}
	if d := c.link.latency(); d > 0 {
		c.link.clk.Sleep(d)
	}
	// Re-check: the link may have been severed while we were waiting.
	if c.link.Partitioned() {
		return 0, ErrPartitioned
	}
	return c.Conn.Write(b)
}
