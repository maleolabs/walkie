package ctlsocket

import (
	"bufio"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// The policy under test, stated where the brief asks it to be explicit:
// BOUNDED PER-SUBSCRIBER QUEUE THAT DROPS AND REPORTS A GAP. These tests pin
// its three load-bearing properties:
//
//  1. the producer never blocks, however far behind a reader falls;
//  2. per-subscriber memory is capped (queue bound, observable);
//  3. the loss is REPORTED — a gap event precedes the next delivered batch,
//     so no reader ever mistakes a truncated stream for a complete one.

// Drop accounting in its pure form: exact counts, no sockets, no timing.
// enqueue past the bound drops the NEWEST event and counts it; the next take
// folds the count into ONE leading gap event and resets; a clean take leaves
// nothing behind.
func TestSubscriberDropAccounting(t *testing.T) {
	sub := newSubscriber(2)

	sub.enqueue([]byte("a\n"))
	sub.enqueue([]byte("b\n"))
	sub.enqueue([]byte("c\n")) // dropped: queue was full

	batch := sub.take()
	if len(batch) != 3 {
		t.Fatalf("batch = %d lines, want 3 (gap + 2 queued)", len(batch))
	}
	var m map[string]any
	if err := json.Unmarshal(batch[0], &m); err != nil || m["event"] != "gap" {
		t.Fatalf("batch[0] = %s, want the gap event", batch[0])
	}
	if m["dropped"] != float64(1) {
		t.Errorf("gap.dropped = %v, want 1", m["dropped"])
	}
	if string(batch[1]) != "a\n" || string(batch[2]) != "b\n" {
		t.Errorf("queued events lost or reordered: %q %q", batch[1], batch[2])
	}

	// Counter reset: the next clean delivery carries no gap.
	sub.enqueue([]byte("d\n"))
	batch = sub.take()
	if len(batch) != 1 || string(batch[0]) != "d\n" {
		t.Fatalf("post-gap batch = %q, want just d", batch)
	}

	// Empty take (spurious wake) delivers nothing, not an empty gap.
	if batch := sub.take(); len(batch) != 0 {
		t.Fatalf("empty take produced %q", batch)
	}
}

// A subscriber attached but never reading: publishes must complete instantly
// (criterion 5's "does not block"), the queue must stay at its bound ("does
// not leak"), and resuming the reader must surface the gap.
func TestNeverReadSubscriberDoesNotBlockOrLeak(t *testing.T) {
	// Small queue bound; the DEFAULT (long) write deadline, deliberately:
	// this test exercises the QUEUE-BOUND regime, where the pump has wedged
	// against the unread kernel buffer but the connection stays up. The
	// kernel-stalled regime — deadline fires, connection dies — is
	// TestStalledWriterIsDisconnectedByWriteDeadline's job.
	cfg := Config{MaxQueuedEvents: 8}
	srv := startTestServer(t, cfg)

	tc := dialCtl(t, srv.path)
	tc.writeLine(t, `{"cmd":"subscribe"}`)
	drainSnapshot(t, tc)

	// Flood far past both the queue bound and the kernel socket buffer, with
	// the reader attached but silent. If any publish blocked, this loop would
	// hang and the fail-safe below would never even matter — completion
	// itself is the no-block assertion. Enough volume that the pump MUST
	// wedge against the unread buffer and start dropping.
	done := make(chan struct{})
	const total = 40000
	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			srv.PublishMessage("01JTEST", "alpha", "beta", "dm:beta")
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("publish blocked behind a subscriber that never reads")
	}

	// Memory half: find the connection's subscriber and assert its queue sat
	// at the bound, not at the flood size. (White-box on purpose: the bound
	// lives inside the type, so the assertion reads it there.)
	sub := onlySubscriber(t, srv.Server)
	if got := sub.pending(); got > cfg.maxQueued() {
		t.Fatalf("subscriber queue grew to %d events, bound is %d", got, cfg.maxQueued())
	}
	if sub.droppedCount() == 0 {
		t.Fatal("flood produced no drops; the bound was never exercised")
	}

	// Resume reading. Lines already sitting in the kernel buffer come first;
	// somewhere in the stream the gap report MUST appear, ahead of the
	// events that were dropped relative to it.
	deadline := time.Now().Add(10 * time.Second)
	sawGap := false
	for time.Now().Before(deadline) && !sawGap {
		m := tc.readLine(t)
		if m["event"] == "gap" {
			sawGap = true
			if m["dropped"].(float64) < 1 {
				t.Fatalf("gap reports dropped=%v, want >= 1", m["dropped"])
			}
		}
	}
	if !sawGap {
		t.Fatal("read the whole stream back and never saw the gap report")
	}

	// And the stream continues cleanly afterwards.
	next := tc.readLine(t)
	if next["type"] != "event" {
		t.Fatalf("post-gap stream broken: %v", next)
	}
}

// Abrupt disconnect mid-stream — the SIGKILL'd shell script is the NORMAL case
// per the brief. The server must notice, reclaim the subscription, keep
// serving everyone else, and never panic on publishes addressed to the dead
// connection.
func TestAbruptDisconnectMidStreamIsReclaimed(t *testing.T) {
	srv := startTestServer(t, Config{})

	dead := dialCtl(t, srv.path)
	dead.writeLine(t, `{"cmd":"subscribe"}`)
	drainSnapshot(t, dead)

	// Events in flight to the doomed connection...
	srv.PublishPresence(PresenceEntry{Device: "before", Online: true})
	// ...then the kill. No goodbye, no drain: exactly what SIGKILL does.
	if err := dead.c.Close(); err != nil {
		t.Fatalf("abrupt close: %v", err)
	}
	// ...and events published AFTER the kill, at the connection nobody is
	// reading. Must be a silent no-op for the dead sub, not a panic or block.
	for i := 0; i < 16; i++ {
		srv.PublishPresence(PresenceEntry{Device: "after", Online: true})
	}

	// Reclamation is asynchronous (the read loop notices EOF); wait for the
	// registry to shrink rather than assuming a propagation delay.
	waitFor(t, 5*time.Second, func() bool { return connCount(srv.Server) == 0 })

	// The service itself is unharmed: a fresh client gets a full round trip.
	fresh := dialCtl(t, srv.path)
	resp, _ := fresh.cmd(t, `{"cmd":"presence"}`)
	if resp["ok"] != true {
		t.Fatalf("server unusable after abrupt disconnect: %v", resp)
	}
}

// A subscriber stalled AT THE KERNEL — process alive but stopped reading
// (SIGSTOP, or a pager holding a pipe) — is the one failure mode the queue
// bound cannot catch, because the bytes left the queue and sit in the socket
// buffer. The write deadline is what turns that stall into a disconnect.
// This test necessarily lets a real (short) deadline elapse; it asserts the
// DISCONNECT outcome, not any timing precision.
func TestStalledWriterIsDisconnectedByWriteDeadline(t *testing.T) {
	srv := startTestServer(t, Config{WriteTimeout: 50 * time.Millisecond})

	conn := dialRaw(t, srv.path)
	writeCmd(t, conn, `{"cmd":"subscribe"}`)
	drainSnapshotConn(t, conn)

	// Do NOT read anymore. Enough events to fill the kernel socket buffer and
	// then some; the pump wedges on write, hits the deadline, tears the
	// connection down.
	for i := 0; i < 4096; i++ {
		srv.PublishMessage("id", "from", "to", "broadcast")
	}
	waitFor(t, 5*time.Second, func() bool { return connCount(srv.Server) == 0 })
}

// --- helpers ---

// startedServer pairs a running server with its socket path.
type startedServer struct {
	*Server
	path string
}

// startTestServer wires a recording-free server onto a fresh socket path.
func startTestServer(t *testing.T, cfg Config) *startedServer {
	t.Helper()
	srv, err := New(Handlers{
		Send:      func(string, string) error { return nil },
		SetStatus: func(string) error { return nil },
		Presence:  func() []PresenceEntry { return nil },
		ConnState: func() string { return "online" },
	}, cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	path := filepath.Join(t.TempDir(), "walkie.sock")
	l, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { srv.Close(); _ = l.Close() })
	go func() { _ = srv.Serve(l) }()
	return &startedServer{Server: srv, path: path}
}

func dialRaw(t *testing.T, path string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func writeCmd(t *testing.T, c net.Conn, line string) {
	t.Helper()
	if _, err := c.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write command: %v", err)
	}
}

// drainSnapshot consumes the subscribe response plus the seeding snapshot,
// whatever order the two writer paths delivered them.
func drainSnapshot(t *testing.T, tc *ctlClient) {
	t.Helper()
	gotResp, gotSnap := false, false
	for !gotResp || !gotSnap {
		m := tc.readLine(t)
		switch m["type"] {
		case "response":
			gotResp = true
		case "event":
			if m["event"] == "snapshot" {
				gotSnap = true
			}
		}
	}
}

// drainSnapshotConn consumes the subscribe response plus the seeding snapshot
// over a raw conn, whatever order the two writer paths delivered them.
func drainSnapshotConn(t *testing.T, c net.Conn) {
	t.Helper()
	gotResp, gotSnap := false, false
	deadline := time.Now().Add(5 * time.Second)
	_ = c.SetReadDeadline(deadline)
	scan := bufio.NewScanner(c)
	for (!gotResp || !gotSnap) && scan.Scan() {
		var m map[string]any
		if json.Unmarshal(scan.Bytes(), &m) != nil {
			continue
		}
		switch m["type"] {
		case "response":
			gotResp = true
		case "event":
			if m["event"] == "snapshot" {
				gotSnap = true
			}
		}
	}
	if !gotResp || !gotSnap {
		t.Fatal("never saw subscribe response + snapshot")
	}
	_ = c.SetReadDeadline(time.Time{}) // lift the handshake deadline again
}

// connCount reads the live-connection registry under its lock (the count is
// also how reclamation is observed; the value itself asserts nothing about
// timing).
func connCount(srv *Server) int {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return len(srv.conns)
}

// onlySubscriber fetches the single live subscription for white-box asserts.
func onlySubscriber(t *testing.T, srv *Server) *subscriber {
	t.Helper()
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for sc := range srv.conns {
		if sub := sc.subscriber(); sub != nil {
			return sub
		}
	}
	t.Fatal("no subscribed connection found")
	return nil
}

// waitFor polls cond until true or the deadline passes. The poll exists
// because peer-close propagation is an OS event no channel exposes; the
// deadline is a fail-safe against a broken reclamation path, never part of
// what is asserted.
func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition never became true")
}
