package history

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/message"
)

// The tests pin sto:message-history's criteria on a *clock.Fake and a real
// store: every retention claim is made true by advancing the clock or by
// counting rows, permissions are asserted with os.Stat rather than trusted
// from code reading (criterion 4 is verified by test, not inspection), and
// restart survival is exercised by closing and reopening the same path. No
// test sleeps in real time (ts:test-harness).

var epoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

const (
	testTTL = time.Hour
	testCap = 64 // high enough that cap tests set their own lower cap
)

func testClock() *clock.Fake { return clock.NewFake(epoch) }

func testOptions() Options {
	return Options{Local: "laptop.tail-scale.ts.net.", TTL: testTTL, MaxMessages: testCap}
}

func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

func mustOpen(t *testing.T, path string, clk clock.Clock, opts Options) *Store {
	t.Helper()
	s, err := Open(path, clk, opts)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// msg builds a received DM from peer, or a sent one when fromLocal. Times are
// offsets from the fake clock's epoch so every test stays on one timeline.
func msg(id, peer, body string, fromLocal bool, sentAgo, recvAgo time.Duration) message.Message {
	m := message.Message{
		ID:         id,
		Sender:     peer,
		Recipient:  "laptop.tail-scale.ts.net.",
		Body:       body,
		SentAt:     epoch.Add(-sentAgo),
		ReceivedAt: epoch.Add(-recvAgo),
	}
	if fromLocal {
		m.Sender = "laptop.tail-scale.ts.net."
		m.Recipient = peer
	}
	return m
}

func ids(msgs []message.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

func assertIDs(t *testing.T, got []message.Message, want ...string) {
	t.Helper()
	if strings.Join(ids(got), ",") != strings.Join(want, ",") {
		t.Fatalf("messages = %v, want %v", ids(got), want)
	}
}

// TestOpenCreatesFileAndDirectoryWithRestrictivePermissions is criterion 4,
// both halves of it: the FILE at 0600 (store.Open's contract) AND the parent
// DIRECTORY at 0700 — this item's addition, because a 0600 file inside a
// umask-created 0755 directory is a weaker guarantee than it looks.
func TestOpenCreatesFileAndDirectoryWithRestrictivePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "walkie")
	path := filepath.Join(dir, "history.db")

	s := mustOpen(t, path, testClock(), testOptions())

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("file mode = %#o, want 600", got)
	}
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := di.Mode().Perm(); got != 0o700 {
		t.Errorf("dir mode = %#o, want 700", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestOpenTightensPreexistingLooseDirectory: enforcement must not be
// create-time-only. A directory left at 0755 by an older build or another
// tool is tightened on the next Open, exactly like store.Open tightens a
// loose file.
func TestOpenTightensPreexistingLooseDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "walkie")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	path := filepath.Join(dir, "history.db")

	mustOpen(t, path, testClock(), testOptions())

	di, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := di.Mode().Perm(); got != 0o700 {
		t.Fatalf("pre-existing loose dir mode after Open = %#o, want tightened to 700", got)
	}
}

// TestRestartSurvivesAndReopenDisplaysPerConversationOrder is criterion 1:
// history survives a client restart, and reopen displays each conversation in
// per-conversation arrival order. The interleaving is deliberate — A1 B1 A2 B2
// across two conversations plus a broadcast — so the assertion exercises
// per-conversation order WITHOUT any global-order assumption: each
// conversation's sequence is read back independently and correctly.
func TestRestartSurvivesAndReopenDisplaysPerConversationOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	clk := testClock()

	s1 := mustOpen(t, path, clk, testOptions())
	// Interleave three conversations; also mix SENT and RECEIVED messages —
	// both directions persist (the item description's "received AND sent").
	appends := []message.Message{
		msg("m-a1", "alice.tail-scale.ts.net.", "a1", false, time.Minute, time.Minute),
		msg("m-b1", "bob.tail-scale.ts.net.", "b1", false, 2*time.Minute, 2*time.Minute),
		msg("m-a2", "alice.tail-scale.ts.net.", "a2", true, 3*time.Minute, 3*time.Minute),
		message.Message{ID: "m-x1", Sender: "carol.tail-scale.ts.net.", Body: "x1",
			SentAt: epoch.Add(-4 * time.Minute), ReceivedAt: epoch.Add(-4 * time.Minute)},
		msg("m-b2", "bob.tail-scale.ts.net.", "b2", true, 5*time.Minute, 5*time.Minute),
		msg("m-a3", "alice.tail-scale.ts.net.", "a3", false, 6*time.Minute, 6*time.Minute),
	}
	for _, m := range appends {
		if err := s1.Append(m); err != nil {
			t.Fatalf("append %s: %v", m.ID, err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close first session: %v", err)
	}
	// mustOpen registered a cleanup Close on s1 too; Close is idempotent
	// through database/sql, and the explicit call above is the restart point.

	// The restart: a fresh handle on the same file, same clock position.
	s2, err := Open(path, clk, testOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	alice, err := s2.Conversation(message.ConversationKey("alice.tail-scale.ts.net."))
	if err != nil {
		t.Fatalf("query alice: %v", err)
	}
	assertIDs(t, alice, "m-a1", "m-a2", "m-a3")

	bob, err := s2.Conversation(message.ConversationKey("bob.tail-scale.ts.net."))
	if err != nil {
		t.Fatalf("query bob: %v", err)
	}
	assertIDs(t, bob, "m-b1", "m-b2")

	bc, err := s2.Conversation(message.BroadcastConversation)
	if err != nil {
		t.Fatalf("query broadcast: %v", err)
	}
	assertIDs(t, bc, "m-x1")

	// Both timestamps survived the round trip — req:text-messaging keeps the
	// skew pair legible, so storage may not collapse it to one clock.
	got := alice[0]
	want := appends[0]
	if !got.SentAt.Equal(want.SentAt) || !got.ReceivedAt.Equal(want.ReceivedAt) {
		t.Fatalf("timestamps did not survive restart: sent %v/%v received %v/%v",
			got.SentAt, want.SentAt, got.ReceivedAt, want.ReceivedAt)
	}
	if got.Body != want.Body || got.Sender != want.Sender || got.Recipient != want.Recipient {
		t.Fatalf("content did not survive restart: %+v vs %+v", got, want)
	}
}

// TestAppendDeduplicatesByULID: delivery is at-least-once on the wire, so a
// replayed queue drain re-delivers old ULIDs. History must stay idempotent at
// rest the same way the display Log dedups at screen time — one stored row per
// ULID, never two lines on reopen.
func TestAppendDeduplicatesByULID(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "history.db"), testClock(), testOptions())
	m := msg("m-dup", "alice.tail-scale.ts.net.", "once", false, time.Minute, time.Minute)

	if err := s.Append(m); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := s.Append(m); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}

	got, err := s.Conversation(message.ConversationKey("alice.tail-scale.ts.net."))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	assertIDs(t, got, "m-dup")
}

// TestTTLExpiryOnFakeClock: a message past the TTL is REMOVED, not merely
// invisible at read time, expiry happens without any read having to happen
// first (the sweep rides Append and Open), and the log line records counts —
// content-free, criterion 3's no-content rule applied to this package's own
// logs.
func TestTTLExpiryOnFakeClock(t *testing.T) {
	logs := &bytes.Buffer{}
	opts := testOptions()
	opts.Logger = testLogger(logs)
	clk := testClock()
	s := mustOpen(t, filepath.Join(t.TempDir(), "history.db"), clk, opts)

	secret := "the launch codes are under the mat"
	if err := s.Append(msg("m-old", "alice.tail-scale.ts.net.", secret, false, time.Minute, time.Minute)); err != nil {
		t.Fatalf("append old: %v", err)
	}

	// Half the TTL later a second message lands; then cross only the FIRST
	// message's deadline. Exactly it expires.
	clk.Advance(testTTL / 2)
	if err := s.Append(msg("m-new", "alice.tail-scale.ts.net.", "harmless", false, 0, 0)); err != nil {
		t.Fatalf("append new: %v", err)
	}
	clk.Advance(testTTL / 2)
	if err := s.Append(msg("m-trigger", "alice.tail-scale.ts.net.", "trigger", false, 0, 0)); err != nil {
		t.Fatalf("append trigger: %v", err)
	}

	got, err := s.Conversation(message.ConversationKey("alice.tail-scale.ts.net."))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	assertIDs(t, got, "m-new", "m-trigger")

	out := logs.String()
	if !strings.Contains(out, "history message expired") || !strings.Contains(out, "count=1") {
		t.Errorf("expiry not recorded in logs; log:\n%s", out)
	}
	if strings.Contains(out, secret) {
		t.Errorf("retention log carries message BODY — the no-content rule is violated; log:\n%s", out)
	}
}

// TestExpiredDuringDowntimeSweptAtOpen: messages that lapsed while no client
// was running are evicted at startup, before anything can display them stale.
func TestExpiredDuringDowntimeSweptAtOpen(t *testing.T) {
	logs := &bytes.Buffer{}
	path := filepath.Join(t.TempDir(), "history.db")
	clk := testClock()

	opts := testOptions()
	s1 := mustOpen(t, path, clk, opts)
	if err := s1.Append(msg("m-lapsed", "alice.tail-scale.ts.net.", "old", false, time.Minute, time.Minute)); err != nil {
		t.Fatalf("append: %v", err)
	}
	s1.Close()

	// Downtime: the clock runs past the TTL with nobody watching.
	clk.Advance(2 * testTTL)

	restartOpts := testOptions()
	restartOpts.Logger = testLogger(logs)
	s2, err := Open(path, clk, restartOpts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.Conversation(message.ConversationKey("alice.tail-scale.ts.net."))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("downtime-expired message resurrected: %v", ids(got))
	}
	if out := logs.String(); !strings.Contains(out, "history message expired") {
		t.Errorf("startup sweep did not log the downtime expiry; log:\n%s", out)
	}
}

// TestSizeCapEvictsOldestLoudly: reaching the cap is bounded retention working
// — the OLDEST rows leave, the newest survive, nothing grows unbounded, and
// the eviction is loud in the logs (counts and bounds, never bodies).
func TestSizeCapEvictsOldestLoudly(t *testing.T) {
	logs := &bytes.Buffer{}
	opts := testOptions()
	opts.MaxMessages = 3
	opts.Logger = testLogger(logs)
	s := mustOpen(t, filepath.Join(t.TempDir(), "history.db"), testClock(), opts)

	const secret = "private body that must never reach a log"
	var appended []string
	for i := 0; i < 5; i++ {
		id := message.NewID(epoch.Add(time.Duration(i) * time.Second))
		appended = append(appended, id)
		m := msg(id, "alice.tail-scale.ts.net.", secret, false, 0, 0)
		if err := s.Append(m); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got, err := s.Query(QueryOptions{})
	if err != nil {
		t.Fatalf("query all: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("retained %d messages, want exactly the cap of 3", len(got))
	}
	// The OLDEST left, the newest stayed: survivors are the last three arrivals.
	assertIDs(t, got, appended[2], appended[3], appended[4])

	out := logs.String()
	if !strings.Contains(out, "history size cap reached") {
		t.Errorf("cap eviction not logged loudly; log:\n%s", out)
	}
	if strings.Contains(out, secret) {
		t.Errorf("eviction log carries message BODY — the no-content rule is violated; log:\n%s", out)
	}
}

// TestQueryByTimeRange filters on received_at inclusively on both ends — the
// CLI's -from/-to surface rests on exactly this behaviour.
func TestQueryByTimeRange(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "history.db"), testClock(), testOptions())
	// Received 40, 30, 20 minutes ago; sent times deliberately skewed so a
	// filter accidentally hitting sent_at would return a different set.
	for i, ago := range []time.Duration{40 * time.Minute, 30 * time.Minute, 20 * time.Minute} {
		m := msg(message.NewID(epoch.Add(time.Duration(i)*time.Second)),
			"alice.tail-scale.ts.net.", "body", false, 90*time.Minute, ago)
		if err := s.Append(m); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	from := epoch.Add(-31 * time.Minute)
	to := epoch.Add(-19 * time.Minute)
	got, err := s.Query(QueryOptions{From: &from, To: &to})
	if err != nil {
		t.Fatalf("query range: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("range returned %d messages, want 2 (inclusive bounds)", len(got))
	}

	// Conversation AND range compose.
	from = epoch.Add(-45 * time.Minute)
	to = epoch.Add(-35 * time.Minute)
	got, err = s.Query(QueryOptions{
		Conversation: message.ConversationKey("alice.tail-scale.ts.net."),
		From:         &from, To: &to,
	})
	if err != nil {
		t.Fatalf("query conv+range: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("conv+range returned %d messages, want 1", len(got))
	}

	// An empty result is a valid answer, not an error.
	from = epoch.Add(-90 * time.Minute)
	to = epoch.Add(-80 * time.Minute)
	got, err = s.Query(QueryOptions{From: &from, To: &to})
	if err != nil {
		t.Fatalf("empty range query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty range returned %v", ids(got))
	}
}

// TestOpenRefusesWiringErrors: non-positive bounds, missing device name or a
// nil clock are programmer errors refused at construction, per the house rule
// — a history that expires instantly or evicts everything is a wiring bug,
// not a mode.
func TestOpenRefusesWiringErrors(t *testing.T) {
	clk := testClock()
	path := filepath.Join(t.TempDir(), "history.db")

	cases := []struct {
		name string
		opts Options
		clk  clock.Clock
	}{
		{"nil clock", testOptions(), nil},
		{"empty local", func() Options { o := testOptions(); o.Local = ""; return o }(), clk},
		{"zero ttl", func() Options { o := testOptions(); o.TTL = 0; return o }(), clk},
		{"negative ttl", func() Options { o := testOptions(); o.TTL = -time.Second; return o }(), clk},
		{"zero cap", func() Options { o := testOptions(); o.MaxMessages = 0; return o }(), clk},
		{"negative cap", func() Options { o := testOptions(); o.MaxMessages = -1; return o }(), clk},
	}
	for _, tc := range cases {
		if _, err := Open(path, tc.clk, tc.opts); err == nil {
			t.Errorf("%s: Open succeeded, want error", tc.name)
		}
	}
}

// TestAppendRefusesEmptyID: the ULID is the whole dedup mechanism; storing a
// row without one would be un-deduplicable filler.
func TestAppendRefusesEmptyID(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "history.db"), testClock(), testOptions())
	m := msg("", "alice.tail-scale.ts.net.", "body", false, 0, 0)
	if err := s.Append(m); err == nil {
		t.Fatal("append without ID succeeded, want error")
	}
}
