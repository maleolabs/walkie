package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/history"
	"github.com/maleolabs/walkie/internal/message"
)

// The tests exercise runHistory directly — the subcommand is a function over
// (args, stdout, stderr) returning an exit code, so no exec, no real clock,
// no dependence on where the test binary was built. The database is seeded
// through history.Open/Append on a *clock.Fake, which is the same seam the
// client will use, and read back through the CLI's own parse of RFC3339.

var cliEpoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

const cliLocal = "laptop.tail-scale.ts.net."

// seedHistory opens a history store at a temp path and appends the given
// messages at fixed offsets from cliEpoch.
func seedHistory(t *testing.T, msgs ...message.Message) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.db")
	s, err := history.Open(path, clock.NewFake(cliEpoch), history.Options{
		TTL:         24 * time.Hour,
		MaxMessages: 100,
	})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	for _, m := range msgs {
		if err := s.Append(cliLocal, m); err != nil {
			t.Fatalf("seed append %s: %v", m.ID, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	return path
}

func dm(id, peer, body string, fromLocal bool, recvAgo time.Duration) message.Message {
	m := message.Message{
		ID:         id,
		Sender:     peer,
		Recipient:  cliLocal,
		Body:       body,
		SentAt:     cliEpoch.Add(-recvAgo - time.Minute),
		ReceivedAt: cliEpoch.Add(-recvAgo),
	}
	if fromLocal {
		m.Sender = cliLocal
		m.Recipient = peer
	}
	return m
}

type outLine struct {
	ID           string `json:"id"`
	Conversation string `json:"conversation"`
	Sender       string `json:"sender"`
	Recipient    string `json:"recipient"`
	SentAt       string `json:"sent_at"`
	ReceivedAt   string `json:"received_at"`
	Body         string `json:"body"`
}

func parseLines(t *testing.T, out string) []outLine {
	t.Helper()
	var lines []outLine
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l == "" {
			continue
		}
		var ol outLine
		if err := json.Unmarshal([]byte(l), &ol); err != nil {
			t.Fatalf("output line is not JSON: %q: %v", l, err)
		}
		lines = append(lines, ol)
	}
	return lines
}

func TestHistoryQueryByConversation(t *testing.T) {
	path := seedHistory(t,
		dm("m-a1", "alice.ts.net.", "from alice", false, 40*time.Minute),
		dm("m-b1", "bob.ts.net.", "from bob", false, 30*time.Minute),
		dm("m-a2", "alice.ts.net.", "to alice", true, 20*time.Minute),
	)

	var stdout, stderr bytes.Buffer
	code := runHistory([]string{"-db", path, "-conversation", "alice.ts.net."}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr:\n%s", code, stderr.String())
	}

	lines := parseLines(t, stdout.String())
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2; output:\n%s", len(lines), stdout.String())
	}
	// Per-conversation arrival order, oldest first — criterion 1's order made
	// visible through the query surface.
	if lines[0].ID != "m-a1" || lines[1].ID != "m-a2" {
		t.Fatalf("order = [%s, %s], want [m-a1, m-a2]", lines[0].ID, lines[1].ID)
	}
	for _, l := range lines {
		if l.Conversation != "dm:alice.ts.net." {
			t.Errorf("conversation = %q, want dm:alice.ts.net.", l.Conversation)
		}
	}
	// Direction survives round trip: sent vs received orientation is legible.
	if lines[0].Sender == cliLocal || lines[1].Sender != cliLocal {
		t.Errorf("sender orientation wrong: %+v", lines)
	}
}

func TestHistoryQueryByTimeRange(t *testing.T) {
	path := seedHistory(t,
		dm("m-old", "alice.ts.net.", "old", false, 40*time.Minute),
		dm("m-mid", "alice.ts.net.", "mid", false, 30*time.Minute),
		dm("m-new", "alice.ts.net.", "new", false, 10*time.Minute),
	)

	var stdout, stderr bytes.Buffer
	args := []string{
		"-db", path,
		"-from", cliEpoch.Add(-35 * time.Minute).Format(time.RFC3339),
		"-to", cliEpoch.Add(-5 * time.Minute).Format(time.RFC3339),
	}
	code := runHistory(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr:\n%s", code, stderr.String())
	}

	lines := parseLines(t, stdout.String())
	if len(lines) != 2 || lines[0].ID != "m-mid" || lines[1].ID != "m-new" {
		t.Fatalf("range result = %+v, want [m-mid, m-new]", lines)
	}
	// Timestamps come back machine-parseable in UTC.
	ts, err := time.Parse(time.RFC3339Nano, lines[0].ReceivedAt)
	if err != nil {
		t.Fatalf("received_at not RFC3339: %q: %v", lines[0].ReceivedAt, err)
	}
	if want := cliEpoch.Add(-30 * time.Minute); !ts.Equal(want) {
		t.Errorf("received_at = %v, want %v", ts, want)
	}
}

func TestHistoryQueryConversationAndRangeCompose(t *testing.T) {
	path := seedHistory(t,
		dm("m-a-old", "alice.ts.net.", "a old", false, 50*time.Minute),
		dm("m-b-old", "bob.ts.net.", "b old", false, 45*time.Minute),
		dm("m-a-new", "alice.ts.net.", "a new", false, 15*time.Minute),
	)

	var stdout, stderr bytes.Buffer
	args := []string{
		"-db", path,
		"-conversation", "alice.ts.net.",
		"-from", cliEpoch.Add(-60 * time.Minute).Format(time.RFC3339),
		"-to", cliEpoch.Add(-40 * time.Minute).Format(time.RFC3339),
	}
	code := runHistory(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr:\n%s", code, stderr.String())
	}
	lines := parseLines(t, stdout.String())
	if len(lines) != 1 || lines[0].ID != "m-a-old" {
		t.Fatalf("composed filter = %+v, want exactly m-a-old", lines)
	}
}

func TestHistoryQueryBroadcastKey(t *testing.T) {
	path := seedHistory(t,
		message.Message{ID: "m-x", Sender: "carol.ts.net.", Body: "shout",
			SentAt: cliEpoch.Add(-time.Minute), ReceivedAt: cliEpoch.Add(-time.Minute)},
	)

	var stdout, stderr bytes.Buffer
	code := runHistory([]string{"-db", path, "-conversation", "broadcast"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr:\n%s", code, stderr.String())
	}
	lines := parseLines(t, stdout.String())
	if len(lines) != 1 || lines[0].ID != "m-x" || lines[0].Conversation != "broadcast" {
		t.Fatalf("broadcast query = %+v, want m-x under broadcast", lines)
	}
}

func TestHistoryQueryMissingDatabaseFailsCleanly(t *testing.T) {
	var stdout, stderr bytes.Buffer
	missing := filepath.Join(t.TempDir(), "nope.db")
	code := runHistory([]string{"-db", missing}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no history database at "+missing) {
		t.Errorf("stderr does not name the missing path:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout not empty on failure:\n%s", stdout.String())
	}
}

func TestHistoryUsageErrorsAndHelp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	cases := []struct {
		name string
		args []string
		want int
	}{
		{"bad timestamp", []string{"-db", path, "-from", "not-a-time"}, 2},
		{"stray argument", []string{"-db", path, "extra"}, 2},
	}
	for _, tc := range cases {
		var stdout, stderr bytes.Buffer
		if code := runHistory(tc.args, &stdout, &stderr); code != tc.want {
			t.Errorf("%s: exit = %d, want %d", tc.name, code, tc.want)
		}
	}

	// -help must succeed AND carry the unencrypted-at-rest statement — it is
	// one of the two homes criterion 5 names for it.
	var stdout, stderr bytes.Buffer
	if code := runHistory([]string{"-help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("-help exit = %d", code)
	}
	usage := stderr.String()
	for _, phrase := range []string{"UNENCRYPTED", "adr:004-security-model"} {
		if !strings.Contains(usage, phrase) {
			t.Errorf("-help missing required security statement %q; usage:\n%s", phrase, usage)
		}
	}
}

// TestDefaultDBPathUnderHome pins the default to ~/.walkie/history.db — the
// path .gitignore's /.walkie/ entry already anticipates, and the path the
// assembled client must open too.
func TestDefaultDBPathUnderHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory in this environment")
	}
	want := filepath.Join(home, ".walkie", "history.db")
	if got := defaultDBPath(); got != want {
		t.Fatalf("defaultDBPath() = %q, want %q", got, want)
	}
}
