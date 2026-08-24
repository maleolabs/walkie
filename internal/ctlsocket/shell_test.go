//go:build unix

package ctlsocket

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Shell-level proof for acceptance criteria 2 and 3: a script with NO
// walkie-specific tooling must be able to send a command and read the event
// stream. These tests shell out and speak the protocol with socat — the
// generic swiss-army relay every admin already has, and the exact tool the
// item brief names alongside nc. An in-process Go client talking to itself
// proves nothing about this; the brief is explicit that it must be a shell.
//
// (A bare `printf ... > sock` cannot work at ALL for Unix domain sockets:
// open(2) on a socket file returns ENXIO — connecting requires connect(2),
// which is precisely why socat/nc exist. The brief's "plain redirect" posture
// is honoured by piping INTO socat, which is a redirect in every sense that
// matters to a script author.)
//
// PATH carries only the standard system directories: nothing walkie-shaped
// participates.

const shellPath = "/usr/bin:/bin"

// runShell runs one `sh -c` script with SOCK in its environment and returns
// its standard output.
func runShell(t *testing.T, sockPath, script string) string {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{"SOCK=" + sockPath, "PATH=" + shellPath}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("sh -c %q: %v\noutput so far: %s", script, err, out)
	}
	return string(out)
}

// assertNDJSON parses every line as JSON and returns the objects.
func assertNDJSON(t *testing.T, out string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line is not JSON: %q (%v)", line, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// Criterion 2, send half: drive a message through the socket from a shell,
// read the response back on the same connection. No nc flags beyond socat's
// address spec, no walkie binary.
//
// socat's default linger (0.5s after stdin EOF) is what lets the response
// come back before the client side tears down; the wait is bounded idle time
// in the TOOL, not a sleep the test relies on for correctness.
func TestShellSendsCommandAndReadsResponse(t *testing.T) {
	h := newCtlHarness(t, nil)

	out := runShell(t, h.path, `printf '%s\n' '{"cmd":"send","to":"beta","body":"hello from plain sh","id":"s1"}' | socat - UNIX-CONNECT:"$SOCK"`)
	lines := assertNDJSON(t, out)
	if len(lines) != 1 {
		t.Fatalf("expected exactly the response line, got %d: %v", len(lines), lines)
	}
	resp := lines[0]
	if resp["ok"] != true || resp["id"] != "s1" {
		t.Fatalf("response = %v, want ok:true with id echo", resp)
	}
	if len(h.sentTo) != 1 || h.sentTo[0] != "beta" || h.sentBody[0] != "hello from plain sh" {
		t.Fatalf("handler saw to=%q body=%q; the shell's message did not arrive intact", h.sentTo, h.sentBody)
	}
}

// startShellObserver starts a long-lived subscriber: it writes the subscribe
// line, then holds its stdin open so socat keeps streaming socket -> stdout.
// The returned pipe is the hold; close it (or kill the process) to end the
// subscription the way a killed pipeline would.
func startShellObserver(t *testing.T, sockPath string, stdoutPath string) *exec.Cmd {
	t.Helper()
	// The subshell writes the subscribe line, then `cat` holds stdin open so
	// socat keeps the connection and streams socket -> $OUT.
	cmd := exec.Command("sh", "-c", `{ printf '%s\n' '{"cmd":"subscribe"}'; cat; } | socat - UNIX-CONNECT:"$SOCK" > "$OUT"`)
	cmd.Env = []string{"SOCK=" + sockPath, "OUT=" + stdoutPath, "PATH=" + shellPath}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	// Own process group, so the test can kill the whole pipeline (shell AND
	// socat) at once — what a terminal close does to a user's script.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start observer: %v", err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = killGroup(cmd)
		_, _ = cmd.Process.Wait()
	})
	return cmd
}

// killGroup SIGKILLs cmd's whole process group.
func killGroup(cmd *exec.Cmd) error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// Criterion 3, event half: a shell subscriber receiving line-delimited JSON
// while events are published, then SIGKILL'd mid-stream — the normal way a
// pipeline dies. The server must reclaim the subscriber and keep serving.
func TestShellReadsLineDelimitedEventStream(t *testing.T) {
	h := newCtlHarness(t, nil)

	obsOut := filepath.Join(t.TempDir(), "events.ndjson")
	observer := startShellObserver(t, h.path, obsOut)

	// Readiness: the snapshot line exists only once subscribe took effect
	// server-side. Poll the artifact, never the clock.
	waitFor(t, 5*time.Second, func() bool {
		buf, err := os.ReadFile(obsOut)
		return err == nil && strings.Contains(string(buf), `"snapshot"`)
	})

	// Publish while the shell reads. Fan-out reaches it as complete lines.
	h.srv.PublishConn("connecting", "online", "handshake completed", "2026-08-24T01:02:03Z")
	h.srv.PublishMessage("01JSHELL", "alpha", "", "broadcast")

	ok := waitForOK(t, 5*time.Second, func() bool {
		buf, err := os.ReadFile(obsOut)
		return err == nil && strings.Contains(string(buf), `"event":"message"`)
	})
	if !ok {
		buf, _ := os.ReadFile(obsOut)
		t.Fatalf("events never arrived; observer alive=%v; file:\n%s", observer.Process.Signal(nil) == nil, buf)
	}
	buf, err := os.ReadFile(obsOut)
	if err != nil {
		t.Fatalf("read events file: %v", err)
	}
	sawConn, sawMsg := false, false
	for _, m := range assertNDJSON(t, string(buf)) {
		if m["event"] == "conn" && m["to"] == "online" {
			sawConn = true
		}
		if m["event"] == "message" && m["id"] == "01JSHELL" {
			sawMsg = true
		}
	}
	if !sawConn || !sawMsg {
		t.Fatalf("event stream incomplete: conn=%v message=%v\n%s", sawConn, sawMsg, buf)
	}

	// SIGKILL mid-stream — the whole process GROUP, because that is what a
	// user's Ctrl-C / closed terminal does to a `sh -c '... | socat'`
	// pipeline: killing only the shell would leave socat (its child) holding
	// the socket, which is a live subscriber, not a dead one.
	if err := killGroup(observer); err != nil {
		t.Fatalf("kill observer group: %v", err)
	}
	_, _ = observer.Process.Wait()

	waitFor(t, 5*time.Second, func() bool { return connCount(h.srv) == 0 })

	// And the socket still serves: a fresh round trip works.
	fresh := dialCtl(t, h.path)
	if resp, _ := fresh.cmd(t, `{"cmd":"presence"}`); resp["ok"] != true {
		t.Fatalf("server unusable after SIGKILL'd subscriber: %v", resp)
	}
}

// Criteria 2 AND 3 together, the full posture from the brief: two independent
// shell processes — one observing events, one sending a command — plus NDJSON
// parsing of what arrived. This is the demo flow ts:docs-quickstart-runbook
// will document, proven end to end.
func TestShellTwoProcessesSendAndObserve(t *testing.T) {
	h := newCtlHarness(t, nil)

	obsOut := filepath.Join(t.TempDir(), "events.ndjson")
	startShellObserver(t, h.path, obsOut)

	waitFor(t, 5*time.Second, func() bool {
		buf, err := os.ReadFile(obsOut)
		return err == nil && strings.Contains(string(buf), `"snapshot"`)
	})

	// Sender: one shot, fire-and-forget, its own connection.
	runShell(t, h.path, `printf '%s\n' '{"cmd":"send","body":"via socat"}' | socat - UNIX-CONNECT:"$SOCK"`)

	// The observer sees what the SERVER publishes. This harness's Send
	// handler records without filing, so the send produces no message event
	// here; publish one presence change for the observer to catch, proving
	// live fan-out into a plain shell process.
	h.srv.PublishPresence(PresenceEntry{Device: "gamma", Online: true})

	waitFor(t, 5*time.Second, func() bool {
		buf, err := os.ReadFile(obsOut)
		return err == nil && strings.Contains(string(buf), `"gamma"`)
	})

	buf, err := os.ReadFile(obsOut)
	if err != nil {
		t.Fatalf("read events file: %v", err)
	}
	lines := assertNDJSON(t, string(buf))
	sawSnapshot, sawPresence := false, false
	for _, m := range lines {
		if m["event"] == "snapshot" {
			sawSnapshot = true
		}
		if m["event"] == "presence" && m["device"] == "gamma" {
			sawPresence = true
		}
	}
	if !sawSnapshot || !sawPresence {
		t.Fatalf("observer stream incomplete: snapshot=%v presence=%v\n%s", sawSnapshot, sawPresence, buf)
	}

	// And the sender's command really reached the handler.
	if len(h.sentBody) != 1 || h.sentBody[0] != "via socat" {
		t.Fatalf("redirected send did not arrive: %q", h.sentBody)
	}
}
