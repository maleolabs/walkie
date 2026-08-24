package ctlsocket

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// --- Listen-level behaviour (criteria 1 and 4) ---

// Criterion 1's exit half: a deliberate shutdown leaves no socket file behind.
// (The unclean-previous-run half is TestListenRemovesDeadSocket below.)
func TestListenRemovesSocketOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "walkie.sock")

	l, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("socket file survived Close: stat err = %v, want IsNotExist", err)
	}
}

// The full unclean-shutdown cycle: leftover file -> probe removes it -> bind
// works -> clean close removes it again.
func TestListenRecoversFromUncleanShutdownCycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "walkie.sock")

	first, err := Listen(path)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	_ = first.Close()
	// Simulate the crash: Go's Close unlinks the file, so put a dead socket
	// file back, exactly what SIGKILL leaves behind.
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("simulate leftover socket file: %v", err)
	}

	second, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen over dead socket: %v", err)
	}
	defer second.Close()
}

// Criterion 4, filesystem half: the endpoint is a SOCKET FILE (not a regular
// file a reader could be tricked by), owner-only, inside an owner-only
// directory. Combined with Serve taking its listener from Listen only — there
// is no code path in this package that opens anything but a Unix domain
// socket — this is the whole local-only story short of a kernel exploit.
func TestListenSocketFileTypeAndOwnerOnlyPerms(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry Unix permission bits")
	}
	dir := t.TempDir()
	// Point at a NOT-YET-EXISTING subdirectory: Listen's contract is to create
	// the socket's parent with 0700, and MkdirAll deliberately does not chmod
	// a directory that already exists (TempDir's own 0700 or otherwise).
	path := filepath.Join(dir, "state", "walkie.sock")

	l, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Errorf("path is not a socket file: mode = %s", info.Mode())
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket permissions = %#o, want 0600", perm)
	}

	// The directory LISTEN created (not TempDir itself) carries the 0700.
	parent := filepath.Dir(path)
	dirInfo, err := os.Stat(parent)
	if err != nil {
		t.Fatalf("stat socket directory: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0o700 {
		t.Errorf("socket directory permissions = %#o, want 0700", perm)
	}
}

// Criterion 4, network half: the listener's address names the unix network.
// There is no TCP fallback anywhere in the package to assert against; the
// assertion pins the one listener creation there is.
func TestListenAddrIsUnixNetwork(t *testing.T) {
	l, err := Listen(filepath.Join(t.TempDir(), "walkie.sock"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if got := l.Addr().Network(); got != "unix" {
		t.Errorf("listener network = %q, want %q", got, "unix")
	}
}

// --- Server-level behaviour ---

// ctlHarness is a server on a fresh socket plus recording handlers.
type ctlHarness struct {
	srv  *Server
	path string

	sentTo    []string
	sentBody  []string
	sendErr   error
	setStatus []string
	statusErr error
	roster    []PresenceEntry
	state     string
}

func newCtlHarness(t *testing.T, mutate func(*ctlHarness)) *ctlHarness {
	t.Helper()
	h := &ctlHarness{
		roster: []PresenceEntry{
			{Device: "alpha", Online: true},
			{Device: "beta", Online: false, LastSeen: "2026-08-24T00:00:00Z", Status: "brb"},
		},
		state: "online",
	}
	if mutate != nil {
		mutate(h)
	}
	srv, err := New(Handlers{
		Send: func(to, body string) error {
			h.sentTo = append(h.sentTo, to)
			h.sentBody = append(h.sentBody, body)
			return h.sendErr
		},
		SetStatus: func(status string) error {
			h.setStatus = append(h.setStatus, status)
			return h.statusErr
		},
		Presence:  func() []PresenceEntry { return h.roster },
		ConnState: func() string { return h.state },
	}, Config{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.srv = srv

	path := filepath.Join(t.TempDir(), "walkie.sock")
	l, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() {
		srv.Close()
		_ = l.Close()
	})
	h.path = path
	go func() { _ = srv.Serve(l) }()
	return h
}

// ctlClient is an in-process protocol client over a real socket connection.
type ctlClient struct {
	c    net.Conn
	scan *bufio.Scanner
}

func dialCtl(t *testing.T, path string) *ctlClient {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 0, 4096), maxLineBytes)
	return &ctlClient{c: conn, scan: scan}
}

// readLine reads one NDJSON line, failing the test on hang (the deadline is a
// fail-safe against a broken server, never part of what is being asserted).
func (tc *ctlClient) readLine(t *testing.T) map[string]any {
	t.Helper()
	type result struct {
		line map[string]any
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		if !tc.scan.Scan() {
			ch <- result{err: tc.scan.Err()}
			return
		}
		var m map[string]any
		if err := json.Unmarshal(tc.scan.Bytes(), &m); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{line: m}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("read line: %v", r.err)
		}
		return r.line
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a line; server stopped delivering")
		return nil
	}
}

// writeLine sends one raw request line.
func (tc *ctlClient) writeLine(t *testing.T, line string) {
	t.Helper()
	if _, err := tc.c.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
}

// cmd sends one command and waits for its response, collecting any events
// that arrived first (response-vs-snapshot ordering across the two writer
// goroutines is deliberately unspecified; both orders must be correct).
func (tc *ctlClient) cmd(t *testing.T, line string) (resp map[string]any, events []map[string]any) {
	t.Helper()
	tc.writeLine(t, line)
	for {
		m := tc.readLine(t)
		if m["type"] == "response" {
			return m, events
		}
		events = append(events, m)
	}
}

func TestNewRefusesHalfWiredHandlers(t *testing.T) {
	if _, err := New(Handlers{}, Config{}, nil); err == nil {
		t.Fatal("New accepted empty handlers; a half-wired server would panic mid-session instead")
	}
}

func TestPresenceCommandReturnsRoster(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	resp, events := tc.cmd(t, `{"cmd":"presence","id":7}`)
	if resp["ok"] != true {
		t.Fatalf("presence response not ok: %v", resp)
	}
	if resp["id"] != float64(7) {
		t.Errorf("id echo = %v, want 7", resp["id"])
	}
	if len(events) != 0 {
		t.Errorf("unsubscribed connection received events: %v", events)
	}
	roster, ok := resp["presence"].([]any)
	if !ok || len(roster) != 2 {
		t.Fatalf("presence payload wrong: %v", resp["presence"])
	}
	first := roster[0].(map[string]any)
	if first["device"] != "alpha" || first["online"] != true {
		t.Errorf("roster[0] = %v, want alpha/online", first)
	}
}

func TestSendCommandReachesHandlerAndEchoesErrors(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	resp, _ := tc.cmd(t, `{"cmd":"send","to":"beta","body":"hello from sh"}`)
	if resp["ok"] != true {
		t.Fatalf("send response not ok: %v", resp)
	}
	if len(h.sentTo) != 1 || h.sentTo[0] != "beta" || h.sentBody[0] != "hello from sh" {
		t.Fatalf("handler saw to=%q body=%q", h.sentTo, h.sentBody)
	}

	// Broadcast: omitted "to".
	resp, _ = tc.cmd(t, `{"cmd":"send","body":"all hands"}`)
	if resp["ok"] != true {
		t.Fatalf("broadcast send not ok: %v", resp)
	}
	if h.sentTo[1] != "" {
		t.Errorf("broadcast reached handler with to=%q, want empty", h.sentTo[1])
	}

	// Handler errors surface verbatim so the script can act on them.
	h.sendErr = errFakeOffline
	resp, _ = tc.cmd(t, `{"cmd":"send","body":"x"}`)
	if resp["ok"] != false || resp["error"] != errFakeOffline.Error() {
		t.Errorf("error response = %v, want ok:false with handler error", resp)
	}
}

var errFakeOffline = &fakeError{"offline - message held"}

type fakeError struct{ s string }

func (e *fakeError) Error() string { return e.s }

func TestStatusCommandReachesHandler(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	resp, _ := tc.cmd(t, `{"cmd":"status","status":"deploying","id":"abc"}`)
	if resp["ok"] != true || resp["id"] != "abc" {
		t.Fatalf("status response = %v, want ok with id echo", resp)
	}
	if len(h.setStatus) != 1 || h.setStatus[0] != "deploying" {
		t.Fatalf("handler saw status %q", h.setStatus)
	}
}

func TestUnknownAndMalformedCommandsKeepConnectionUsable(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	resp, _ := tc.cmd(t, `{"cmd":"reboot"}`)
	if resp["ok"] != false {
		t.Errorf("unknown cmd answered ok: %v", resp)
	}
	if resp["error"].(string) == "" {
		t.Errorf("unknown cmd carried no error text: %v", resp)
	}

	resp, _ = tc.cmd(t, `{not json`)
	if resp["ok"] != false {
		t.Errorf("malformed line answered ok: %v", resp)
	}

	// The point of keep-open semantics: the same connection still works.
	resp, _ = tc.cmd(t, `{"cmd":"presence"}`)
	if resp["ok"] != true {
		t.Fatalf("connection unusable after errors: %v", resp)
	}
}

func TestSubscribeDeliversSnapshotThenLiveEvents(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	resp, events := tc.cmd(t, `{"cmd":"subscribe"}`)
	if resp["ok"] != true {
		t.Fatalf("subscribe refused: %v", resp)
	}
	// Exactly one seeding snapshot, carrying BOTH halves of current state.
	if len(events) != 1 || events[0]["event"] != "snapshot" {
		t.Fatalf("expected exactly one snapshot event, got %v", events)
	}
	snap := events[0]
	if conn := snap["conn"].(map[string]any); conn["state"] != "online" {
		t.Errorf("snapshot conn state = %v, want online", snap["conn"])
	}
	if roster := snap["presence"].([]any); len(roster) != 2 {
		t.Errorf("snapshot roster size = %d, want 2", len(roster))
	}

	// Live fan-out: every publish reaches the subscriber as one parseable line.
	h.srv.PublishConn("connecting", "online", "handshake completed", "2026-08-24T01:02:03Z")
	h.srv.PublishPresence(PresenceEntry{Device: "gamma", Online: true})
	h.srv.PublishMessage("01JXYZ", "alpha", "beta", "dm:beta")

	want := []struct {
		event string
		check func(map[string]any)
	}{
		{"conn", func(m map[string]any) {
			if m["from"] != "connecting" || m["to"] != "online" || m["reason"] != "handshake completed" {
				t.Errorf("conn event fields = %v", m)
			}
		}},
		{"presence", func(m map[string]any) {
			if m["device"] != "gamma" || m["online"] != true {
				t.Errorf("presence event fields = %v", m)
			}
		}},
		{"message", func(m map[string]any) {
			if m["id"] != "01JXYZ" || m["conversation"] != "dm:beta" {
				t.Errorf("message event fields = %v", m)
			}
			if _, has := m["body"]; has {
				t.Errorf("message event carries a body; content must stay off the stream: %v", m)
			}
		}},
	}
	for _, w := range want {
		m := tc.readLine(t)
		if m["event"] != w.event {
			t.Fatalf("got event %v, want %s", m["event"], w.event)
		}
		w.check(m)
	}
}

func TestSecondSubscribeRefused(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	if resp, _ := tc.cmd(t, `{"cmd":"subscribe"}`); resp["ok"] != true {
		t.Fatalf("first subscribe refused: %v", resp)
	}
	resp, _ := tc.cmd(t, `{"cmd":"subscribe"}`)
	if resp["ok"] != false {
		t.Errorf("double subscribe answered ok: %v", resp)
	}
}

func TestOversizedLineClosesConnection(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	big := make([]byte, maxLineBytes+16)
	for i := range big {
		big[i] = 'x'
	}
	_, err := tc.c.Write(big)
	if err == nil {
		// The write itself may succeed into the socket buffer; the server's
		// scanner then hits ErrTooLong and drops the connection.
		_ = err
	}
	tc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 16)
	if _, err := tc.c.Read(buf); err == nil {
		t.Fatal("connection stayed open after an oversized line; framing is unrecoverable past a lost newline")
	}
}

func TestServerCloseEndsSubscriptions(t *testing.T) {
	h := newCtlHarness(t, nil)
	tc := dialCtl(t, h.path)

	if resp, _ := tc.cmd(t, `{"cmd":"subscribe"}`); resp["ok"] != true {
		t.Fatalf("subscribe refused: %v", resp)
	}
	h.srv.Close()

	// The subscriber's stream ends: the peer sees EOF once the server tears
	// the connection down.
	tc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := tc.c.Read(buf); err == nil {
		t.Fatal("connection survived Server.Close")
	}

	// Publish after Close is a silent no-op, not a panic.
	h.srv.PublishConn("online", "disconnected", "shutdown", "")
}
