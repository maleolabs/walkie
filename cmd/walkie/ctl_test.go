package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/control"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/presenceview"
)

// The control-socket adapter (ctl.go): what cmd/walkie ADDS to the generic
// ctlsocket server. The protocol itself is proven in internal/ctlsocket;
// here we prove the local validation, the error mapping and the metadata-only
// event wiring.

func TestSendStatusRefusesOversizedBeforeAnyWireByte(t *testing.T) {
	fx := newFixture(t)

	err := sendStatus(fx.client, strings.Repeat("é", 128)+"!") // 257 bytes
	if err == nil {
		t.Fatal("256-byte bound not enforced locally")
	}
	if len(fx.client.sent) != 0 {
		t.Fatalf("refused draft still reached the transport: %d envelopes", len(fx.client.sent))
	}
}

func TestSendStatusRefusesNonUTF8(t *testing.T) {
	fx := newFixture(t)

	err := sendStatus(fx.client, "\xff\xfe")
	if err == nil {
		t.Fatal("non-UTF-8 status accepted")
	}
	if len(fx.client.sent) != 0 {
		t.Fatalf("invalid status reached the transport: %d envelopes", len(fx.client.sent))
	}
}

func TestSendStatusSendsPresenceStatusChange(t *testing.T) {
	fx := newFixture(t)

	if err := sendStatus(fx.client, "deploying"); err != nil {
		t.Fatalf("sendStatus: %v", err)
	}
	if len(fx.client.sent) != 1 {
		t.Fatalf("sent %d envelopes, want 1", len(fx.client.sent))
	}
	payload, ok := fx.client.sent[0].Payload.(*walkiev1.Envelope_PresenceStatusChange)
	if !ok {
		t.Fatalf("payload = %T, want PresenceStatusChange", fx.client.sent[0].Payload)
	}
	if got := payload.PresenceStatusChange.GetStatus(); got != "deploying" {
		t.Errorf("status on the wire = %q, want %q", got, "deploying")
	}

	// Empty clears: same payload shape, empty string.
	if err := sendStatus(fx.client, ""); err != nil {
		t.Fatalf("sendStatus(clear): %v", err)
	}
	if len(fx.client.sent) != 2 {
		t.Fatalf("clear did not send; sent = %d", len(fx.client.sent))
	}
}

func TestSendStatusOfflineNamesTheFix(t *testing.T) {
	fx := newFixture(t)
	fx.client.sendErr = control.ErrOffline

	err := sendStatus(fx.client, "brb")
	if err == nil || !strings.Contains(err.Error(), "offline") {
		t.Fatalf("offline error = %v, want a message naming the offline condition", err)
	}
	if errors.Is(err, control.ErrOffline) && !strings.Contains(err.Error(), "retry") {
		t.Errorf("error should tell the script what to do next: %v", err)
	}
}

func TestDeviceToEntryConversion(t *testing.T) {
	seen := time.Date(2026, 8, 24, 1, 2, 3, 500000000, time.FixedZone("+0400", 4*3600))

	entry := deviceToEntry(presenceview.Device{
		Device:   "beta",
		Online:   false,
		LastSeen: seen.UnixNano(),
		Status:   "brb",
	})
	if entry.Device != "beta" || entry.Online || entry.Status != "brb" {
		t.Errorf("entry = %+v", entry)
	}
	// Timestamps cross the control socket as UTC RFC3339Nano regardless of
	// the zone they arrived in — sortable as text, no epoch arithmetic.
	// 01:02:03.5 at +04:00 is 21:02:03.5Z the day before; trailing zeros are
	// trimmed by RFC3339Nano.
	want := "2026-08-23T21:02:03.5Z"
	if entry.LastSeen != want {
		t.Errorf("last_seen = %q, want %q", entry.LastSeen, want)
	}

	online := deviceToEntry(presenceview.Device{Device: "alpha", Online: true})
	if online.LastSeen != "" {
		t.Errorf("online device carried last_seen %q; the JSON must omit it like the wire does", online.LastSeen)
	}
}

// The full wiring, in process: a filed message reaches a subscribed script as
// a message event carrying METADATA ONLY — no body field can exist, because
// PublishMessage has no body parameter to receive one.
func TestFiledMessageReachesSubscribersWithoutContent(t *testing.T) {
	fx := newFixture(t)
	fx.client.identity = "self"

	mach, err := control.NewMachine(clock.NewFake(testEpoch), nil)
	if err != nil {
		t.Fatalf("machine: %v", err)
	}
	defer mach.Close()
	presence := presenceview.New(nil)

	path := filepath.Join(t.TempDir(), "walkie.sock")
	srv, err := startControlSocket(path, ctlDeps{
		app:      fx.app,
		client:   fx.client,
		mach:     mach,
		presence: presence,
	}, nil)
	if err != nil {
		t.Fatalf("startControlSocket: %v", err)
	}
	defer srv.Close()

	tc := dialLoopback(t, path)
	tc.writeLine(t, `{"cmd":"subscribe"}`)
	drainSnapshotLines(t, tc)

	// Wire the hook exactly as runClient does, then file a message through
	// the same notify() path OnEnvelope and SendFromUI use.
	fx.app.SetOnFiled(func(msg message.Message) {
		srv.PublishMessage(msg.ID, msg.Sender, msg.Recipient,
			message.ConversationKeyFor(fx.client.ConnectedAs(), msg))
	})
	fx.app.notify(message.Message{
		ID:        "01JTESTMSG",
		Sender:    "peer",
		Recipient: fx.client.identity,
		Body:      "SECRET BODY THAT MUST NOT TRAVEL",
	})

	ev := tc.readLine(t)
	if ev["event"] != "message" {
		t.Fatalf("got %v, want a message event", ev)
	}
	if ev["id"] != "01JTESTMSG" || ev["conversation"] != message.ConversationKey("peer") {
		t.Errorf("metadata wrong: %v", ev)
	}
	if _, has := ev["body"]; has {
		t.Errorf("message event carries a body field: %v", ev)
	}
}

// --- small in-process protocol client for the adapter tests ---

type loopClient struct {
	c    net.Conn
	scan *bufio.Scanner
}

func dialLoopback(t *testing.T, path string) *loopClient {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	scan := bufio.NewScanner(conn)
	return &loopClient{c: conn, scan: scan}
}

func (lc *loopClient) writeLine(t *testing.T, line string) {
	t.Helper()
	if _, err := lc.c.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
}

func (lc *loopClient) readLine(t *testing.T) map[string]any {
	t.Helper()
	type result struct {
		line map[string]any
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		if !lc.scan.Scan() {
			ch <- result{err: lc.scan.Err()}
			return
		}
		var m map[string]any
		if err := json.Unmarshal(lc.scan.Bytes(), &m); err != nil {
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
		t.Fatal("timed out waiting for a line")
		return nil
	}
}

// drainSnapshotLines consumes the subscribe response plus the seeding
// snapshot in whatever order they arrived.
func drainSnapshotLines(t *testing.T, lc *loopClient) {
	t.Helper()
	gotResp, gotSnap := false, false
	for !gotResp || !gotSnap {
		m := lc.readLine(t)
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
