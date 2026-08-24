package coordinator

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/coordinator/presence"
	"github.com/maleolabs/walkie/internal/coordinator/queue"
	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/messagehub"
	"github.com/maleolabs/walkie/internal/obs"
	"github.com/maleolabs/walkie/internal/store"
)

// Criterion 1, enforced rather than asserted: representative coordinator
// operations run end-to-end while EVERY log line they produce is captured,
// then audited two ways —
//
//  1. STRUCTURE: each line parses as a JSON object carrying time, level and
//     msg, with the level drawn from the fixed four-level scheme
//     (obs/logging.go). Structured means machine-readable, not
//     mostly-readable.
//  2. CONTENT: no canary message body and no encoding of any canary public
//     key appears anywhere in the stream. The canaries ride REAL traffic —
//     routed messages, a held-and-drained offline message, broadcasts, a
//     status label, PublicKeyAnnounce key bytes — i.e. exactly the payloads a
//     debugging body log would leak. Public keys are scanned in base64 and
//     hex because those are the forms a log line could carry; fingerprints
//     are deliberately NOT violations (crypto.Keystore logs fingerprints
//     instead of key bytes, by design).
//
// The rig mirrors startPresenceRig but logs through obs.NewLogger's JSON
// handler at Debug level — the production format, and the most leak-prone
// verbosity, audited together.

func startAuditRig(t *testing.T) (*presenceRig, *obs.Metrics, *queue.Queue) {
	t.Helper()

	logs := &syncBuffer{} // race-safe capture: server goroutines write while the audit reads
	logger := obs.NewLogger(logs, slog.LevelDebug)
	clk := clock.NewFake(rigEpoch)

	st, err := store.Open(t.TempDir()+"/coord.db", clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tr, err := presence.NewTracker(st, clk, e2eTTL, logger)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}

	keys, err := crypto.OpenKeystore(t.TempDir()+"/peers.json", clk, logger)
	if err != nil {
		t.Fatalf("open keystore: %v", err)
	}
	inbox, err := queue.NewSealed(st, clk, 24*time.Hour, 64, logger, queue.NewKeystoreSealer(keys, logger))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}

	metrics := obs.NewMetrics()
	ln := newPipeListener()
	resolver := tsauth.NewStaticResolver(nil)
	srv := NewServer(resolver, clk, logger, tr, inbox)
	srv.WireKeys(keys, nil)
	srv.WireMetrics(metrics)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ctx, ln); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	rig := &presenceRig{resolver: resolver, clk: clk, tracker: tr, logs: logs, ln: ln, st: st, srv: srv, nextIP: 1}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop within grace")
		}
		inbox.Close()
		keys.Close()
		tr.Close()
		st.Close()
	})
	return rig, metrics, inbox
}

// keyEncodings lists the textual forms a leaked key byte string could take.
func keyEncodings(pub []byte) []string {
	return []string{
		base64.StdEncoding.EncodeToString(pub),
		base64.RawStdEncoding.EncodeToString(pub),
		hex.EncodeToString(pub),
		strings.ToUpper(hex.EncodeToString(pub)),
	}
}

func TestLogsCarryNoContentOrKeyMaterial(t *testing.T) {
	rig, _, inbox := startAuditRig(t)

	const (
		dmBody   = "CANARY-dm-body-q7xk92plainsight"
		bcBody   = "CANARY-broadcast-z3m8vaseenbyserver"
		heldBody = "CANARY-held-k9w4ttonlycoordinator"
		statusTx = "CANARY-status-v2n6rdlabel"
	)

	laptopID := mustIdentity(t)
	phoneID := mustIdentity(t)
	laptopPub := laptopID.PublicKey()
	phonePub := phoneID.PublicKey()
	canaries := []string{dmBody, bcBody, heldBody, statusTx}
	canaries = append(canaries, keyEncodings(laptopPub[:])...)
	canaries = append(canaries, keyEncodings(phonePub[:])...)

	// 1. Handshakes + key pins (PublicKeyAnnounce bytes cross the wire).
	laptop := rig.connectDevice(t, laptopName)
	nextDirectory(t, laptop)
	laptop.send(t, announceEnvelope(laptopID.PublicKey()))
	nextDirectory(t, laptop) // refresh proves Authorize ran and logged

	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone)
	phone.send(t, announceEnvelope(phoneID.PublicKey()))
	nextDirectory(t, phone)

	// 2. Routed direct message with a canary body.
	hub := messagehub.New(laptopName, rig.clk, nil)
	env, err := hub.SendDirect(phoneName, dmBody)
	if err != nil {
		t.Fatalf("compose dm: %v", err)
	}
	laptop.send(t, env)
	if got := nextTextEnvelope(t, phone); got.GetDirectMessage() == nil {
		t.Fatalf("phone got %T, want DirectMessage", got.GetPayload())
	}

	// 3. Broadcast with a canary body.
	benv, err := hub.SendBroadcast(bcBody)
	if err != nil {
		t.Fatalf("compose broadcast: %v", err)
	}
	laptop.send(t, benv)
	if got := nextTextEnvelope(t, phone); got.GetBroadcastMessage() == nil {
		t.Fatalf("phone got %T, want BroadcastMessage", got.GetPayload())
	}

	// 4. Status label with canary text; laptop's PresenceUpdate proves the
	// SetStatus path ran.
	phone.send(t, &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PresenceStatusChange{PresenceStatusChange: &walkiev1.PresenceStatusChange{
			Status: statusTx,
		}},
	})
	for {
		got := laptop.nextPresence(t)
		if got.GetDevice() == phoneName && got.GetStatus() == statusTx {
			break
		}
	}

	// 5. Offline hold: laptop disconnects, phone sends it a canary message,
	// the queue seals and holds it. A following heartbeat echo proves the
	// dispatch loop processed the DM (dispatch is sequential per connection).
	laptop.teardown(t)
	holdHub := messagehub.New(phoneName, rig.clk, nil)
	henv, err := holdHub.SendDirect(laptopName, heldBody)
	if err != nil {
		t.Fatalf("compose held dm: %v", err)
	}
	phone.send(t, henv)
	phone.heartbeat(t) // its echo proves the DM above was already dispatched

	// 6. Drain: laptop returns, receives the sealed delivery. The drained
	// frame count lands in a log line here.
	laptop2 := rig.connectDevice(t, laptopName, 0)
	for {
		got := laptop2.nextEnvelope(t)
		if got.GetSealedDelivery() != nil {
			break
		}
	}

	// 7. TTL eviction of whatever remains, observed as the event it is.
	rig.clk.Advance(25 * time.Hour)
	select {
	case <-inbox.Swept():
	case <-time.After(5 * time.Second):
		t.Fatal("no TTL sweep completed; eviction path never ran")
	}

	// --- Audit the whole captured stream. ---

	out := rig.logs.String()
	if out == "" {
		t.Fatal("no logs captured; the audit would pass vacuously")
	}

	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	structured := 0
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %d is not a JSON object (criterion 1): %q", i+1, line)
		}
		msg, _ := record["msg"].(string)
		level, _ := record["level"].(string)
		if msg == "" || level == "" {
			t.Fatalf("log line %d missing msg/level: %q", i+1, line)
		}
		switch level {
		case "DEBUG", "INFO", "WARN", "ERROR":
		default:
			t.Fatalf("log line %d level %q outside the documented scheme: %q", i+1, level, line)
		}
		structured++
	}
	if structured < 10 {
		t.Fatalf("only %d log lines from a full representative workload; audit coverage too thin", structured)
	}

	for _, canary := range canaries {
		if strings.Contains(out, canary) {
			t.Errorf("LOG LEAK: canary %q appears in the captured stream — message content or key material reached a log line", redact(canary))
		}
	}
}

// redact keeps the failure message itself from reprinting the full canary —
// the test output is still a log, and the habit should hold everywhere.
func redact(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8] + "…"
}
