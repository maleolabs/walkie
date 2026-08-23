package coordinator

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/coordinator/queue"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/messagehub"
)

// The tests in this file drive ts:queue-sealed-box END-TO-END through the
// real server on the presence rig: real PublicKeyAnnounce frames pinned by a
// real keystore, real PublicKeyDirectory snapshots on the wire, and the
// sealed offline-queue flow — hold, ciphertext at rest (asserted at the byte
// level in the database), resume, decrypt. The changed-key flow is tested in
// BOTH branches: refused (headless default) and explicitly confirmed.

// wireKeys attaches a keystore to the rig's server before any client
// connects, mirroring wireQueue's wiring discipline.
func wireKeys(t *testing.T, rig *presenceRig, ks *crypto.Keystore, confirm crypto.ConfirmFunc) {
	t.Helper()
	rig.srv.WireKeys(ks, confirm)
}

// mustKeystore opens a keystore under a fresh temp dir on the rig's clock,
// logging into the RIG's captured buffer — the changed-key warnings criterion
// 5 demands are keystore log lines, so they must land where the tests (and
// the operator) read them.
func mustKeystore(t *testing.T, rig *presenceRig) *crypto.Keystore {
	t.Helper()
	ks, err := crypto.OpenKeystore(t.TempDir()+"/peers.json", rig.clk, slog.New(slog.NewTextHandler(rig.logs, nil)))
	if err != nil {
		t.Fatalf("open keystore: %v", err)
	}
	t.Cleanup(func() { ks.Close() })
	return ks
}

// mustIdentity generates one device identity key for a test.
func mustIdentity(t *testing.T) *crypto.IdentityKey {
	t.Helper()
	id, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	t.Cleanup(id.Zero)
	return id
}

// announceEnvelope builds the client→coordinator publish frame.
func announceEnvelope(pub [32]byte) *walkiev1.Envelope {
	key := pub // copy before escaping to the heap
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PublicKeyAnnounce{PublicKeyAnnounce: &walkiev1.PublicKeyAnnounce{
			PublicKey: append([]byte(nil), key[:]...),
		}},
	}
}

// nextDirectory returns the next PublicKeyDirectory frame, skipping the one
// asynchronous frame kind that legitimately interleaves on another device's
// stream (PresenceUpdates from the presence pump). Everything else must be
// the directory, or the test's model of the wire is wrong and it says so.
func nextDirectory(t *testing.T, c *testClient) *walkiev1.PublicKeyDirectory {
	t.Helper()
	for {
		env := c.nextEnvelope(t)
		if env.GetPresenceUpdate() != nil {
			continue
		}
		if dir := env.GetPublicKeyDirectory(); dir != nil {
			return dir
		}
		t.Fatalf("%s: expected PublicKeyDirectory, got %T", c.name, env.GetPayload())
	}
}

// directoryEntry finds one device's entry, reporting whether it exists.
func directoryEntry(dir *walkiev1.PublicKeyDirectory, device string) ([]byte, bool) {
	for _, e := range dir.GetEntries() {
		if e.GetDevice() == device {
			return e.GetPublicKey(), true
		}
	}
	return nil, false
}

// TestAnnouncePinsAndDirectoryServes is criterion 2 over the wire: the first
// key seen for a peer is pinned; the pin is served to clients as a directory
// snapshot (at hello and after each change); re-announcing the SAME key is
// idempotent and emits nothing.
//
// Frame discipline: on a keys-wired rig every HelloAck is followed by the
// hello-time directory snapshot, and every ACCEPTED announce is followed by
// exactly one refresh. Consuming them in order makes each assertion read a
// causally-closed window — no sleeps, no races.
func TestAnnouncePinsAndDirectoryServes(t *testing.T) {
	rig := startPresenceRig(t)
	wireKeys(t, rig, mustKeystore(t, rig), nil)

	laptop := rig.connectDevice(t, laptopName)
	nextDirectory(t, laptop) // hello-time snapshot (empty: nothing pinned yet)
	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone) // hello-time snapshot (still empty)
	phoneID := mustIdentity(t)

	// First announce: pinned, and every live connection (announcer included)
	// receives the fresh snapshot.
	phone.send(t, announceEnvelope(phoneID.PublicKey()))
	wantPhone := phoneID.PublicKey()
	dir := nextDirectory(t, phone)
	got, ok := directoryEntry(dir, phoneName)
	if !ok || !bytes.Equal(got, wantPhone[:]) {
		t.Fatalf("directory after first announce: phone entry present=%v match=%v; want the announced key", ok, ok && bytes.Equal(got, wantPhone[:]))
	}
	if _, ok := directoryEntry(dir, laptopName); ok {
		t.Fatal("directory contains an entry for a device that never announced")
	}
	nextDirectory(t, laptop) // the refresh reached the other connection too

	// Pinned means PINNED: the keystore holds exactly this key for phone.
	// Causally safe to read: the refresh frame above was written AFTER the
	// pin persisted (dispatch is sequential in the server's read loop).
	pinned, ok := rig.srv.keys.PinnedKey(phoneName)
	if !ok || pinned != phoneID.PublicKey() {
		t.Fatalf("keystore pin mismatch: want the announced key (ok=%v)", ok)
	}

	// Idempotent re-announce: no refusal, no new snapshot. Causally proven
	// with a follow-up Hello probe — whose ack is itself followed by the
	// hello-time snapshot, which must carry the SAME single entry (a
	// re-announce neither adds nor duplicates).
	phone.send(t, announceEnvelope(phoneID.PublicKey()))
	phone.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	dir = nextDirectory(t, phone)
	if n := len(dir.GetEntries()); n != 1 {
		t.Fatalf("post-re-announce snapshot has %d entries, want exactly 1 (idempotent)", n)
	}

	// Snapshot-at-hello: a device connecting NOW gets the full table right
	// after its handshake, without announcing anything itself.
	tablet := rig.connectDevice(t, tabletName)
	dir = nextDirectory(t, tablet)
	if _, ok := directoryEntry(dir, phoneName); !ok {
		t.Fatalf("hello-time snapshot missing phone's pin: %d entries", len(dir.GetEntries()))
	}
}

// TestChangedKeyRefusedHeadless is criterion 5's default branch, end to end:
// a known peer announces a DIFFERENT key; the coordinator warns loudly,
// REFUSES (headless: nil confirmer), tells the announcer its key did not
// take, keeps serving the OLD pin, and never accepts the new one silently.
func TestChangedKeyRefusedHeadless(t *testing.T) {
	rig := startPresenceRig(t)
	wireKeys(t, rig, mustKeystore(t, rig), nil) // nil confirm: headless refusal

	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone) // hello-time snapshot (empty)
	oldID := mustIdentity(t)
	newID := mustIdentity(t)

	phone.send(t, announceEnvelope(oldID.PublicKey()))
	nextDirectory(t, phone) // pin landed; refresh consumed
	wantOld := oldID.PublicKey()
	wantNew := newID.PublicKey()

	// The changed-key announce: loud warning + refusal in the log, and a
	// structured ProtocolError back to the announcer naming fingerprints.
	// The error is written AFTER both log lines (sequential handler), so
	// receiving it causally guarantees the log check below is complete.
	phone.send(t, announceEnvelope(newID.PublicKey()))
	var protoErr *walkiev1.ProtocolError
	for {
		env := phone.nextEnvelope(t)
		if pe := env.GetProtocolError(); pe != nil {
			protoErr = pe
			break
		}
	}
	if protoErr.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED {
		t.Fatalf("refusal code = %v, want MALFORMED", protoErr.GetCode())
	}
	if !strings.Contains(protoErr.GetDetail(), "refused") ||
		!strings.Contains(protoErr.GetDetail(), "operator confirmation") {
		t.Fatalf("refusal detail should name the refusal and the confirmation duty: %q", protoErr.GetDetail())
	}
	if strings.Contains(protoErr.GetDetail(), string(wantNew[:])) {
		t.Fatal("refusal detail carries raw key bytes — no-key-material rule violated")
	}

	// The pin stands: still the OLD key. Silent acceptance would show up
	// here as the new key having replaced it.
	pinned, ok := rig.srv.keys.PinnedKey(phoneName)
	if !ok || pinned != oldID.PublicKey() {
		t.Fatalf("pin after refused change moved (ok=%v) — silent acceptance is the named defect", ok)
	}

	// Loud, in the log: warning AND refusal, fingerprints only.
	out := rig.logs.String()
	if !strings.Contains(out, "PEER KEY CHANGED") {
		t.Fatalf("no loud changed-key warning; log:\n%s", out)
	}
	if !strings.Contains(out, "REFUSED") {
		t.Fatalf("refusal not logged; log:\n%s", out)
	}
	if strings.Contains(out, string(wantNew[:])) ||
		strings.Contains(out, string(wantOld[:])) {
		t.Fatalf("log carries raw key bytes — no-key-material rule violated; log:\n%s", out)
	}
}

// TestChangedKeyConfirmedRepins is criterion 5's explicit branch: with a
// human confirmer wired, the same changed-key announce RE-PINS — and the
// confirmation had to survive a fingerprint comparison first.
func TestChangedKeyConfirmedRepins(t *testing.T) {
	rig := startPresenceRig(t)
	ks := mustKeystore(t, rig)

	oldID := mustIdentity(t)
	newID := mustIdentity(t)

	// The gate a real operator walks: compare BOTH fingerprints out of band
	// before answering. The test asserts the request carried exactly those
	// fingerprints — the confirmer decides on display values, never bytes.
	asked := false
	confirm := func(req crypto.KeyChangeRequest) bool {
		asked = true
		return req.Peer == phoneName &&
			req.OldFingerprint == crypto.Fingerprint(oldID.PublicKey()) &&
			req.NewFingerprint == crypto.Fingerprint(newID.PublicKey())
	}
	wireKeys(t, rig, ks, confirm)

	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone) // hello-time snapshot (empty)
	phone.send(t, announceEnvelope(oldID.PublicKey()))
	nextDirectory(t, phone) // pin landed; refresh consumed

	phone.send(t, announceEnvelope(newID.PublicKey()))
	dir := nextDirectory(t, phone) // confirmed change refreshes the fleet's view
	if _, ok := directoryEntry(dir, phoneName); !ok {
		t.Fatal("post-confirmation snapshot lost phone's entry")
	}

	if !asked {
		t.Fatal("confirmation gate was never consulted — silent acceptance path taken")
	}
	pinned, ok := rig.srv.keys.PinnedKey(phoneName)
	if !ok || pinned != newID.PublicKey() {
		t.Fatalf("pin after confirmed change = the wrong key (ok=%v)", ok)
	}

	if out := rig.logs.String(); !strings.Contains(out, "CONFIRMED by operator") {
		t.Fatalf("confirmed change not logged; log:\n%s", out)
	}
}

// wireSealedQueue builds a SEALED queue over the rig's store — through the
// production sealer (queue.NewKeystoreSealer over the rig's own keystore) —
// wired as the server's offline sink before any client connects.
func wireSealedQueue(t *testing.T, rig *presenceRig, ks *crypto.Keystore) *queue.Queue {
	t.Helper()
	q, err := queue.NewSealed(rig.st, rig.clk, time.Hour, 64, slog.New(slog.NewTextHandler(rig.logs, nil)), queue.NewKeystoreSealer(ks, slog.New(slog.NewTextHandler(rig.logs, nil))))
	if err != nil {
		t.Fatalf("wire sealed queue: %v", err)
	}
	rig.srv.offline = q
	t.Cleanup(q.Close)
	return q
}

// identityHub builds the recipient-side seam with its device key wired: what
// the assembled client will run on every inbound envelope (OnEnvelope →
// Apply → ack). The hub plays the recipient in these tests because the
// interactive client is not assembled yet; the operations are exactly its.
// Its logger writes into the RIG's buffer, because the loud-drop lines
// criterion 4 demands are client-side now — they must land where the tests
// (and an operator) read them.
func identityHub(t *testing.T, rig *presenceRig, name string, id *crypto.IdentityKey) *messagehub.Hub {
	t.Helper()
	hub := messagehub.New(name, rig.clk, slog.New(slog.NewTextHandler(rig.logs, nil)))
	hub.UseIdentity(id)
	return hub
}

// nextSealedDelivery returns the next SealedDelivery frame, skipping the one
// asynchronous frame kind that legitimately interleaves (PresenceUpdates).
// Anything else means the test's model of the drain is wrong and it says so.
func nextSealedDelivery(t *testing.T, c *testClient) *walkiev1.Envelope {
	t.Helper()
	for {
		env := c.nextEnvelope(t)
		if env.GetPresenceUpdate() != nil {
			continue
		}
		if sd := env.GetSealedDelivery(); sd != nil {
			return env
		}
		t.Fatalf("%s: expected SealedDelivery, got %T", c.name, env.GetPayload())
	}
}

// TestSealedQueueEndToEndOfflineDelivery is the item's headline flow end to
// end through the real server, on the FLIPPED production wiring: A sends to
// offline B; the ROW in the coordinator's database is ciphertext (asserted at
// the byte level); B reconnects and receives an opaque SealedDelivery frame;
// B's client opens it with its OWN identity key and files the original
// message intact. The coordinator never holds plaintext on any step of this
// path after ingress.
func TestSealedQueueEndToEndOfflineDelivery(t *testing.T) {
	rig := startPresenceRig(t)
	ks := mustKeystore(t, rig)

	phoneID := mustIdentity(t)
	wireKeys(t, rig, ks, nil)
	wireSealedQueue(t, rig, ks)
	phoneHub := identityHub(t, rig, phoneName, phoneID)

	// B comes online just long enough to publish its key, then dies.
	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone) // hello-time snapshot (empty)
	phone.send(t, announceEnvelope(phoneID.PublicKey()))
	nextDirectory(t, phone) // pin landed
	phone.teardown(t)

	laptop := rig.connectDevice(t, laptopName)
	nextDirectory(t, laptop) // hello-time snapshot (has phone's pin now)
	laptop.assertSilent(t)

	// A sends to offline B: held by the sealed queue.
	id := message.NewID(rig.clk.Now())
	laptop.send(t, directEnvelope(id, phoneName, "sealed secret payload", rigEpoch))
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)
	if out := rig.logs.String(); !strings.Contains(out, "handed to offline sink") {
		t.Fatalf("message not held; log:\n%s", out)
	}

	// CRITERION 3, at the byte level, against the PRODUCTION wiring: read
	// the row straight out of the database. No plaintext anywhere in it, and
	// it carries the framed sealed-box shape (marker 0x01, version byte
	// 0x01). A round-trip assertion cannot say this; raw bytes can.
	var body []byte
	if err := rig.st.DB().QueryRow(
		`SELECT body FROM queue_inbox WHERE recipient = ?`, phoneName,
	).Scan(&body); err != nil {
		t.Fatalf("read queued row: %v", err)
	}
	if bytes.Contains(body, []byte("sealed secret payload")) ||
		bytes.Contains(body, []byte(id)) {
		t.Fatal("stored queue row CONTAINS plaintext — criterion 3 violated")
	}
	if len(body) < 1+1+32+24+16 || body[0] != 0x01 || body[1] != 0x01 {
		t.Fatalf("stored row does not match the framed sealed-box shape (marker=%#x version=%#x len=%d)", body[0], body[1], len(body))
	}

	// B reconnects: exactly one delivery, and it is a SEALED frame — the
	// coordinator ships the box it cannot open. (handleHello's order is
	// HelloAck, then the promised deliveries, then the key-directory
	// snapshot.)
	phone2 := rig.connectDevice(t, phoneName)
	if got := phone2.helloAck.GetPendingCount(); got != 1 {
		t.Fatalf("pending_count = %d, want 1", got)
	}
	frame := nextSealedDelivery(t, phone2)
	if frame.GetPosition() != 1 {
		t.Fatalf("delivery position = %d, want 1 (the ack handle rides outside the box)", frame.GetPosition())
	}

	// THE RECIPIENT'S CLIENT OPENS IT: same call the assembled client makes
	// from OnEnvelope. The inner envelope must be the ORIGINAL message —
	// same ULID, same sender attribution, same ingress received_at, same
	// body.
	msg, displayed := phoneHub.Apply(frame)
	if !displayed {
		t.Fatal("recipient's hub refused a box addressed to its own key")
	}
	if msg.ID != id || msg.Sender != laptopName || msg.Body != "sealed secret payload" {
		t.Fatalf("decrypted delivery drifted: %+v", msg)
	}
	if !msg.ReceivedAt.Equal(rigEpoch) {
		t.Fatalf("received_at rewritten across seal/open: %v", msg.ReceivedAt)
	}

	// Ack closes the loop: retention drops the row. The probe Hello's ack
	// (followed by its own directory snapshot, parked in pending) proves
	// the ack was processed before the count below runs.
	phone2.send(t, &walkiev1.Envelope{Payload: &walkiev1.Envelope_QueueAck{
		QueueAck: &walkiev1.QueueAck{AcknowledgedPosition: 1},
	}})
	phone2.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	phone2.awaitHelloAck(t)
	nextDirectory(t, phone2)
	var rows int
	if err := rig.st.DB().QueryRow(
		`SELECT COUNT(*) FROM queue_inbox WHERE recipient = ?`, phoneName,
	).Scan(&rows); err != nil {
		t.Fatalf("count rows after ack: %v", err)
	}
	if rows != 0 {
		t.Fatalf("%d rows survive the ack, want 0", rows)
	}
}

// TestTamperedRowDropsLoudlyAtRecipient is criterion 4 across the flipped
// boundary, end to end: a stored row tampered with WHILE the recipient is
// offline crosses as a SealedDelivery frame carrying the damaged box, fails
// authentication in the recipient's client, files NOTHING, and the loud drop
// is logged content-free — while the healthy sibling message still delivers
// and displays.
func TestTamperedRowDropsLoudlyAtRecipient(t *testing.T) {
	rig := startPresenceRig(t)
	ks := mustKeystore(t, rig)

	phoneID := mustIdentity(t)
	wireKeys(t, rig, ks, nil)
	wireSealedQueue(t, rig, ks)
	phoneHub := identityHub(t, rig, phoneName, phoneID)

	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone)
	phone.send(t, announceEnvelope(phoneID.PublicKey()))
	nextDirectory(t, phone)
	phone.teardown(t)

	laptop := rig.connectDevice(t, laptopName)
	nextDirectory(t, laptop)

	// Two messages held: the middle one will be corrupted at rest.
	laptop.send(t, directEnvelope(message.NewID(rig.clk.Now()), phoneName, "first intact", rigEpoch))
	badID := message.NewID(rig.clk.Now())
	laptop.send(t, directEnvelope(badID, phoneName, "second tampered", rigEpoch))
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)

	// Hostile edit at rest: flip one bit inside the middle row's ciphertext,
	// directly in the database.
	var body []byte
	if err := rig.st.DB().QueryRow(
		`SELECT body FROM queue_inbox WHERE recipient = ? AND position = 2`, phoneName,
	).Scan(&body); err != nil {
		t.Fatalf("read queued row: %v", err)
	}
	tampered := append([]byte(nil), body...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := rig.st.DB().Exec(
		`UPDATE queue_inbox SET body = ? WHERE recipient = ? AND position = 2`,
		tampered, phoneName,
	); err != nil {
		t.Fatalf("tamper stored row: %v", err)
	}

	// B reconnects: BOTH frames ship (the coordinator cannot tell which box
	// is dead), position 1 first.
	phone2 := rig.connectDevice(t, phoneName)
	if got := phone2.helloAck.GetPendingCount(); got != 2 {
		t.Fatalf("pending_count = %d, want 2", got)
	}
	good := nextSealedDelivery(t, phone2)
	if good.GetPosition() != 1 {
		t.Fatalf("first delivery position = %d, want 1", good.GetPosition())
	}
	bad := nextSealedDelivery(t, phone2)
	if bad.GetPosition() != 2 {
		t.Fatalf("second delivery position = %d, want 2", bad.GetPosition())
	}

	// The healthy box opens and files; the tampered box drops WHOLE — no
	// partial content, nothing filed under the tampered ULID.
	msg, displayed := phoneHub.Apply(good)
	if !displayed || msg.Body != "first intact" {
		t.Fatalf("healthy delivery drifted: %+v displayed=%v", msg, displayed)
	}
	msg, displayed = phoneHub.Apply(bad)
	if displayed || msg != (message.Message{}) {
		t.Fatalf("tampered box produced output %+v — must drop whole", msg)
	}
	if filed := phoneHub.Conversation(message.ConversationKey(laptopName)); len(filed) != 1 {
		t.Fatalf("conversation holds %d messages, want exactly the healthy one", len(filed))
	}

	// Loud, content-free: the drop names the position and reason class,
	// never the body, never bytes.
	laptop.send(t, helloEnvelope(walkiev1.MaxProtocolVersion))
	laptop.awaitHelloAck(t)
	out := rig.logs.String()
	if !strings.Contains(out, "UNREADABLE") {
		t.Fatalf("tampered-row drop not logged loudly; log:\n%s", out)
	}
	if strings.Contains(out, "second tampered") {
		t.Fatalf("log carries message BODY — no-content rule violated; log:\n%s", out)
	}
}

// TestLiveTrafficStaysPlaintext is the scope guard, verified on the wire:
// with the sealed queue AND keystore fully wired, a direct message between
// two ONLINE devices crosses exactly as sent — adr:004 keeps live traffic on
// WireGuard alone, and encrypting it here would be the scope violation the
// brief names as a defect. By construction the live path never touches the
// crypto package: handleDirect/handleBroadcast build and write stamped
// envelopes directly; this test pins that nothing started sneaking bytes
// through them.
func TestLiveTrafficStaysPlaintext(t *testing.T) {
	rig := startPresenceRig(t)
	ks := mustKeystore(t, rig)
	wireKeys(t, rig, ks, nil)
	wireSealedQueue(t, rig, ks)

	laptop := rig.connectDevice(t, laptopName)
	nextDirectory(t, laptop)
	phone := rig.connectDevice(t, phoneName)
	nextDirectory(t, phone)

	const live = "live traffic reads exactly as typed"
	id := message.NewID(rig.clk.Now())
	laptop.send(t, directEnvelope(id, phoneName, live, rigEpoch))

	env := phone.nextEnvelope(t)
	dm := env.GetDirectMessage()
	if dm == nil {
		t.Fatalf("expected live DirectMessage, got %T", env.GetPayload())
	}
	if dm.GetBody() != live {
		t.Fatalf("LIVE traffic was transformed: body %q, want verbatim %q — scope violation", dm.GetBody(), live)
	}
	if env.GetPosition() != 0 {
		t.Fatalf("live delivery carries queue position %d, want 0 (live marker)", env.GetPosition())
	}

	// And nothing rested: an online delivery creates no queue rows to seal.
	var rows int
	if err := rig.st.DB().QueryRow(
		`SELECT COUNT(*) FROM queue_inbox`,
	).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("%d queue rows exist after a purely-live exchange, want 0", rows)
	}
}
