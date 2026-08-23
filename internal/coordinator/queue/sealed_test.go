package queue

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// mustMarshal renders env exactly as Enqueue's marshalOpaque does, for
// assertions that reason about the sealed box's plaintext size.
func mustMarshal(t *testing.T, env *walkiev1.Envelope) []byte {
	t.Helper()
	b, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

// The tests in this file pin ts:queue-sealed-box's at-rest guarantees at the
// byte level, against the PRODUCTION wiring (NewSealed over a real
// crypto.Keystore via NewKeystoreSealer — the exact construction
// cmd/walkie-coordinator wires). Criterion 3 is explicit that decrypt-round-
// trip assertions do NOT satisfy it — a queue that stored plaintext alongside
// ciphertext would round-trip perfectly — so every secrecy assertion here
// reads the STORED BYTES straight out of the database and inspects them as
// bytes.
//
// Since the SealedDelivery carrier landed, the coordinator no longer opens
// anything: Resume ships the still-sealed box and the RECIPIENT opens it.
// The open half of these tests therefore plays the recipient's client role —
// crypto.Open with the recipient's identity key, the operation the future
// client assembly performs on every SealedDelivery frame.

// testKeystore opens a real keystore under a fresh temp dir — the same type,
// file format and pin rules production runs.
func testKeystore(t *testing.T) *crypto.Keystore {
	t.Helper()
	ks, err := crypto.OpenKeystore(t.TempDir()+"/peers.json", testClock(), testLogger(&bytes.Buffer{}))
	if err != nil {
		t.Fatalf("open keystore: %v", err)
	}
	t.Cleanup(func() { ks.Close() })
	return ks
}

// mustPin pins pub for peer through the real TOFU gate (first-seen path).
func mustPin(t *testing.T, ks *crypto.Keystore, peer string, pub [32]byte) {
	t.Helper()
	if err := ks.Authorize(peer, pub, nil); err != nil {
		t.Fatalf("pin %q: %v", peer, err)
	}
}

// storedBody reads one row's body column DIRECTLY from the database — the
// criterion-3 instrument. No queue method involved: if the queue grew a
// plaintext mirror column or rewrote bytes on the way down, this read sees it.
func storedBody(t *testing.T, q *Queue, recipient string, position uint64) []byte {
	t.Helper()
	var body []byte
	if err := q.db.QueryRow(
		`SELECT body FROM queue_inbox WHERE recipient = ? AND position = ?`,
		recipient, int64(position),
	).Scan(&body); err != nil {
		t.Fatalf("read stored body %q pos %d: %v", recipient, position, err)
	}
	return body
}

// tamperStoredBody flips the last ciphertext byte of one row directly in the
// database — simulating storage corruption or a hostile edit at rest.
func tamperStoredBody(t *testing.T, q *Queue, recipient string, position uint64) {
	t.Helper()
	body := storedBody(t, q, recipient, position)
	tampered := append([]byte(nil), body...)
	tampered[len(tampered)-1] ^= 0x01 // last ciphertext byte: AEAD catches it
	if _, err := q.db.Exec(
		`UPDATE queue_inbox SET body = ? WHERE recipient = ? AND position = ?`,
		tampered, recipient, int64(position),
	); err != nil {
		t.Fatalf("tamper stored row: %v", err)
	}
}

// TestStoredBytesAreCiphertextNotPlaintext is criterion 3, verbatim, against
// the PRODUCTION wiring: a queued payload read directly out of the
// coordinator's database is ciphertext after a real enqueue through the
// sealed queue.
//
// Four assertions, all on raw stored bytes:
//
//  1. the plaintext appears NOWHERE in the stored BLOB (a substring check —
//     the strongest cheap form: any plaintext leakage, header to tail, fails);
//  2. the blob carries the sealed marker and matches the sealed-box wire
//     shape behind it: version byte 1, then a 32-byte ephemeral public key,
//     then a 24-byte nonce, then at least the 16-byte Poly1305 tag;
//  3. the negative control: the SAME envelope through the plain queue stores
//     bytes that DO contain the plaintext — proving assertion 1 can actually
//     fail, i.e. this test is not tautological;
//  4. the bootstrap rule at the byte level: an UNPINNED recipient's row
//     rests under the plain marker with readable envelope bytes — honest
//     plaintext-at-rest, never dropped, never encrypted to nothing.
func TestStoredBytesAreCiphertextNotPlaintext(t *testing.T) {
	const secret = "the coordinator must not be able to read this at rest"
	const phone = "phone.tail-scale.ts.net."

	phoneID, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer phoneID.Zero()

	buf := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	ks := testKeystore(t)
	mustPin(t, ks, phone, phoneID.PublicKey())

	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(buf), NewKeystoreSealer(ks, testLogger(buf)))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	env := directEnv("msg-sealed-1", secret, epoch)
	pos := enqueueOK(t, q, phone, env)

	body := storedBody(t, q, phone, pos)

	// 1. No plaintext anywhere in the stored bytes.
	if bytes.Contains(body, []byte(secret)) {
		t.Fatal("stored queue body CONTAINS the plaintext — criterion 3 violated")
	}
	if bytes.Contains(body, []byte(phone)) {
		t.Fatal("stored queue body contains unencrypted envelope metadata")
	}

	// 2. Framing + sealed-box wire shape:
	//    marker(1)=0x01 || version(1)=0x01 || ephPub(32) || nonce(24) || ct+tag.
	const (
		ephPubLen = 32
		nonceLen  = 24
		tagLen    = 16
	)
	framed := 1 + 1 + ephPubLen + nonceLen + tagLen
	if len(body) < framed {
		t.Fatalf("stored body is %d bytes; shorter than the smallest framed sealed box (%d)", len(body), framed)
	}
	if body[0] != storedSealedMarker {
		t.Fatalf("stored body marker = %#x, want %#x (sealed)", body[0], storedSealedMarker)
	}
	if body[1] != 0x01 {
		t.Fatalf("sealed box version byte = %#x, want 0x01", body[1])
	}
	// Ciphertext length accounts for the WHOLE marshalled envelope (identity
	// fields and timestamps included — they are sealed too) plus the tag: the
	// body cannot be a truncated or padded something-else that happens to
	// start with the right version byte.
	wantCT := len(mustMarshal(t, env)) + tagLen
	gotCT := len(body) - (1 + 1 + ephPubLen + nonceLen)
	if gotCT != wantCT {
		t.Fatalf("stored ciphertext length = %d, want marshalled envelope %d + tag %d", gotCT, len(mustMarshal(t, env)), tagLen)
	}

	// 3. Negative control on a PLAINTEXT queue over the same store schema:
	// identical envelope, no seam — the plaintext MUST be visible, proving
	// the assertions above distinguish ciphertext from plaintext.
	plain := mustNewQueue(t, st, testClock(), buf)
	plainPos := enqueueOK(t, plain, "laptop.tail-scale.ts.net.", directEnv("msg-plain-1", secret, epoch))
	plainBody := storedBody(t, plain, "laptop.tail-scale.ts.net.", plainPos)
	if !bytes.Contains(plainBody, []byte(secret)) {
		t.Fatal("negative control failed: plaintext queue does not store readable plaintext — assertions above prove nothing")
	}

	// 4. Bootstrap rule, byte level: a recipient with NO pinned key gets an
	// honestly-plaintext row (plain marker, readable envelope), never a drop
	// and never a box nobody can open.
	unpinnedPos := enqueueOK(t, q, "tablet.tail-scale.ts.net.", directEnv("msg-unpinned-1", secret, epoch))
	unpinnedBody := storedBody(t, q, "tablet.tail-scale.ts.net.", unpinnedPos)
	if unpinnedBody[0] != storedPlainMarker {
		t.Fatalf("unpinned recipient's row marker = %#x, want %#x (honest plaintext-at-rest)", unpinnedBody[0], storedPlainMarker)
	}
	if !bytes.Contains(unpinnedBody, []byte(secret)) {
		t.Fatal("unpinned recipient's row is neither sealed nor plainly stored — undocumented third state")
	}
	if out := buf.String(); !strings.Contains(out, "stored UNSEALED") {
		t.Fatalf("unsealed hold not logged for operator visibility; log:\n%s", out)
	}
}

// TestSealedRowShipsAsSealedDeliveryAndRoundTrips proves the sealed row is
// the ORIGINAL envelope and nothing else — after the recipient opens it. The
// coordinator's Resume ships a SealedDelivery frame carrying the opaque box;
// opening with the recipient's identity key yields the inner envelope
// field-exact — message_id, both timestamps, sender, recipient, body — with
// the position stamped only on the OUTER frame.
func TestSealedRowShipsAsSealedDeliveryAndRoundTrips(t *testing.T) {
	const phone = "phone.tail-scale.ts.net."
	phoneID, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer phoneID.Zero()

	ks := testKeystore(t)
	mustPin(t, ks, phone, phoneID.PublicKey())

	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(&bytes.Buffer{}), NewKeystoreSealer(ks, testLogger(&bytes.Buffer{})))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	env := directEnv("msg-rt-1", "round trip body", epoch)
	pos := enqueueOK(t, q, phone, env)

	got, err := q.Resume(phone, 0)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(got) != 1 || got[0].Position != pos {
		t.Fatalf("resume returned %+v, want exactly position %d", got, pos)
	}
	frame := got[0].Envelope
	sd := frame.GetSealedDelivery()
	if sd == nil {
		t.Fatalf("resume shipped %T, want a SealedDelivery frame", frame.GetPayload())
	}
	if frame.GetPosition() != pos {
		t.Fatalf("outer frame position = %d, want %d (the ack handle rides outside the box)", frame.GetPosition(), pos)
	}

	// THE RECIPIENT'S OPERATION: open the box with its own identity key.
	opened, err := crypto.Open(phoneID, sd.GetCiphertext())
	if err != nil {
		t.Fatalf("recipient could not open the delivered box: %v", err)
	}
	var inner walkiev1.Envelope
	if err := proto.Unmarshal(opened, &inner); err != nil {
		t.Fatalf("unmarshal opened envelope: %v", err)
	}
	d := &inner
	if d.GetMessageId() != "msg-rt-1" ||
		d.GetDirectMessage().GetBody() != "round trip body" ||
		d.GetDirectMessage().GetSender() != "laptop.tail-scale.ts.net." ||
		d.GetDirectMessage().GetRecipient() != "phone.tail-scale.ts.net." {
		t.Fatalf("opened envelope drifted: %+v", d)
	}
	if !d.GetSentAt().AsTime().Equal(epoch) || !d.GetReceivedAt().AsTime().Equal(epoch) {
		t.Fatalf("timestamps not preserved verbatim: sent=%v received=%v",
			d.GetSentAt().AsTime(), d.GetReceivedAt().AsTime())
	}
	if d.GetPosition() != 0 {
		t.Fatalf("inner position = %d, want 0 (position lives on the outer frame only)", d.GetPosition())
	}
}

// TestTamperedRowCrossesVerbatimAndFailsAtRecipient is criterion 4 end to end
// across the new boundary: a bit-flipped stored body crosses the wire EXACTLY
// as stored (the coordinator can neither detect nor repair it — it cannot
// open the box), fails authentication at the RECIPIENT, and yields no
// plaintext — while the healthy sibling rows still deliver and open.
func TestTamperedRowCrossesVerbatimAndFailsAtRecipient(t *testing.T) {
	const phone = "phone.tail-scale.ts.net."
	phoneID, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer phoneID.Zero()

	ks := testKeystore(t)
	mustPin(t, ks, phone, phoneID.PublicKey())

	buf := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(buf), NewKeystoreSealer(ks, testLogger(buf)))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	enqueueOK(t, q, phone, directEnv("msg-good-1", "first intact", epoch))
	badPos := enqueueOK(t, q, phone, directEnv("msg-bad-1", "second tampered", epoch))
	enqueueOK(t, q, phone, directEnv("msg-good-2", "third intact", epoch))

	// Flip one bit INSIDE the ciphertext of the middle row, directly in the
	// database — simulating storage corruption or a hostile edit at rest.
	tamperStoredBody(t, q, phone, badPos)
	wantBytes := append([]byte(nil), storedBody(t, q, phone, badPos)...)[1:] // strip marker: what the frame must carry

	got, err := q.Resume(phone, 0)
	if err != nil {
		t.Fatalf("resume must survive a tampered row: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("resume delivered %d frames, want all 3 (the coordinator cannot tell which box is dead)", len(got))
	}

	// The tampered box crosses VERBATIM — byte-for-byte what rested, minus
	// the storage marker. Any coordinator-side normalization would break the
	// AEAD in ways neither side could attribute.
	var badFrame *walkiev1.SealedDelivery
	for _, d := range got {
		if d.Position == badPos {
			badFrame = d.Envelope.GetSealedDelivery()
		}
	}
	if badFrame == nil {
		t.Fatalf("tampered position %d did not ship", badPos)
	}
	if !bytes.Equal(badFrame.GetCiphertext(), wantBytes) {
		t.Fatal("tampered ciphertext was altered in transit across the resume boundary")
	}

	// At the recipient: authentication FAILS, whole. No plaintext, no partial
	// content, and the failure is the typed auth failure — not a parse error
	// blaming the wrong layer.
	_, err = crypto.Open(phoneID, badFrame.GetCiphertext())
	if !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("tampered box error = %v, want crypto.ErrAuthFailed", err)
	}

	// The healthy siblings still deliver AND open: one dead box must not
	// withhold the rest of the queue.
	for _, d := range got {
		if d.Position == badPos {
			continue
		}
		opened, err := crypto.Open(phoneID, d.Envelope.GetSealedDelivery().GetCiphertext())
		if err != nil {
			t.Fatalf("healthy box at position %d failed to open: %v", d.Position, err)
		}
		var inner walkiev1.Envelope
		if err := proto.Unmarshal(opened, &inner); err != nil {
			t.Fatalf("healthy box at position %d did not decode: %v", d.Position, err)
		}
		if strings.Contains(inner.GetDirectMessage().GetBody(), "tampered") {
			t.Fatal("tampered row content leaked into a delivery")
		}
	}
}

// TestKeyLostRecipientCannotOpenItsRows pins criterion 7's cost as behaviour
// at the new boundary: rows sealed to a key the recipient no longer holds
// cross as ordinary SealedDelivery frames, fail authentication against the
// fresh identity, and yield NOTHING — forfeiture happens at the recipient,
// loudly, never partially.
func TestKeyLostRecipientCannotOpenItsRows(t *testing.T) {
	const phone = "phone.tail-scale.ts.net."
	lostID, err := crypto.GenerateIdentity() // the key that will be lost
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer lostID.Zero()

	ks := testKeystore(t)
	mustPin(t, ks, phone, lostID.PublicKey())

	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(&bytes.Buffer{}), NewKeystoreSealer(ks, testLogger(&bytes.Buffer{})))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	enqueueOK(t, q, phone, directEnv("msg-lost-1", "forfeited", epoch))

	// A re-installed device generated a FRESH identity: it holds A key, not
	// THE key these rows were sealed to.
	fresh, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate fresh identity: %v", err)
	}
	defer fresh.Zero()

	got, err := q.Resume(phone, 0)
	if err != nil {
		t.Fatalf("resume with a lost-key backlog must not error the drain: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("resume delivered %d frames, want 1 (the coordinator ships boxes it cannot judge)", len(got))
	}
	sd := got[0].Envelope.GetSealedDelivery()
	if sd == nil {
		t.Fatalf("expected SealedDelivery frame, got %T", got[0].Envelope.GetPayload())
	}
	_, err = crypto.Open(fresh, sd.GetCiphertext())
	if !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("fresh-key open error = %v, want crypto.ErrAuthFailed (key loss forfeits queued messages)", err)
	}
}

// TestUnknownStorageMarkerSkippedLoudly: a row written by a FUTURE format
// this build cannot speak is skipped whole — logged content-free, never
// delivered, never partially processed — without withholding the rest of the
// resume.
func TestUnknownStorageMarkerSkippedLoudly(t *testing.T) {
	const phone = "phone.tail-scale.ts.net."
	phoneID, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer phoneID.Zero()

	ks := testKeystore(t)
	mustPin(t, ks, phone, phoneID.PublicKey())

	buf := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(buf), NewKeystoreSealer(ks, testLogger(buf)))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	enqueueOK(t, q, phone, directEnv("m-alien", "from the future", epoch))
	enqueueOK(t, q, phone, directEnv("m-known", "readable today", epoch))

	// Rewrite the first row into a format this build does not know.
	if _, err := q.db.Exec(
		`UPDATE queue_inbox SET body = ? WHERE recipient = ? AND position = 1`,
		[]byte{0x7F, 0xDE, 0xAD}, phone,
	); err != nil {
		t.Fatalf("write alien row: %v", err)
	}

	got, err := q.Resume(phone, 0)
	if err != nil {
		t.Fatalf("resume must survive an alien-format row: %v", err)
	}
	if len(got) != 1 || got[0].Position != 2 {
		t.Fatalf("delivered %+v, want exactly position 2 (the alien row skipped whole)", got)
	}
	sd := got[0].Envelope.GetSealedDelivery()
	if sd == nil {
		t.Fatalf("surviving row shipped as %T, want SealedDelivery", got[0].Envelope.GetPayload())
	}
	opened, err := crypto.Open(phoneID, sd.GetCiphertext())
	if err != nil {
		t.Fatalf("surviving row did not open: %v", err)
	}
	var inner walkiev1.Envelope
	if err := proto.Unmarshal(opened, &inner); err != nil {
		t.Fatalf("surviving row did not decode: %v", err)
	}
	if inner.GetMessageId() != "m-known" {
		t.Fatalf("surviving row is %q, want m-known", inner.GetMessageId())
	}
	out := buf.String()
	if !strings.Contains(out, "unknown storage format") || !strings.Contains(out, "m-alien") {
		t.Fatalf("alien-row skip not logged loudly; log:\n%s", out)
	}
	if strings.Contains(out, "from the future") {
		t.Fatalf("log carries message BODY — no-content rule violated; log:\n%s", out)
	}
}

// TestBootstrapRulePinAppliesForwardOnly pins the decided bootstrap rule at
// the behaviour level: rows enqueued BEFORE the recipient's first pin rest
// plain and replay as their ordinary payload shape; rows enqueued AFTER seal.
// A later pin never retroactively seals earlier rows.
func TestBootstrapRulePinAppliesForwardOnly(t *testing.T) {
	const phone = "phone.tail-scale.ts.net."
	phoneID, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	defer phoneID.Zero()

	ks := testKeystore(t) // nothing pinned yet
	buf := &bytes.Buffer{}
	sealLog := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(buf), NewKeystoreSealer(ks, testLogger(sealLog)))
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	beforePos := enqueueOK(t, q, phone, directEnv("msg-before-pin", "queued pre-announce", epoch))

	mustPin(t, ks, phone, phoneID.PublicKey()) // the announce lands
	afterPos := enqueueOK(t, q, phone, directEnv("msg-after-pin", "queued post-announce", epoch))

	if storedBody(t, q, phone, beforePos)[0] != storedPlainMarker {
		t.Fatal("pre-pin row was re-sealed retroactively — bootstrap rule violated")
	}
	if storedBody(t, q, phone, afterPos)[0] != storedSealedMarker {
		t.Fatal("post-pin row was not sealed")
	}

	got, err := q.Resume(phone, 0)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("resume delivered %d frames, want 2", len(got))
	}
	if dm := got[0].Envelope.GetDirectMessage(); dm == nil || dm.GetBody() != "queued pre-announce" {
		t.Fatalf("pre-pin row replayed as %T, want the ordinary DirectMessage shape", got[0].Envelope.GetPayload())
	}
	if sd := got[1].Envelope.GetSealedDelivery(); sd == nil {
		t.Fatalf("post-pin row replayed as %T, want SealedDelivery", got[1].Envelope.GetPayload())
	} else if _, err := crypto.Open(phoneID, sd.GetCiphertext()); err != nil {
		t.Fatalf("post-pin box did not open with the recipient's key: %v", err)
	}
}

// TestNilAtRestRefusedInNewSealed: asking for encrypted-at-rest and silently
// getting plaintext would be the quietest possible security bug; the
// constructor refuses instead.
func TestNilAtRestRefusedInNewSealed(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	if _, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(&bytes.Buffer{}), nil); err == nil {
		t.Fatal("NewSealed accepted a nil AtRest — would have built a plaintext queue silently")
	}
}

// compile-time guard: the production sealer satisfies the seam.
var _ AtRest = (*keystoreSealer)(nil)
