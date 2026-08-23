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
// byte level. Criterion 3 is explicit that decrypt-round-trip assertions do
// NOT satisfy it — a queue that stored plaintext alongside ciphertext would
// round-trip perfectly — so every assertion here reads the STORED BYTES
// straight out of the database and inspects them as bytes.

// recipientKeys is a test AtRest built from the real construction
// (crypto.Seal/crypto.Open against pinned X25519 keys): sealing looks each
// recipient's key up exactly the way the coordinator's keystore wiring will,
// opening uses the recipient's identity private key — the operation that
// belongs to the recipient once the drain ships sealed boxes.
type recipientKeys struct {
	pinned map[string][32]byte            // what a coordinator keystore holds: public only
	holds  map[string]*crypto.IdentityKey // who can actually open
}

func newRecipientKeys() *recipientKeys {
	return &recipientKeys{
		pinned: map[string][32]byte{},
		holds:  map[string]*crypto.IdentityKey{},
	}
}

func (rk *recipientKeys) enroll(recipient string) *crypto.IdentityKey {
	t := recipient // keep the map key honest
	id, err := crypto.GenerateIdentity()
	if err != nil {
		panic(err) // entropy failure: nothing downstream is meaningful
	}
	rk.pinned[t] = id.PublicKey()
	rk.holds[t] = id
	return id
}

func (rk *recipientKeys) Seal(recipient string, marshalled []byte) ([]byte, error) {
	pub, ok := rk.pinned[recipient]
	if !ok {
		return nil, errors.New("no pinned key for recipient")
	}
	return crypto.Seal(pub, marshalled)
}

func (rk *recipientKeys) Open(recipient string, stored []byte) ([]byte, error) {
	id, ok := rk.holds[recipient]
	if !ok {
		return nil, errors.New("recipient identity not held")
	}
	return crypto.Open(id, stored)
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

// TestStoredBytesAreCiphertextNotPlaintext is criterion 3, verbatim: a queued
// payload read directly out of the coordinator's database is ciphertext.
//
// Three assertions, all on raw stored bytes:
//
//  1. the plaintext appears NOWHERE in the stored BLOB (a substring check —
//     the strongest cheap form: any plaintext leakage, header to tail, fails);
//  2. the blob matches the sealed-box wire shape: version byte 1, then a
//     32-byte ephemeral public key, then a 24-byte nonce, then at least the
//     16-byte Poly1305 tag of ciphertext;
//  3. the negative control: the SAME envelope through the plain queue stores
//     bytes that DO contain the plaintext — proving assertion 1 can actually
//     fail, i.e. this test is not tautological.
func TestStoredBytesAreCiphertextNotPlaintext(t *testing.T) {
	const secret = "the coordinator must not be able to read this at rest"
	rk := newRecipientKeys()
	rk.enroll("phone.tail-scale.ts.net.")

	buf := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(buf), rk)
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	env := directEnv("msg-sealed-1", secret, epoch)
	pos := enqueueOK(t, q, "phone.tail-scale.ts.net.", env)

	body := storedBody(t, q, "phone.tail-scale.ts.net.", pos)

	// 1. No plaintext anywhere in the stored bytes.
	if bytes.Contains(body, []byte(secret)) {
		t.Fatal("stored queue body CONTAINS the plaintext — criterion 3 violated")
	}
	if bytes.Contains(body, []byte("phone.tail-scale.ts.net.")) {
		t.Fatal("stored queue body contains unencrypted envelope metadata")
	}

	// 2. Sealed-box wire shape: version || ephPub(32) || nonce(24) || ct+tag.
	const (
		wantVersion = byte(1)
		ephPubLen   = 32
		nonceLen    = 24
		tagLen      = 16
	)
	if len(body) < 1+ephPubLen+nonceLen+tagLen {
		t.Fatalf("stored body is %d bytes; shorter than the smallest sealed box", len(body))
	}
	if body[0] != wantVersion {
		t.Fatalf("stored body version byte = %#x, want %#x (sealed box v1)", body[0], wantVersion)
	}
	// Ciphertext length accounts for the WHOLE marshalled envelope (identity
	// fields and timestamps included — they are sealed too) plus the tag: the
	// body cannot be a truncated or padded something-else that happens to
	// start with the right version byte.
	wantCT := len(mustMarshal(t, env)) + tagLen
	gotCT := len(body) - (1 + ephPubLen + nonceLen)
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
}

// TestSealedRowRoundTripsToOriginalEnvelope proves the sealed row is the
// ORIGINAL envelope and nothing else: opened with the recipient's identity
// key it comes back field-exact — message_id, both timestamps, sender,
// recipient, body — with the position stamped only by Resume. This is the
// correctness half behind criterion 3's secrecy half.
func TestSealedRowRoundTripsToOriginalEnvelope(t *testing.T) {
	rk := newRecipientKeys()
	rk.enroll("phone.tail-scale.ts.net.")

	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(&bytes.Buffer{}), rk)
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	env := directEnv("msg-rt-1", "round trip body", epoch)
	pos := enqueueOK(t, q, "phone.tail-scale.ts.net.", env)

	got, err := q.Resume("phone.tail-scale.ts.net.", 0)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(got) != 1 || got[0].Position != pos {
		t.Fatalf("resume returned %+v, want exactly position %d", got, pos)
	}
	d := got[0].Envelope
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
	if d.GetPosition() != pos {
		t.Fatalf("replay stamp: position = %d, want %d", d.GetPosition(), pos)
	}
}

// TestTamperedRowRejectedWholeNotPartiallyProcessed is criterion 4 end to end
// at the storage boundary: a bit-flipped stored body fails authentication and
// is rejected WHOLE — skipped loudly, never delivered, never partially
// processed — while the healthy sibling row still delivers.
func TestTamperedRowRejectedWholeNotPartiallyProcessed(t *testing.T) {
	rk := newRecipientKeys()
	rk.enroll("phone.tail-scale.ts.net.")

	buf := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(buf), rk)
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	enqueueOK(t, q, "phone.tail-scale.ts.net.", directEnv("msg-good-1", "first intact", epoch))
	badPos := enqueueOK(t, q, "phone.tail-scale.ts.net.", directEnv("msg-bad-1", "second tampered", epoch))
	enqueueOK(t, q, "phone.tail-scale.ts.net.", directEnv("msg-good-2", "third intact", epoch))

	// Flip one bit INSIDE the ciphertext of the middle row, directly in the
	// database — simulating storage corruption or a hostile edit at rest.
	body := storedBody(t, q, "phone.tail-scale.ts.net.", badPos)
	tampered := append([]byte(nil), body...)
	tampered[len(tampered)-1] ^= 0x01 // last ciphertext byte: AEAD catches it
	if _, err := q.db.Exec(
		`UPDATE queue_inbox SET body = ? WHERE recipient = ? AND position = ?`,
		tampered, "phone.tail-scale.ts.net.", int64(badPos),
	); err != nil {
		t.Fatalf("tamper stored row: %v", err)
	}

	got, err := q.Resume("phone.tail-scale.ts.net.", 0)
	if err != nil {
		t.Fatalf("resume must survive a tampered row: %v", err)
	}
	// Exactly the two healthy rows deliver; the tampered one is gone from
	// the delivery set WHOLE — no partial content ever surfaced.
	var ids []string
	for _, d := range got {
		ids = append(ids, d.Envelope.GetMessageId())
	}
	if len(ids) != 2 || ids[0] != "msg-good-1" || ids[1] != "msg-good-2" {
		t.Fatalf("delivered %v, want exactly [msg-good-1 msg-good-2] — tampered row must be skipped whole", ids)
	}
	for _, d := range got {
		if strings.Contains(d.Envelope.GetDirectMessage().GetBody(), "tampered") {
			t.Fatal("tampered row content leaked into a delivery")
		}
	}

	// Loud: the drop is logged with the row's identity, content-free.
	out := buf.String()
	if !strings.Contains(out, "undecryptable") || !strings.Contains(out, "msg-bad-1") {
		t.Fatalf("tampered-row drop not logged loudly; log:\n%s", out)
	}
	if strings.Contains(out, "second tampered") {
		t.Fatalf("log carries message BODY — no-content rule violated; log:\n%s", out)
	}
}

// TestRowsDroppedWhenRecipientKeyLost pins criterion 7's cost as behaviour:
// rows sealed to a key the recipient no longer holds are undecryptable
// forever, are dropped deliberately (loudly, never partially processed), and
// do not block the resume machinery itself.
func TestRowsDroppedWhenRecipientKeyLost(t *testing.T) {
	rk := newRecipientKeys()
	rk.enroll("phone.tail-scale.ts.net.") // key later lost — never enrolled into the opener

	st := mustOpenStore(t, t.TempDir()+"/q.db", testClock())
	q, err := NewSealed(st, testClock(), testTTL, testMaxSize, testLogger(&bytes.Buffer{}), rk)
	if err != nil {
		t.Fatalf("new sealed queue: %v", err)
	}
	defer q.Close()

	enqueueOK(t, q, "phone.tail-scale.ts.net.", directEnv("msg-lost-1", "forfeited", epoch))

	// A re-installed device generated a FRESH identity: it holds A key, not
	// THE key these rows were sealed to.
	fresh, err := crypto.GenerateIdentity()
	if err != nil {
		t.Fatalf("generate fresh identity: %v", err)
	}
	defer fresh.Zero()
	rk.holds["phone.tail-scale.ts.net."] = fresh

	got, err := q.Resume("phone.tail-scale.ts.net.", 0)
	if err != nil {
		t.Fatalf("resume with wrong key must not error the drain: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("wrong-key resume delivered %d messages, want 0 (key loss forfeits queued messages)", len(got))
	}
	if _, err := q.Resume("phone.tail-scale.ts.net.", 0); err != nil {
		t.Fatalf("resume machinery itself must stay healthy after drops: %v", err)
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

// compile-time guard: the test AtRest satisfies the seam.
var _ AtRest = (*recipientKeys)(nil)
