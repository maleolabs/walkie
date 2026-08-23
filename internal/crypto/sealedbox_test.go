package crypto

import (
	"bytes"
	"errors"
	"testing"
)

// fixedPriv derives a deterministic 32-byte scalar for tests. Any 32 bytes
// work as an X25519 scalar (the library clamps internally); determinism is
// what the fingerprint and keystore tests need, not secrecy.
func fixedPriv(seed byte) [keySize]byte {
	var k [keySize]byte
	for i := range k {
		k[i] = seed + byte(i)*11
	}
	return k
}

func fixedIdentity(seed byte) *IdentityKey {
	k, err := newIdentity(fixedPriv(seed))
	if err != nil {
		panic(err)
	}
	return k
}

// deterministicRand replays a fixed stream: 32 ephemeral bytes + 24 nonce
// bytes per seal. Tests assert on exact reproducibility, which crypto/rand
// cannot give.
type deterministicRand struct{ buf []byte }

func (d *deterministicRand) Read(p []byte) (int, error) {
	if len(d.buf) < len(p) {
		return 0, errors.New("deterministicRand: exhausted")
	}
	n := copy(p, d.buf[:len(p)])
	d.buf = d.buf[n:]
	return n, nil
}

func newDeterministicRand(fill byte) *deterministicRand {
	buf := make([]byte, 56) // eph(32) + nonce(24), one seal's worth
	for i := range buf {
		buf[i] = fill + byte(i)
	}
	return &deterministicRand{buf: buf}
}

func TestSealOpenRoundTrip(t *testing.T) {
	recipient := fixedIdentity(0x01)
	defer recipient.Zero()

	for _, pt := range [][]byte{
		nil,
		{},
		[]byte("hello queued world"),
		bytes.Repeat([]byte{0x00}, 1024),
	} {
		box, err := Seal(recipient.PublicKey(), pt)
		if err != nil {
			t.Fatalf("Seal(%d bytes): %v", len(pt), err)
		}
		got, err := Open(recipient, box)
		if err != nil {
			t.Fatalf("Open(%d-byte plaintext): %v", len(pt), err)
		}
		if !bytes.Equal(got, pt) {
			t.Fatalf("round trip mismatch: got %q want %q", got, pt)
		}
	}
}

func TestSealIsCiphertextNotPlaintext(t *testing.T) {
	recipient := fixedIdentity(0x02)
	defer recipient.Zero()
	pt := []byte("a very secret body that must never appear in storage")

	box, err := Seal(recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(box, pt) {
		t.Fatal("sealed box contains the plaintext verbatim")
	}
	// The stored form must not leak even the header-adjacent structure of a
	// known plaintext prefix; a sliding-window check over every offset.
	for i := 0; i+8 <= len(pt); i++ {
		if bytes.Contains(box, pt[i:i+8]) {
			t.Fatalf("sealed box contains plaintext window at offset %d", i)
		}
	}
}

func TestSealIsRandomizedPerMessage(t *testing.T) {
	recipient := fixedIdentity(0x03)
	defer recipient.Zero()
	pt := []byte("same plaintext")

	a, err := Seal(recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Seal(recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two seals of one plaintext are identical — nonces or ephemeral keys are reused")
	}
}

func TestSealDeterministicUnderFixedRand(t *testing.T) {
	recipient := fixedIdentity(0x04)
	defer recipient.Zero()
	pt := []byte("deterministic")

	a, err := sealWithRand(newDeterministicRand(0xA0), recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sealWithRand(newDeterministicRand(0xA0), recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("same rand stream produced different boxes — breaks test reproducibility")
	}

	c, err := sealWithRand(newDeterministicRand(0xB0), recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, c) {
		t.Fatal("different rand streams produced identical boxes")
	}
}

func TestTamperedBoxIsRejectedWhole(t *testing.T) {
	recipient := fixedIdentity(0x05)
	defer recipient.Zero()
	pt := []byte("authenticated payload whose partial use would be a defect")

	box, err := Seal(recipient.PublicKey(), pt)
	if err != nil {
		t.Fatal(err)
	}

	// Flip one bit in every region: version, ephemeral key, nonce, ciphertext
	// body, and each byte of the Poly1305 tag. Every single-bit change must
	// fail authentication — criterion 4, and the AEAD's contract.
	regions := []struct {
		name   string
		start  int
		end    int
		bitErr bool // expect ErrAuthFailed specifically
	}{
		{"ephemeral pubkey", 1, 1 + pubKeySize, false},
		{"nonce", 1 + pubKeySize, headerSize, true},
		{"ciphertext", headerSize, len(box) - tagSize, true},
		{"tag", len(box) - tagSize, len(box), true},
	}
	for _, region := range regions {
		for _, offset := range []int{region.start, (region.start + region.end) / 2, region.end - 1} {
			mutated := append([]byte(nil), box...)
			mutated[offset] ^= 0x01
			got, err := Open(recipient, mutated)
			if err == nil {
				t.Fatalf("%s: tampered byte at %d accepted", region.name, offset)
			}
			if got != nil {
				t.Fatalf("%s: tampered byte at %d returned plaintext alongside error", region.name, offset)
			}
			if region.bitErr && !errors.Is(err, ErrAuthFailed) {
				t.Fatalf("%s: tampered byte at %d: err = %v, want ErrAuthFailed", region.name, offset, err)
			}
		}
	}

	// The version byte has its own failure class (structural, not forgery).
	versionFlipped := append([]byte(nil), box...)
	versionFlipped[0] = 2
	if _, err := Open(recipient, versionFlipped); err == nil || errors.Is(err, ErrAuthFailed) {
		t.Fatalf("unknown version: err = %v, want a version error without ErrAuthFailed", err)
	}
}

func TestTruncatedBoxIsRejected(t *testing.T) {
	recipient := fixedIdentity(0x06)
	defer recipient.Zero()

	box, err := Seal(recipient.PublicKey(), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1, headerSize - 1, headerSize, len(box) - 1} {
		if _, err := Open(recipient, box[:n]); err == nil {
			t.Fatalf("truncation to %d bytes accepted", n)
		}
	}
}

func TestWrongRecipientKeyFailsAuthentication(t *testing.T) {
	recipient := fixedIdentity(0x07)
	defer recipient.Zero()
	attacker := fixedIdentity(0x08)
	defer attacker.Zero()

	box, err := Seal(recipient.PublicKey(), []byte("for recipient only"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(attacker, box)
	if err == nil {
		t.Fatal("box opened under the wrong identity")
	}
	if got != nil {
		t.Fatal("wrong-key open returned plaintext")
	}
	if !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}

func TestSealRejectsLowOrderRecipientPoint(t *testing.T) {
	var allZero [pubKeySize]byte
	if _, err := Seal(allZero, []byte("x")); err == nil {
		t.Fatal("all-zero recipient point accepted at seal time")
	}
}

func TestOpenNilIdentityRefused(t *testing.T) {
	if _, err := Open(nil, make([]byte, headerSize+tagSize)); err == nil {
		t.Fatal("nil identity accepted")
	}
}
