package crypto

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

// The sealed box: the one construction adr:004-security-model approves for
// application-level encryption, applied to exactly the coordinator's two
// blind spots — messages resting in the offline queue, and (phase 2, not
// built here) payloads on the relay-fallback path. Direct live traffic is
// deliberately NOT sealed; it relies on WireGuard, and a second layer inside
// the tunnel was rejected by the ADR as defence-in-depth theatre.
//
// # Construction, stated exactly
//
//	ephemeral X25519 keypair per message          (curve25519, x/crypto)
//	shared := X25519(ephemeral, recipient)        (X25519 key agreement)
//	key     := HKDF-SHA256(shared,
//	                  salt = ephemeral_pub || recipient_pub,
//	                  info = "walkie/sealedbox/v1") (hkdf, x/crypto)
//	box     := version || ephemeral_pub || nonce(24) ||
//	           XChaCha20-Poly1305(key, nonce, plaintext)
//
// Every primitive is taken from the audited standard extension libraries.
// What little composition exists uses each primitive exactly as documented:
//
//   - HKDF-SHA256 is the KDF — a standard construction from x/crypto, not a
//     hand-rolled hash chain. The salt binds the derived key to BOTH public
//     keys, so a ciphertext is cryptographically bound to its key pair (the
//     same role libsodium's sealed box gives its keyed hash); info pins the
//     construction version so a future format change cannot silently reuse
//     old keys with new semantics.
//   - The nonce is 24 fresh random bytes per message from crypto/rand — the
//     exact usage chacha20poly1305.NewX documents for its random-nonce mode.
//     No counters, no invented scheme. At fleet messaging rates the random-
//     nonce collision probability is negligible by orders of magnitude.
//   - Authentication comes from the Poly1305 tag inside the AEAD. There is NO
//     signature scheme: the ADR names the AEAD as the authenticator, and
//     adding signatures would add a second key type to manage for no threat
//     the model recognises.
//
// The recipient's key never needs to be online or secret-sharing with the
// sender: anyone who knows a peer's public key can seal to it; only the
// holder of the private half can open. That asymmetry is what lets the
// coordinator store queued messages it cannot read.

const (
	// sealedBoxVersion is the leading format byte. It exists so a future
	// construction change (key rotation above all — adr:004 consequence 4)
	// can be introduced without ambiguity about which rules a given box
	// follows. Open refuses anything else rather than guessing.
	sealedBoxVersion byte = 1

	nonceSizeX = chacha20poly1305.NonceSizeX // 24
	tagSize    = chacha20poly1305.Overhead   // 16

	// headerSize: version byte + ephemeral public key + nonce.
	headerSize = 1 + pubKeySize + nonceSizeX

	sealedBoxInfo = "walkie/sealedbox/v1"
)

// ErrAuthFailed reports that a sealed box did not authenticate: the ciphertext,
// the tag, or the implied key does not match. Callers MUST treat this as a
// rejection of the WHOLE box — Open returns no plaintext on failure, and no
// caller may act on partial content. Tampering and wrong-recipient are
// indistinguishable by design (both are "not for you / not intact").
var ErrAuthFailed = errors.New("crypto: sealed box failed authentication")

// Seal encrypts plaintext to the holder of recipient's private key.
//
// The result is safe to hand to the coordinator for storage: it is
// ciphertext end to end, and criterion 3 depends on that — the queue row must
// be inspectably NOT the plaintext. Uses crypto/rand internally; tests drive
// the unexported sealWithRand for determinism.
func Seal(recipient [pubKeySize]byte, plaintext []byte) ([]byte, error) {
	return sealWithRand(rand.Reader, recipient, plaintext)
}

func sealWithRand(r io.Reader, recipient [pubKeySize]byte, plaintext []byte) ([]byte, error) {
	var eph [keySize]byte
	if _, err := io.ReadFull(r, eph[:]); err != nil {
		return nil, fmt.Errorf("crypto: seal: ephemeral entropy: %w", err)
	}
	defer zero(eph[:])

	ephPub, err := curve25519.X25519(eph[:], curve25519.Basepoint)
	if err != nil {
		return nil, fmt.Errorf("crypto: seal: ephemeral key: %w", err)
	}
	shared, err := curve25519.X25519(eph[:], recipient[:])
	if err != nil {
		// A low-order recipient point would produce an all-zero-ish shared
		// secret; the library refuses it here. Refusing at seal time keeps a
		// malformed pinned key from ever producing a box nobody can open.
		return nil, fmt.Errorf("crypto: seal: recipient key rejected: %w", err)
	}
	defer zero(shared)

	key, err := deriveSealedBoxKey(shared, ephPub, recipient[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: seal: derive key: %w", err)
	}
	defer zero(key)

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: seal: aead: %w", err)
	}

	var nonce [nonceSizeX]byte
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		return nil, fmt.Errorf("crypto: seal: nonce entropy: %w", err)
	}

	out := make([]byte, 0, headerSize+len(plaintext)+tagSize)
	out = append(out, sealedBoxVersion)
	out = append(out, ephPub...)
	out = append(out, nonce[:]...)
	return aead.Seal(out, nonce[:], plaintext, nil), nil
}

// Open authenticates and decrypts a sealed box against identity.
//
// On ANY failure — truncation, unknown version, tampered bytes, wrong key —
// the returned plaintext is nil and the error wraps [ErrAuthFailed] where the
// failure is cryptographic. Rejection is whole-box by construction: the AEAD
// releases plaintext only after the tag verifies, so there is no code path
// that yields partial content (criterion 4).
func Open(identity *IdentityKey, sealed []byte) ([]byte, error) {
	if identity == nil {
		return nil, errors.New("crypto: open: identity must not be nil")
	}
	if len(sealed) < headerSize+tagSize {
		return nil, fmt.Errorf("crypto: open: truncated box (%d bytes, want >= %d)", len(sealed), headerSize+tagSize)
	}
	if sealed[0] != sealedBoxVersion {
		return nil, fmt.Errorf("crypto: open: unsupported format version %d", sealed[0])
	}

	ephPub := sealed[1 : 1+pubKeySize]
	nonce := sealed[1+pubKeySize : headerSize]
	body := sealed[headerSize:]

	shared, err := curve25519.X25519(identity.priv[:], ephPub)
	if err != nil {
		// Malformed or low-order ephemeral point: structural garbage, not a
		// forgery attempt against a real box.
		return nil, fmt.Errorf("crypto: open: ephemeral key rejected: %w", err)
	}
	defer zero(shared)

	key, err := deriveSealedBoxKey(shared, ephPub, identity.pub[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: open: derive key: %w", err)
	}
	defer zero(key)

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: open: aead: %w", err)
	}

	pt, err := aead.Open(nil, nonce, body, nil)
	if err != nil {
		// Deliberately opaque: whether the tag failed because of tampering,
		// truncation-with-valid-header or a different key, the answer to the
		// caller is the same — reject whole. Details stay here, and nothing
		// about the key material leaks into the error.
		return nil, fmt.Errorf("%w", ErrAuthFailed)
	}
	return pt, nil
}

// deriveSealedBoxKey runs HKDF-SHA256 over the X25519 shared secret. See the
// construction comment at the top of the file for why salt and info carry
// what they carry.
func deriveSealedBoxKey(shared, ephPub, recipientPub []byte) ([]byte, error) {
	salt := make([]byte, 0, len(ephPub)+len(recipientPub))
	salt = append(salt, ephPub...)
	salt = append(salt, recipientPub...)

	out := make([]byte, keySize)
	// hkdf.New returns a stream reader; reading keySize bytes from it cannot
	// fail short of an unreachable internal state, but the error is checked
	// rather than assumed away.
	if _, err := io.ReadFull(hkdf.New(sha256.New, shared, salt, []byte(sealedBoxInfo)), out); err != nil {
		return nil, err
	}
	return out, nil
}
