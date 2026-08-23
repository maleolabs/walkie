package crypto

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/crypto/curve25519"
)

// pubKeySize is the size of an X25519 public key, and keySize of its private
// counterpart. Both are fixed by the curve; they exist as named constants so
// wire-parsing code asserts intent instead of scattering 32s.
const (
	pubKeySize = 32
	keySize    = 32
)

// fileMode and dirMode are the permissions enforced on the identity key file
// and its parent directory on EVERY open.
//
// Why enforced rather than set once at creation: os.OpenFile's mode is
// filtered by the process umask, so a "0600 at creation" promise silently
// degrades on hosts with a permissive umask. os.Chmod is not umask-filtered,
// so applying it on every open both creates owner-only and TIGHTENS any
// pre-existing over-permissive file or directory — including one created by
// an older walkie version or another tool. Same discipline as store.Open's
// fileMode and history.Open's dirMode; see those comments for the full
// reasoning.
//
// Why it matters here specifically: the private key IS the defence for
// everything queued for this device (adr:004-security-model scopes queue-at-
// rest protection to the sealed box, and forbids encrypting the key itself —
// that would require a password, and adr:004 puts no password anywhere in the
// system). File permissions are therefore part of the real control, not
// ceremony.
const (
	fileMode os.FileMode = 0o600
	dirMode  os.FileMode = 0o700
)

// identityMagic prefixes the identity key file. Why a header at all: a bare
// 32-byte file is indistinguishable from any other 32-byte file, so a
// mispointed path would load garbage as a key and fail only much later, in
// ways that look like corruption elsewhere. The magic makes that mistake fail
// HERE, loudly, before the key is used for anything.
//
// The v1 suffix leaves room for a future format change (key rotation is a
// recorded gap, adr:004 consequence 4 — when it arrives it will need exactly
// this seam).
var identityMagic = []byte("walkie-x25519-v1\x00")

// IdentityKey is a device's long-lived X25519 keypair: the identity that
// queued messages are sealed to, generated on first run and persisted with
// owner-only permissions (criterion 1).
//
// It is the ONLY long-term secret this package owns. Everything else — pins,
// fingerprints, sealed boxes — derives from public bytes.
type IdentityKey struct {
	priv [keySize]byte
	pub  [pubKeySize]byte
}

// GenerateIdentity creates a fresh X25519 keypair from crypto/rand.
//
// No entropy source parameter is exported deliberately: callers cannot pass
// anything but the system CSPRNG, which is the only correct choice for a
// long-term identity. Tests use the unexported generateIdentity hook.
func GenerateIdentity() (*IdentityKey, error) {
	return generateIdentity(rand.Reader)
}

func generateIdentity(r io.Reader) (*IdentityKey, error) {
	var priv [keySize]byte
	if _, err := io.ReadFull(r, priv[:]); err != nil {
		return nil, fmt.Errorf("crypto: generate identity: read entropy: %w", err)
	}
	k, err := newIdentity(priv)
	if err != nil {
		zero(priv[:])
		return nil, err
	}
	return k, nil
}

// newIdentity derives the public half and validates the private half.
//
// curve25519.X25519 applies the standard clamping internally, so any 32
// random bytes are a usable scalar; the call below is where a degenerate
// scalar would surface, which is why it runs at construction rather than at
// first use.
func newIdentity(priv [keySize]byte) (*IdentityKey, error) {
	pub, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	if err != nil {
		return nil, fmt.Errorf("crypto: derive public key: %w", err)
	}
	k := &IdentityKey{priv: priv}
	copy(k.pub[:], pub)
	return k, nil
}

// PublicKey returns a copy of the public key. Safe to publish, log about (via
// [Fingerprint]), and distribute through the coordinator's key directory.
func (k *IdentityKey) PublicKey() [pubKeySize]byte {
	return k.pub
}

// Zero best-effort wipes the private key from this process's memory.
//
// Best-effort is the honest word: Go gives no control over GC copies, stack
// spills or swap. This clears the canonical copy so a later misuse of a
// supposedly-discarded key fails fast, and it is called on shutdown paths;
// it is NOT a guarantee against a determined local attacker, who is out of
// scope behind the file permissions and the tailnet boundary (adr:004).
func (k *IdentityKey) Zero() {
	zero(k.priv[:])
}

// LoadOrCreateIdentity returns the device's identity key, generating and
// persisting it on first run (criterion 1) and loading it on every later run.
//
// Permission promises, enforced on EVERY call — see fileMode/dirMode for why
// enforcement beats creation-time modes:
//
//   - the key file is 0600 (a pre-existing looser file is tightened);
//   - the parent directory is 0700 (a pre-existing looser directory is
//     tightened — a 0600 file under 0755 is weaker than it looks, because
//     anyone who can traverse the directory can attack the file through
//     programs running as them).
//
// The write on first run is atomic (temp file + rename): a crash mid-write
// must not leave a truncated key file, because a half-written identity is
// silently a DIFFERENT identity after the next run regenerates it — and
// criterion 7's cost attaches to exactly that event. Every message queued for
// the lost key becomes undecryptable.
//
// The key is stored unencrypted BY DESIGN: encrypting it would require a
// passphrase, and adr:004 puts no password anywhere in walkie. The control is
// owner-only permissions plus the tailnet boundary, and the accepted loss
// (stolen disk reads the key) is recorded in the ADR alongside the identical
// acceptance for unencrypted local history.
func LoadOrCreateIdentity(path string) (*IdentityKey, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("crypto: resolve identity path %s: %w", path, err)
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("crypto: create %s: %w", dir, err)
	}
	// MkdirAll's mode is umask-filtered; Chmod is not. Enforcing on every
	// open also tightens a pre-existing loose directory — same reasoning as
	// history.Open and store.Open.
	if err := os.Chmod(dir, dirMode); err != nil {
		return nil, fmt.Errorf("crypto: enforce %#o on %s: %w", dirMode, dir, err)
	}

	data, err := os.ReadFile(abs)
	if err == nil {
		k, err := parseIdentityFile(data)
		if err != nil {
			return nil, fmt.Errorf("crypto: identity %s: %w", abs, err)
		}
		// Tighten a pre-existing loose file (created by an older build or a
		// careless manual copy). Chmod is not umask-filtered, so this is a
		// real correction, not a hopeful default.
		if err := os.Chmod(abs, fileMode); err != nil {
			return nil, fmt.Errorf("crypto: enforce %#o on %s: %w", fileMode, abs, err)
		}
		return k, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("crypto: read identity %s: %w", abs, err)
	}

	k, err := GenerateIdentity()
	if err != nil {
		return nil, err
	}
	if err := writeIdentityFile(abs, k); err != nil {
		return nil, err
	}
	return k, nil
}

// writeIdentityFile persists k atomically: temp file in the same directory
// (same filesystem, so rename is atomic), fsynced, then renamed over the
// target. See LoadOrCreateIdentity for why atomicity is a correctness
// requirement and not polish.
func writeIdentityFile(abs string, k *IdentityKey) error {
	dir := filepath.Dir(abs)
	f, err := os.CreateTemp(dir, ".identity-*")
	if err != nil {
		return fmt.Errorf("crypto: create temp identity in %s: %w", dir, err)
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op after successful rename

	// CreateTemp already uses 0600, but it is subject to umask like any
	// other creation; Chmod makes the promise unconditional.
	if err := f.Chmod(fileMode); err != nil {
		f.Close()
		return fmt.Errorf("crypto: enforce %#o on %s: %w", fileMode, tmp, err)
	}
	buf := make([]byte, 0, len(identityMagic)+keySize)
	buf = append(buf, identityMagic...)
	buf = append(buf, k.priv[:]...)
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("crypto: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("crypto: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("crypto: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, abs); err != nil {
		return fmt.Errorf("crypto: rename %s to %s: %w", tmp, abs, err)
	}
	return nil
}

// parseIdentityFile validates the magic header and length, then loads the
// key. Failure modes are structural (wrong file, truncated file) and are
// reported without echoing file contents — error paths never carry key
// material.
func parseIdentityFile(data []byte) (*IdentityKey, error) {
	if len(data) != len(identityMagic)+keySize {
		return nil, fmt.Errorf("wrong size: got %d bytes, want %d", len(data), len(identityMagic)+keySize)
	}
	if string(data[:len(identityMagic)]) != string(identityMagic) {
		return nil, errors.New("not a walkie identity key file (bad header)")
	}
	var priv [keySize]byte
	copy(priv[:], data[len(identityMagic):])
	return newIdentity(priv)
}

// ParsePublicKey converts a wire-form public key (the raw 32 bytes carried by
// PublicKeyAnnounce / PublicKeyDirectory in the control-plane schema) into the
// array form the sealing and pinning APIs take. Length is validated here so
// every downstream site can assume a well-formed key.
func ParsePublicKey(b []byte) ([pubKeySize]byte, error) {
	var pub [pubKeySize]byte
	if len(b) != pubKeySize {
		return pub, fmt.Errorf("crypto: public key must be %d bytes, got %d", pubKeySize, len(b))
	}
	copy(pub[:], b)
	return pub, nil
}

// zero overwrites b. Small enough that the compiler does not get clever; used
// on private scalars and derived secrets immediately after last use.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
