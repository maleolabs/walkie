package crypto

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadOrCreateIdentityCreatesWithOwnerOnlyPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "identity.key")

	k, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentity: %v", err)
	}
	defer k.Zero()

	// Permissions are asserted with os.Stat, not trusted from the code path
	// (same rule as history/store tests): the promise is about bytes on disk.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("key file mode = %#o, want 0600", got)
	}
	di, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat key dir: %v", err)
	}
	if got := di.Mode().Perm(); got != 0o700 {
		t.Fatalf("key dir mode = %#o, want 0700", got)
	}
}

func TestLoadOrCreateIdentityTightensPreExistingLooseDirAndFile(t *testing.T) {
	dir := t.TempDir()
	keyDir := filepath.Join(dir, "loose")
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(keyDir, "identity.key")
	// A pre-existing loose file with valid content: write one via a first
	// LoadOrCreate, loosen both levels, then reload and require tightening.
	first, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("seed identity: %v", err)
	}
	defer first.Zero()
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(keyDir, 0o755); err != nil {
		t.Fatal(err)
	}

	second, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("reload identity: %v", err)
	}
	defer second.Zero()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("pre-existing loose file mode = %#o, want tightened 0600", got)
	}
	di, _ := os.Stat(keyDir)
	if got := di.Mode().Perm(); got != 0o700 {
		t.Errorf("pre-existing loose dir mode = %#o, want tightened 0700", got)
	}
	// Tightening must not have replaced the key with a fresh one.
	if second.PublicKey() != first.PublicKey() {
		t.Error("reload changed the public key")
	}
}

func TestLoadOrCreateIdentityIsStableAcrossRuns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.key")

	first, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	defer first.Zero()

	second, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	defer second.Zero()

	if first.PublicKey() != second.PublicKey() {
		t.Fatal("identity changed between runs — first-run generation is not persisting")
	}
}

func TestLoadOrCreateIdentityRejectsForeignAndTruncatedFiles(t *testing.T) {
	dir := t.TempDir()

	t.Run("not an identity file", func(t *testing.T) {
		path := filepath.Join(dir, "foreign.key")
		// Right size, wrong header: must fail on the header check.
		if err := os.WriteFile(path, bytes.Repeat([]byte{0xAB}, len(identityMagic)+keySize), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadOrCreateIdentity(path); err == nil {
			t.Fatal("expected refusal of a non-identity file, got a key")
		} else if !strings.Contains(err.Error(), "bad header") {
			t.Errorf("error should name the structural problem, got: %v", err)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		path := filepath.Join(dir, "truncated.key")
		if err := os.WriteFile(path, identityMagic[:], 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadOrCreateIdentity(path); err == nil {
			t.Fatal("expected refusal of a truncated key file")
		}
	})
}

func TestGenerateIdentityProducesUsableDistinctKeys(t *testing.T) {
	a, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	defer a.Zero()
	b, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	defer b.Zero()

	if a.PublicKey() == b.PublicKey() {
		t.Fatal("two generated identities share a public key")
	}
	var zeroPub [pubKeySize]byte
	if a.PublicKey() == zeroPub {
		t.Fatal("generated public key is all zeros")
	}
}

func TestParsePublicKeyRejectsWrongLength(t *testing.T) {
	if _, err := ParsePublicKey(bytes.Repeat([]byte{1}, 31)); err == nil {
		t.Error("31 bytes accepted")
	}
	if _, err := ParsePublicKey(bytes.Repeat([]byte{1}, 33)); err == nil {
		t.Error("33 bytes accepted")
	}
	pub, err := ParsePublicKey(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatalf("32 bytes rejected: %v", err)
	}
	for i, b := range pub {
		if b != 7 {
			t.Fatalf("byte %d = %d, want 7", i, b)
		}
	}
}
