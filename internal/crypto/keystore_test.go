package crypto

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

func testClock() *clock.Fake {
	return clock.NewFake(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
}

func openTestKeystore(t *testing.T, dir string) *Keystore {
	t.Helper()
	ks, err := OpenKeystore(filepath.Join(dir, "peers.json"), testClock(), nil)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	t.Cleanup(func() { ks.Close() })
	return ks
}

func TestFirstContactPinsKey(t *testing.T) {
	dir := t.TempDir()
	ks := openTestKeystore(t, dir)

	peer := fixedIdentity(0x10).PublicKey()

	// A nil ConfirmFunc on FIRST contact must still pin: TOFU pins the first
	// key seen; the confirmation gate exists only for CHANGES.
	if err := ks.Authorize("alice", peer, nil); err != nil {
		t.Fatalf("first-contact authorize refused: %v", err)
	}
	got, ok := ks.PinnedKey("alice")
	if !ok || got != peer {
		t.Fatalf("pinned key = %v (ok=%v), want the presented key", got, ok)
	}

	// The pin survives a reopen — persistence is the whole point of a pin.
	reopened, err := OpenKeystore(filepath.Join(dir, "peers.json"), testClock(), nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, ok = reopened.PinnedKey("alice")
	if !ok || got != peer {
		t.Fatalf("after reopen: pinned key = %v (ok=%v), want the original pin", got, ok)
	}
}

func TestSameKeyIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	ks := openTestKeystore(t, dir)
	peer := fixedIdentity(0x11).PublicKey()

	if err := ks.Authorize("bob", peer, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := ks.Authorize("bob", peer, nil); err != nil {
			t.Fatalf("repeat authorize %d refused: %v", i, err)
		}
	}
}

func TestChangedKeyWithNilConfirmerRefuses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	ks, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	old := fixedIdentity(0x12).PublicKey()
	fresh := fixedIdentity(0x13).PublicKey()

	if err := ks.Authorize("carol", old, nil); err != nil {
		t.Fatal(err)
	}

	// nil confirmer = headless/scripted context = REFUSAL. This is the safe
	// default doing its job without anyone opting in.
	err = ks.Authorize("carol", fresh, nil)
	if !errors.Is(err, ErrKeyChanged) {
		t.Fatalf("err = %v, want ErrKeyChanged", err)
	}
	var refused *KeyChangeRefused
	if !errors.As(err, &refused) {
		t.Fatalf("err = %T, want *KeyChangeRefused", err)
	}
	if refused.Peer != "carol" ||
		refused.PinnedFingerprint != Fingerprint(old) ||
		refused.PresentedFingerprint != Fingerprint(fresh) {
		t.Errorf("refusal details wrong: %+v", refused)
	}

	// Refusal must leave the OLD pin standing — check through a fresh handle
	// so an in-memory-only revert cannot fake the result.
	reopened, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, _ := reopened.PinnedKey("carol"); got != old {
		t.Fatal("pin after refusal is not the original pin")
	}
}

func TestChangedKeyRefusedWhenOperatorSaysNo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	ks, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	old := fixedIdentity(0x14).PublicKey()
	fresh := fixedIdentity(0x15).PublicKey()
	if err := ks.Authorize("dave", old, nil); err != nil {
		t.Fatal(err)
	}

	called := false
	confirm := func(req KeyChangeRequest) bool {
		called = true
		if req.OldFingerprint != Fingerprint(old) || req.NewFingerprint != Fingerprint(fresh) {
			t.Errorf("confirmer saw wrong fingerprints: %+v", req)
		}
		return false // the operator compares and declines
	}

	if err := ks.Authorize("dave", fresh, confirm); !errors.Is(err, ErrKeyChanged) {
		t.Fatalf("err = %v, want ErrKeyChanged", err)
	}
	if !called {
		t.Fatal("confirmation gate was never asked")
	}

	reopened, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, _ := reopened.PinnedKey("dave"); got != old {
		t.Fatal("declined change overwrote the pin")
	}
}

func TestChangedKeyAcceptedOnlyOnExplicitConfirmation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	ks, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	old := fixedIdentity(0x16).PublicKey()
	fresh := fixedIdentity(0x17).PublicKey()
	if err := ks.Authorize("erin", old, nil); err != nil {
		t.Fatal(err)
	}

	confirm := func(KeyChangeRequest) bool { return true }
	if err := ks.Authorize("erin", fresh, confirm); err != nil {
		t.Fatalf("confirmed change refused: %v", err)
	}

	reopened, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, _ := reopened.PinnedKey("erin"); got != fresh {
		t.Fatal("confirmed change did not persist the new pin")
	}
}

func TestCorruptKeystoreRefusesToOpen(t *testing.T) {
	dir := t.TempDir()

	t.Run("not json", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.json")
		os.WriteFile(path, []byte("not json at all"), 0o600)
		if _, err := OpenKeystore(path, testClock(), nil); err == nil {
			t.Error("corrupt file opened as an empty store — silent TOFU reset")
		}
	})

	t.Run("future version", func(t *testing.T) {
		path := filepath.Join(dir, "future.json")
		os.WriteFile(path, []byte(`{"version":99,"peers":{}}`), 0o600)
		if _, err := OpenKeystore(path, testClock(), nil); err == nil {
			t.Error("unknown version accepted")
		}
	})

	t.Run("wrong key length", func(t *testing.T) {
		path := filepath.Join(dir, "shortkey.json")
		os.WriteFile(path, []byte(`{"version":1,"peers":{"mallory":{"key":"AAAA","pinned_at":"2026-01-01T00:00:00Z"}}}`), 0o600)
		if _, err := OpenKeystore(path, testClock(), nil); err == nil {
			t.Error("malformed pin entry accepted")
		}
	})
}

func TestKeystorePermissionsEnforcedAndTightened(t *testing.T) {
	dir := t.TempDir()
	keyDir := filepath.Join(dir, "loose")
	os.MkdirAll(keyDir, 0o755)
	path := filepath.Join(keyDir, "peers.json")

	ks, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()
	if err := ks.Authorize("frank", fixedIdentity(0x18).PublicKey(), nil); err != nil {
		t.Fatal(err)
	}

	// Loosen both levels after creation, reopen, require tightening.
	os.Chmod(path, 0o644)
	os.Chmod(keyDir, 0o755)
	reopened, err := OpenKeystore(path, testClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("keystore file mode = %#o, want tightened 0600", got)
	}
	di, _ := os.Stat(keyDir)
	if got := di.Mode().Perm(); got != 0o700 {
		t.Errorf("keystore dir mode = %#o, want tightened 0700", got)
	}
}

func TestAuthorizeRejectsEmptyPeer(t *testing.T) {
	dir := t.TempDir()
	ks := openTestKeystore(t, dir)
	if err := ks.Authorize("", fixedIdentity(0x19).PublicKey(), nil); err == nil {
		t.Fatal("empty peer name accepted")
	}
}

func TestNilClockRefused(t *testing.T) {
	if _, err := OpenKeystore(filepath.Join(t.TempDir(), "peers.json"), nil, nil); err == nil {
		t.Fatal("nil clock accepted — timestamps would silently lose their seam")
	}
}
