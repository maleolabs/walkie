package crypto

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// The keystore: trust on first use, persisted, with a confirmation gate that
// cannot be bypassed by silence (criteria 2 and 5).
//
// # The rule, and the failure mode it forbids
//
// The first key seen for a peer is pinned. A DIFFERENT key for an
// already-known peer is never accepted silently — not with a log line, not
// with "warn and proceed", not behind a default-on flag. adr:004 names
// silent acceptance as the defect that would make fingerprints meaningless
// and the whole trust model decorative; criterion 5 repeats it verbatim.
//
// So the API makes refusal the path of least resistance:
//
//   - [Keystore.Authorize] takes a [ConfirmFunc]. A changed key ALWAYS logs a
//     loud warning first, then asks.
//   - A nil ConfirmFunc REFUSES. Headless and scripted callers — the control
//     socket, automation, anything non-interactive — get the safe outcome by
//     doing nothing at all. Assumed consent is structurally impossible: there
//     is no flag anywhere in this package that defaults to accepting.
//   - Refusal is a typed error ([*KeyChangeRefused], wrapping [ErrKeyChanged])
//     so callers can surface it distinctly from infrastructure failures.
//
// A re-installed device legitimately triggers all of this, and that is
// correct: the MVP has no key rotation and no multi-device identity (adr:004
// consequences 3–4), so a new key IS a security event until a human says
// otherwise.

// KeyChangeRequest describes a peer whose presented key differs from the pin.
// It carries FINGERPRINTS only — the human-readable forms a verifier compares
// out of band. Key bytes deliberately have no way into this struct: what the
// confirmer decides with is exactly what the user can verify aloud.
type KeyChangeRequest struct {
	// Peer is the coordinator-resolved device name presenting the key.
	Peer string

	// OldFingerprint is the pinned key's fingerprint; NewFingerprint the
	// presented key's. Both in [Fingerprint] form.
	OldFingerprint string
	NewFingerprint string
}

// ConfirmFunc is the explicit human confirmation gate for a changed peer key.
//
// Implementations prompt a human and return their answer; returning true
// asserts the operator compared the fingerprints out of band and accepts the
// new key. nil is valid everywhere a ConfirmFunc is taken and means REFUSE —
// the headless/scripted default (see the package-level rule above).
type ConfirmFunc func(KeyChangeRequest) bool

// ErrKeyChanged is wrapped by every refusal of a changed peer key. Callers
// test with errors.Is to distinguish "the human (or the safe default) said no"
// from transport or storage failures.
var ErrKeyChanged = errors.New("crypto: peer presented a different key than pinned")

// KeyChangeRefused is the typed refusal returned when a changed key was not
// explicitly confirmed. Fingerprints only, never key bytes — error strings
// end up in logs, and the no-key-material rule has no exceptions.
type KeyChangeRefused struct {
	Peer                 string
	PinnedFingerprint    string
	PresentedFingerprint string
}

func (e *KeyChangeRefused) Error() string {
	return fmt.Sprintf(
		"crypto: peer %q presented a different key than pinned (pinned %s, presented %s); refusing — confirm the change explicitly to accept it",
		e.Peer, e.PinnedFingerprint, e.PresentedFingerprint,
	)
}

func (e *KeyChangeRefused) Unwrap() error { return ErrKeyChanged }

// pinRecord is one peer's pinned key as persisted. Key is raw 32 bytes (JSON
// base64); PinnedAt is when the pin was first made; ConfirmedAt records an
// explicitly confirmed key CHANGE, keeping the audit trail of human decisions
// inside the store itself.
type pinRecord struct {
	Key         []byte     `json:"key"`
	PinnedAt    time.Time  `json:"pinned_at"`
	ConfirmedAt *time.Time `json:"confirmed_at,omitempty"`
}

// keystoreFile is the on-disk envelope. Version guards future format changes:
// an unrecognized version refuses to load rather than improvising, because a
// misread pin store must never silently reset TOFU state.
type keystoreFile struct {
	Version int                   `json:"version"`
	Peers   map[string]*pinRecord `json:"peers"`
}

const keystoreVersion = 1

// Keystore is the persistent peer-key pin store backing walkie's TOFU rule.
//
// Safe for concurrent use: Authorize and PinnedKey serialise on one mutex,
// which also makes check-then-pin atomic against another goroutine's
// authorize of the same peer.
//
// Owning work item:
//
//	eka get walkie/ts:queue-sealed-box
type Keystore struct {
	path   string
	clk    clock.Clock
	logger *slog.Logger

	mu    sync.Mutex
	peers map[string]*pinRecord
}

// OpenKeystore loads (creating empty on first run) the pin store at path.
//
// Permission discipline matches the identity key: file 0600, parent directory
// 0700, enforced on EVERY open — os.Chmod tightens pre-existing loose files
// and directories rather than inheriting them (see fileMode/dirMode in
// keys.go for why enforcement beats creation-time modes). The pins are the
// memory of every trust decision ever made here; a file any local user could
// have rewritten is not a pin store, it is a suggestion.
//
// A corrupt or future-versioned store REFUSES to open instead of starting
// empty: silently forgetting pins would convert "attacker swapped a key" into
// "first contact" — the exact downgrade a TOFU store exists to prevent. The
// operator fixes or removes the file by hand, loudly.
//
// clk timestamps the pins (ts:test-harness: injectable time); logger receives
// the loud warnings, nil falling back to slog.Default.
func OpenKeystore(path string, clk clock.Clock, logger *slog.Logger) (*Keystore, error) {
	if clk == nil {
		return nil, fmt.Errorf("crypto: open keystore %s: clk must not be nil", path)
	}
	if logger == nil {
		logger = slog.Default()
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("crypto: resolve keystore path %s: %w", path, err)
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("crypto: create %s: %w", dir, err)
	}
	if err := os.Chmod(dir, dirMode); err != nil {
		return nil, fmt.Errorf("crypto: enforce %#o on %s: %w", dirMode, dir, err)
	}

	ks := &Keystore{path: abs, clk: clk, logger: logger, peers: map[string]*pinRecord{}}

	data, err := os.ReadFile(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return ks, nil // first run: empty store, saved on first pin
	}
	if err != nil {
		return nil, fmt.Errorf("crypto: read keystore %s: %w", abs, err)
	}

	var file keystoreFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("crypto: keystore %s: corrupt: %w", abs, err)
	}
	if file.Version != keystoreVersion {
		return nil, fmt.Errorf("crypto: keystore %s: unsupported version %d (want %d)", abs, file.Version, keystoreVersion)
	}
	for peer, rec := range file.Peers {
		if peer == "" || rec == nil || len(rec.Key) != pubKeySize {
			return nil, fmt.Errorf("crypto: keystore %s: corrupt entry for peer %q", abs, peer)
		}
	}
	ks.peers = file.Peers

	// Tighten a pre-existing loose file, same as the identity key path.
	if err := os.Chmod(abs, fileMode); err != nil {
		return nil, fmt.Errorf("crypto: enforce %#o on %s: %w", fileMode, abs, err)
	}
	return ks, nil
}

// Authorize applies the TOFU rule to a key presented for peer — the single
// gate every key crosses before use, whichever wire message delivered it
// (PublicKeyAnnounce, PublicKeyDirectory snapshot, or a queue-side lookup).
//
// Outcomes:
//
//   - unknown peer: the key is PINNED (criterion 2), logged, persisted, and
//     Authorize returns nil;
//
//   - known peer, same key: idempotent no-op, nil;
//
//   - known peer, different key: loud warning ALWAYS, then
//
//     confirm == nil          → refuse (headless default),
//     confirm(req) == false   → refuse,
//     confirm(req) == true    → re-pin, record ConfirmedAt, persist, warn again.
//
// Refusals leave the old pin untouched and return [*KeyChangeRefused]: an
// unanswered challenge must never half-apply. The warning logs carry peer
// name and fingerprints — identifiers and display values, never key bytes
// (the no-key-material rule).
func (ks *Keystore) Authorize(peer string, presented [pubKeySize]byte, confirm ConfirmFunc) error {
	if peer == "" {
		return errors.New("crypto: authorize: peer must not be empty")
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	existing, ok := ks.peers[peer]
	if !ok {
		now := ks.clk.Now()
		key := make([]byte, pubKeySize)
		copy(key, presented[:])
		ks.peers[peer] = &pinRecord{Key: key, PinnedAt: now}
		if err := ks.saveLocked(); err != nil {
			delete(ks.peers, peer) // do not keep an unpersisted pin in memory
			return fmt.Errorf("crypto: authorize %q: persist pin: %w", peer, err)
		}
		ks.logger.Info("peer key pinned on first contact",
			slog.String("peer", peer),
			slog.String("fingerprint", Fingerprint(presented)),
		)
		return nil
	}

	var pinned [pubKeySize]byte
	copy(pinned[:], existing.Key)
	if pinned == presented {
		return nil
	}

	oldFP, newFP := Fingerprint(pinned), Fingerprint(presented)

	// LOUD, unconditional, before any question: whether the operator then
	// confirms, refuses, or is never asked (headless), the log records that a
	// known peer showed up with a different key. This line is criterion 5's
	// "loud warning"; the confirmation gate below is its second half.
	ks.logger.Warn("PEER KEY CHANGED: verification required",
		slog.String("peer", peer),
		slog.String("pinned_fingerprint", oldFP),
		slog.String("presented_fingerprint", newFP),
		slog.String("detail", "compare both fingerprints out of band (read them aloud) before confirming"),
	)

	req := KeyChangeRequest{Peer: peer, OldFingerprint: oldFP, NewFingerprint: newFP}
	if confirm == nil || !confirm(req) {
		ks.logger.Warn("changed peer key REFUSED",
			slog.String("peer", peer),
			slog.String("presented_fingerprint", newFP),
		)
		return &KeyChangeRefused{Peer: peer, PinnedFingerprint: oldFP, PresentedFingerprint: newFP}
	}

	now := ks.clk.Now()
	newKey := make([]byte, pubKeySize)
	copy(newKey, presented[:])
	updated := &pinRecord{Key: newKey, PinnedAt: existing.PinnedAt, ConfirmedAt: &now}
	ks.peers[peer] = updated
	if err := ks.saveLocked(); err != nil {
		ks.peers[peer] = existing // revert: the pin stands unless persistence says otherwise
		return fmt.Errorf("crypto: authorize %q: persist confirmed key: %w", peer, err)
	}
	ks.logger.Warn("changed peer key CONFIRMED by operator; pin updated",
		slog.String("peer", peer),
		slog.String("new_fingerprint", newFP),
	)
	return nil
}

// PinnedKey returns the pinned public key for peer, if any — the sealing side
// looks peers up here before sealing queued payloads to them.
func (ks *Keystore) PinnedKey(peer string) ([pubKeySize]byte, bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	var pub [pubKeySize]byte
	rec, ok := ks.peers[peer]
	if !ok {
		return pub, false
	}
	copy(pub[:], rec.Key)
	return pub, true
}

// Close releases nothing buffered — every mutation persists synchronously —
// and exists so callers can close out symmetrically with other stores.
func (ks *Keystore) Close() error { return nil }

// saveLocked writes the store atomically: temp file in the same directory,
// fsynced, renamed over the target. Callers hold ks.mu.
//
// Atomicity is a correctness requirement, not polish: a torn pin store must
// never become tomorrow's silently-empty TOFU memory (OpenKeystore refuses a
// corrupt file rather than starting fresh, but not losing the file at all is
// still better).
func (ks *Keystore) saveLocked() error {
	file := keystoreFile{Version: keystoreVersion, Peers: ks.peers}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	f, err := os.CreateTemp(filepath.Dir(ks.path), ".keystore-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op after successful rename

	// Same umask argument as writeIdentityFile: Chmod makes 0600 unconditional.
	if err := f.Chmod(fileMode); err != nil {
		f.Close()
		return fmt.Errorf("enforce %#o on %s: %w", fileMode, tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, ks.path); err != nil {
		return fmt.Errorf("rename into place: %w", err)
	}
	return nil
}
