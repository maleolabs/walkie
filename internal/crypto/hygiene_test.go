package crypto

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
)

// recordingHandler captures every log record so tests can assert on both the
// presence of loud warnings and the ABSENCE of key material.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func newRecordingLogger() (*slog.Logger, *recordingHandler) {
	h := &recordingHandler{}
	return slog.New(h), h
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// allText flattens every record into one string: message plus each
// attribute's rendered value. Key material must appear in NEITHER.
func (h *recordingHandler) allText() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	for _, r := range h.records {
		b.WriteString(r.Message)
		b.WriteByte('\n')
		r.Attrs(func(a slog.Attr) bool {
			b.WriteString(a.Key)
			b.WriteByte('=')
			b.WriteString(a.Value.String())
			b.WriteByte('\n')
			return true
		})
	}
	return b.String()
}

func (h *recordingHandler) hasLevelWantingString(level slog.Level, substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level != level || !strings.Contains(r.Message, substr) {
			continue
		}
		return true
	}
	return false
}

// forbiddenForms enumerates every encoding a careless log line might leak:
// raw bytes (as far as UTF-8 round-tripping preserves them), hex, and base64
// in its standard / unpadded / URL-safe variants.
func forbiddenForms(key []byte) []string {
	forms := []string{
		hex.EncodeToString(key),
		base64.StdEncoding.EncodeToString(key),
		base64.RawStdEncoding.EncodeToString(key),
		base64.URLEncoding.EncodeToString(key),
		base64.RawURLEncoding.EncodeToString(key),
		string(key),
	}
	return forms
}

// assertNoKeyMaterial fails t if any byte of any key appears in text under any
// encoding. The no-key-material rule has no exceptions — not in errors, not in
// logs, not at any level.
func assertNoKeyMaterial(t *testing.T, text string, keys ...[]byte) {
	t.Helper()
	for _, key := range keys {
		for _, form := range forbiddenForms(key) {
			if form == "" {
				continue
			}
			if strings.Contains(text, form) {
				t.Fatalf("key material leaked into output (form: %q...)", form[:min(8, len(form))])
			}
		}
	}
}

// TestNoKeyMaterialInLogsOrErrors drives every failure and warning path this
// package has through a capturing logger and asserts that neither the logged
// records nor the returned errors contain any byte of any private or public
// key involved. Criterion hygiene: a log line is forever; a leaked key in one
// defeats every other control in this package.
func TestNoKeyMaterialInLogsOrErrors(t *testing.T) {
	dir := t.TempDir()

	device := fixedIdentity(0x21)
	defer device.Zero()
	peerOld := fixedIdentity(0x22)
	defer peerOld.Zero()
	peerNew := fixedIdentity(0x23)
	defer peerNew.Zero()

	logger, rec := newRecordingLogger()
	ks, err := OpenKeystore(dir+"/peers.json", testClock(), logger)
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	var collectedErrors []error

	// Sealing/opening failure paths.
	badBox := make([]byte, headerSize+tagSize+8)
	for i := range badBox {
		badBox[i] = 0xEE
	}
	if _, err := Seal(peerOld.PublicKey(), []byte("secret body for nobody's eyes")); err != nil {
		collectedErrors = append(collectedErrors, err)
	}
	box, _ := Seal(device.PublicKey(), []byte("secret body for nobody's eyes"))
	if _, err := Open(device, badBox); err != nil {
		collectedErrors = append(collectedErrors, err)
	}
	if _, err := Open(device, box[:headerSize]); err != nil {
		collectedErrors = append(collectedErrors, err)
	}
	wrong := fixedIdentity(0x24)
	defer wrong.Zero()
	if _, err := Open(wrong, box); err != nil {
		collectedErrors = append(collectedErrors, err)
	}

	// Identity file failure paths.
	foreign := dir + "/foreign.key"
	os.WriteFile(foreign, make([]byte, 48), 0o600) // right size, wrong header
	if _, err := LoadOrCreateIdentity(foreign); err == nil {
		t.Fatal("expected load failure for a non-key file")
	} else {
		collectedErrors = append(collectedErrors, err)
	}

	// Keystore paths: pin, change-refuse (nil confirmer), change-confirm.
	if err := ks.Authorize("peer", peerOld.PublicKey(), nil); err != nil {
		collectedErrors = append(collectedErrors, err)
	}
	if err := ks.Authorize("peer", peerNew.PublicKey(), nil); err == nil {
		t.Fatal("expected refusal")
	} else {
		collectedErrors = append(collectedErrors, err)
	}
	if err := ks.Authorize("peer", peerNew.PublicKey(), func(KeyChangeRequest) bool { return true }); err != nil {
		collectedErrors = append(collectedErrors, err)
	}
	collectedErrors = append(collectedErrors, ks.Authorize("", peerOld.PublicKey(), nil))

	for _, err := range collectedErrors {
		if err == nil {
			continue
		}
		assertNoKeyMaterial(t, err.Error(),
			device.priv[:], device.pub[:],
			peerOld.priv[:], peerOld.pub[:],
			peerNew.priv[:], peerNew.pub[:],
			wrong.priv[:], wrong.pub[:],
		)
	}
	assertNoKeyMaterial(t, rec.allText(),
		device.priv[:], device.pub[:],
		peerOld.priv[:], peerOld.pub[:],
		peerNew.priv[:], peerNew.pub[:],
		wrong.priv[:], wrong.pub[:],
	)
}

// TestChangedKeyWarningIsLoud pins the "loud" half of criterion 5: the
// changed-key warning must be at WARN level (it survives default filtering,
// unlike debug/info chatter) and name the situation explicitly.
func TestChangedKeyWarningIsLoud(t *testing.T) {
	dir := t.TempDir()
	logger, rec := newRecordingLogger()
	ks, err := OpenKeystore(dir+"/peers.json", testClock(), logger)
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	old := fixedIdentity(0x25).PublicKey()
	fresh := fixedIdentity(0x26).PublicKey()
	ks.Authorize("grace", old, nil)

	// Refused branch still warns first.
	rec.records = nil
	ks.Authorize("grace", fresh, nil)
	if !rec.hasLevelWantingString(slog.LevelWarn, "PEER KEY CHANGED") {
		t.Fatal("refused change produced no loud warning")
	}

	// Confirmed branch warns too — confirmation does not mute the event.
	rec.records = nil
	ks.Authorize("grace", fresh, func(KeyChangeRequest) bool { return true })
	if !rec.hasLevelWantingString(slog.LevelWarn, "PEER KEY CHANGED") {
		t.Fatal("confirmed change produced no loud warning")
	}
}
