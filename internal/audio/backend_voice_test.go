//go:build voice

package audio

import (
	"errors"
	"testing"
)

// The voice half of the build-tag guard in adr:002-runtime-stack.
func TestVoiceBuildReportsAvailable(t *testing.T) {
	if !Available() {
		t.Fatal(`Available() is false in a build with the "voice" tag; the build-tag split is broken`)
	}
}

// TestVoiceBackendNotWiredYet is a tripwire, not a permanent assertion.
//
// Until spk:audio-toolchain-viability proves miniaudio and libopus work on
// fleet ARM hardware, the voice build must fail loudly and specifically rather
// than hand back a half-working device. DELETE THIS TEST when the real backend
// lands — its failure is the signal that the spike's work arrived.
func TestVoiceBackendNotWiredYet(t *testing.T) {
	constructors := map[string]func() error{
		"OpenCapture":  func() error { _, err := OpenCapture(Options{}); return err },
		"OpenPlayback": func() error { _, err := OpenPlayback(Options{}); return err },
		"NewEncoder":   func() error { _, err := NewEncoder(Options{}); return err },
		"NewDecoder":   func() error { _, err := NewDecoder(Options{}); return err },
	}
	for name, construct := range constructors {
		if err := construct(); !errors.Is(err, ErrNotImplemented) {
			t.Errorf("%s returned %v, want ErrNotImplemented", name, err)
		}
	}
}
