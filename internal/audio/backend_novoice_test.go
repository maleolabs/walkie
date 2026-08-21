//go:build !voice

package audio

import (
	"errors"
	"testing"
)

// This test is the guard on the default build's half of the split in
// adr:002-runtime-stack. If it ever fails, an audio backend has leaked into the
// untagged build — which is exactly the mistake that silently destroys the
// pure-Go static-binary property.
func TestDefaultBuildHasNoAudioBackend(t *testing.T) {
	if Available() {
		t.Fatal(`Available() is true in a build without the "voice" tag; the build-tag split is broken`)
	}

	constructors := map[string]func() error{
		"OpenCapture":  func() error { _, err := OpenCapture(Options{}); return err },
		"OpenPlayback": func() error { _, err := OpenPlayback(Options{}); return err },
		"NewEncoder":   func() error { _, err := NewEncoder(Options{}); return err },
		"NewDecoder":   func() error { _, err := NewDecoder(Options{}); return err },
	}
	for name, construct := range constructors {
		if err := construct(); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s returned %v, want ErrUnavailable", name, err)
		}
	}
}

// The null backend must keep working in the default build, because that is how
// a text-only client's handling of an arriving voice note gets tested.
func TestNullBackendAvailableWithoutVoiceTag(t *testing.T) {
	if src := NewNullCapture(1); src == nil {
		t.Fatal("NewNullCapture returned nil in the default build")
	}
}
