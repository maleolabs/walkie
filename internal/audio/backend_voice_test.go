//go:build voice

package audio

import (
	"errors"
	"io"
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

// TestVoiceBuildExercisesThePipelineThroughNullBackend is the voice-tagged
// half of ts:test-harness criterion 4. The untagged tests prove the null
// pipeline works; this one proves it works *in the voice build specifically* —
// the variant CI has no sound device for — so the capture-encode-decode-play
// flow stays exercisable on both sides of the adr:002 tag split. When the real
// backend lands, this test remains valid: the null components are hardware-free
// by construction and are how the wiring will be tested before any device is.
func TestVoiceBuildExercisesThePipelineThroughNullBackend(t *testing.T) {
	const frames = 3

	src := NewNullCapture(frames)
	enc, dec := NewNullEncoder(), NewNullDecoder()
	sink := NewNullPlayback()
	t.Cleanup(func() {
		_ = src.Close()
		_ = enc.Close()
		_ = dec.Close()
		_ = sink.Close()
	})

	buf := make([]byte, FrameBytes)
	for range frames {
		n, err := src.ReadFrame(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		packet, err := enc.EncodeFrame(nil, buf[:n])
		if err != nil {
			t.Fatalf("EncodeFrame: %v", err)
		}
		pcm, err := dec.DecodeFrame(nil, packet)
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		if err := sink.WriteFrame(pcm); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}

	if sink.Frames != frames {
		t.Errorf("voice build played %d null frames, want %d", sink.Frames, frames)
	}
}
