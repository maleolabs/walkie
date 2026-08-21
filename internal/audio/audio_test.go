package audio

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// These tests carry no build tag, so they run in both build variants. They
// cover the contract every backend must honour, using the null backend as the
// subject — which is what lets continuous integration exercise the audio
// pipeline with no sound hardware present (ts:test-harness).

func TestFrameGeometryMatchesCodecDecision(t *testing.T) {
	// fnd:terminal-audio-constraints fixes wideband mono speech in 20 ms
	// frames. If these numbers drift, the DTX and packet-loss-concealment
	// assumptions behind the bandwidth budget in adr:001 drift with them.
	if got, want := FrameSamples, 320; got != want {
		t.Errorf("FrameSamples = %d, want %d (20 ms at 16 kHz)", got, want)
	}
	if got, want := FrameBytes, 640; got != want {
		t.Errorf("FrameBytes = %d, want %d (int16 mono)", got, want)
	}
	if got, want := FrameDuration.Milliseconds(), int64(20); got != want {
		t.Errorf("FrameDuration = %v, want %d ms", FrameDuration, want)
	}
}

func TestNullCaptureYieldsRequestedFramesThenEOF(t *testing.T) {
	const frames = 3
	src := NewNullCapture(frames)
	t.Cleanup(func() { _ = src.Close() })

	buf := make([]byte, FrameBytes)
	for i := 0; i < frames; i++ {
		n, err := src.ReadFrame(buf)
		if err != nil {
			t.Fatalf("frame %d: unexpected error: %v", i, err)
		}
		if n != FrameBytes {
			t.Fatalf("frame %d: read %d bytes, want %d", i, n, FrameBytes)
		}
	}
	if _, err := src.ReadFrame(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("after %d frames got %v, want io.EOF", frames, err)
	}
}

func TestNullCaptureRejectsShortBuffer(t *testing.T) {
	src := NewNullCapture(1)
	t.Cleanup(func() { _ = src.Close() })

	if _, err := src.ReadFrame(make([]byte, FrameBytes-1)); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("got %v, want ErrShortFrame", err)
	}
}

func TestNullCodecRoundTripsAFrame(t *testing.T) {
	enc, dec := NewNullEncoder(), NewNullDecoder()
	t.Cleanup(func() { _ = enc.Close(); _ = dec.Close() })

	pcm := make([]byte, FrameBytes)
	for i := range pcm {
		pcm[i] = byte(i)
	}

	packet, err := enc.EncodeFrame(nil, pcm)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	out, err := dec.DecodeFrame(nil, packet)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if !bytes.Equal(out, pcm) {
		t.Errorf("round trip changed the frame: got %d bytes, want the original %d", len(out), len(pcm))
	}
}

func TestNullDecoderConcealsDTXGap(t *testing.T) {
	dec := NewNullDecoder()
	t.Cleanup(func() { _ = dec.Close() })

	// An empty packet is a discontinuous-transmission gap, which is the normal
	// case during silence — most of a conversation. It must conceal, never error.
	out, err := dec.DecodeFrame(nil, nil)
	if err != nil {
		t.Fatalf("an empty packet is a DTX gap and must not error: %v", err)
	}
	if len(out) != FrameBytes {
		t.Fatalf("concealed %d bytes, want one frame of %d", len(out), FrameBytes)
	}
	for i, b := range out {
		if b != 0 {
			t.Fatalf("concealment byte %d = %d, want silence", i, b)
		}
	}
}

func TestNullPlaybackRecordsAndRejects(t *testing.T) {
	p := NewNullPlayback()

	if err := p.WriteFrame(make([]byte, FrameBytes)); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if err := p.WriteFrame(make([]byte, FrameBytes-1)); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("short frame: got %v, want ErrShortFrame", err)
	}
	if p.Frames != 1 {
		t.Errorf("Frames = %d, want 1", p.Frames)
	}
	if p.Bytes != FrameBytes {
		t.Errorf("Bytes = %d, want %d", p.Bytes, FrameBytes)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.WriteFrame(make([]byte, FrameBytes)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("after Close got %v, want io.ErrClosedPipe", err)
	}
}

// TestNullPipeline is the shape sto:voice-note will build on: capture frames,
// encode each one, decode it back, play it. It runs with no hardware and no
// codec, in both build variants.
func TestNullPipeline(t *testing.T) {
	const frames = 5

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
	for {
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
		t.Errorf("played %d frames, want %d", sink.Frames, frames)
	}
}
