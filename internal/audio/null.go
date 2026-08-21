package audio

import (
	"errors"
	"io"
)

// The null backend is a pure-Go, deterministic stand-in for real hardware. It
// carries no build tag, so it is available in both build variants.
//
// ts:test-harness requires exactly this: "The null audio backend lets the
// audio-tagged build be tested with no sound device present." Continuous
// integration has no microphone, and the voice-note flow in sto:voice-note
// still has to be exercised end to end.
//
// The null codec is an identity codec, not a fake Opus. That is deliberate:
// it lets the capture-encode-send-decode-play path be tested as a pipeline
// without pulling in a real codec, while still honouring the one codec
// behaviour callers must handle — an empty packet is a DTX gap and decodes to
// concealment rather than to an error.

// ErrShortFrame reports a buffer that is not exactly one frame long.
var ErrShortFrame = errors.New("audio: buffer is not FrameBytes long")

// NewNullCapture returns a Capture that yields frames of silence and then
// io.EOF. A frames value of zero yields io.EOF immediately.
func NewNullCapture(frames int) Capture { return &nullCapture{remaining: frames} }

type nullCapture struct {
	remaining int
	closed    bool
}

func (c *nullCapture) ReadFrame(buf []byte) (int, error) {
	switch {
	case c.closed:
		return 0, io.ErrClosedPipe
	case len(buf) < FrameBytes:
		return 0, ErrShortFrame
	case c.remaining <= 0:
		return 0, io.EOF
	}
	c.remaining--
	for i := range buf[:FrameBytes] {
		buf[i] = 0
	}
	return FrameBytes, nil
}

func (c *nullCapture) Close() error {
	c.closed = true
	return nil
}

// NullPlayback is a Playback that records what it was asked to play, so tests
// can assert on the pipeline's output without a sound device.
type NullPlayback struct {
	// Frames counts accepted frames.
	Frames int
	// Bytes counts accepted PCM bytes.
	Bytes int

	closed bool
}

// NewNullPlayback returns a recording Playback.
func NewNullPlayback() *NullPlayback { return &NullPlayback{} }

func (p *NullPlayback) WriteFrame(pcm []byte) error {
	switch {
	case p.closed:
		return io.ErrClosedPipe
	case len(pcm) != FrameBytes:
		return ErrShortFrame
	}
	p.Frames++
	p.Bytes += len(pcm)
	return nil
}

func (p *NullPlayback) Close() error {
	p.closed = true
	return nil
}

// NewNullEncoder returns an Encoder that passes PCM through unchanged.
func NewNullEncoder() Encoder { return nullCodec{} }

// NewNullDecoder returns a Decoder that passes packets through unchanged and
// conceals DTX gaps as silence.
func NewNullDecoder() Decoder { return nullCodec{} }

type nullCodec struct{}

func (nullCodec) EncodeFrame(dst, pcm []byte) ([]byte, error) {
	if len(pcm) != FrameBytes {
		return nil, ErrShortFrame
	}
	return append(dst, pcm...), nil
}

func (nullCodec) DecodeFrame(dst, packet []byte) ([]byte, error) {
	if len(packet) == 0 {
		// A DTX gap. Conceal it with one frame of silence; a real decoder
		// would run packet-loss concealment here. Never an error.
		return append(dst, make([]byte, FrameBytes)...), nil
	}
	if len(packet) != FrameBytes {
		return nil, ErrShortFrame
	}
	return append(dst, packet...), nil
}

func (nullCodec) Close() error { return nil }
