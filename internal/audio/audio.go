// Package audio is walkie's capture, encode, decode and playback seam.
//
// Owning work items:
//
//	eka get walkie/spk:audio-toolchain-viability   the real backend
//	eka get walkie/sto:voice-note                  the capture-to-send flow
//
// # Why this package sits behind a build tag
//
// fnd:terminal-audio-constraints established that no viable pure-Go Opus
// encoder exists, so any real audio path requires CGO. adr:002-runtime-stack
// therefore confines audio to the "voice" build tag, which keeps the default
// build a pure-Go static binary that still serves text, presence and files.
//
// The split, and it matters that it stays this shape:
//
//	audio.go            no tag   this file: interfaces, constants, errors
//	backend_novoice.go  !voice   constructors return ErrUnavailable
//	backend_voice.go    voice    the real miniaudio + libopus backend
//	null.go             no tag   deterministic fake, works under both tags
//
// A mistake here is invisible until a cross-compile fails, so the guard is
// mechanical: CI runs `make check-cgo`, and any import of a C-backed audio
// library from a file without the "voice" tag breaks that build immediately.
//
// # Callers must treat audio as optional
//
// Audio capability is a property of an individual device's build, not of the
// deployment. Check [Available] and degrade legibly rather than failing: a
// device that cannot play a voice note must still tell the user one arrived.
// That requirement is in req:voice-communication and sto:voice-note.
package audio

import (
	"errors"
	"time"
)

// Codec parameters, fixed by fnd:terminal-audio-constraints: wideband mono
// speech in 20 ms frames. That frame size is the operating point Opus DTX and
// packet-loss concealment are tuned for, and DTX is what collapses bandwidth
// toward zero during silence — which is most of a conversation, and the reason
// walkie can work on the constrained links named in vis:terminal-mesh-comms.
const (
	// SampleRate is the capture and playback rate in hertz.
	SampleRate = 16000

	// Channels is mono. Stereo is not useful for speech in a terminal.
	Channels = 1

	// FrameSamples is the number of samples in one frame, per channel.
	FrameSamples = SampleRate / 1000 * 20 // 320

	// FrameBytes is one frame as signed 16-bit little-endian PCM.
	FrameBytes = FrameSamples * 2 * Channels // 640
)

// FrameDuration is the wall-clock span of one frame.
const FrameDuration = 20 * time.Millisecond

// DefaultBitrate is the encoder target in bits per second. Roughly 20 kbps
// plus IP/UDP overhead at 50 packets per second lands near 31 kbps per
// direction per stream, the figure adr:001-transport-topology budgets against.
const DefaultBitrate = 20000

var (
	// ErrUnavailable reports that this binary carries no real audio backend
	// because it was built without the "voice" build tag. It is an expected
	// condition, not a failure: callers degrade rather than error out.
	ErrUnavailable = errors.New(`audio: built without the "voice" build tag`)

	// ErrNotImplemented reports that the voice-tagged backend exists but has
	// not been wired to a capture library or codec yet. Proving that pairing
	// works on fleet ARM hardware is spk:audio-toolchain-viability's job.
	ErrNotImplemented = errors.New("audio: voice backend not wired yet; see spk:audio-toolchain-viability")
)

// Options configures a backend. The zero value selects package defaults.
type Options struct {
	// Device names a specific capture or playback device. Empty selects the
	// system default.
	Device string

	// Bitrate is the encoder target in bits per second. Zero selects
	// DefaultBitrate.
	Bitrate int

	// DTX enables discontinuous transmission. Callers on constrained links
	// should leave this on.
	DTX bool
}

// Capture is a source of PCM frames from an input device.
type Capture interface {
	// ReadFrame fills buf with exactly one frame of signed 16-bit
	// little-endian mono PCM and reports how many bytes were written. buf
	// must be at least FrameBytes long. It returns io.EOF when the source is
	// exhausted, which only a finite source such as the null backend does.
	ReadFrame(buf []byte) (int, error)

	// Close releases the device.
	Close() error
}

// Playback is a sink for PCM frames to an output device.
type Playback interface {
	// WriteFrame plays exactly one frame of signed 16-bit little-endian mono
	// PCM. pcm must be FrameBytes long.
	WriteFrame(pcm []byte) error

	// Close drains and releases the device.
	Close() error
}

// Encoder compresses PCM frames.
type Encoder interface {
	// EncodeFrame encodes exactly one frame of PCM, appending to dst and
	// returning the extended slice. Passing a nil dst allocates.
	EncodeFrame(dst, pcm []byte) ([]byte, error)

	// Close releases encoder state.
	Close() error
}

// Decoder expands encoded frames back to PCM.
type Decoder interface {
	// DecodeFrame decodes one packet, appending PCM to dst and returning the
	// extended slice. An empty packet is a DTX gap and decodes to one frame of
	// concealment rather than to an error.
	DecodeFrame(dst, packet []byte) ([]byte, error)

	// Close releases decoder state.
	Close() error
}

// Available reports whether this binary includes a real audio backend. It is
// false in the default build. Use it to decide what to offer the user, never
// to decide whether to accept an incoming voice note.
func Available() bool { return available }
