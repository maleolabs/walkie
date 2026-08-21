//go:build voice

package audio

// This file is the real audio backend, compiled only under the "voice" build
// tag. It is deliberately unwired, and that is the point.
//
// spk:audio-toolchain-viability must first prove, on real fleet ARM hardware
// rather than on a development laptop, that:
//
//   - a miniaudio binding captures from ALSA or PulseAudio with no desktop
//     audio stack present,
//   - libopus encodes 20 ms frames at an acceptable per-frame cost on that
//     hardware,
//   - zig cc cross-compiles the pairing to linux/arm64 without maintaining a
//     per-target sysroot, which is what adr:002-runtime-stack assumes,
//   - the default, untagged build still produces a pure-Go static binary.
//
// Adding those C dependencies now would commit the project to a pairing the
// spike exists to validate. It would also be quietly irreversible: if the
// spike fails, plan:roadmap-v1's pre-agreed consequence is that sto:voice-note
// leaves the MVP and the first release ships text and presence only.
//
// So this tag compiles today with zero CGO, which lets CI exercise both build
// variants from day one instead of discovering the split is wrong later.
//
// # Wiring this up
//
// Replace the four constructors below. Nothing outside this file should need
// to change — that is what the interfaces in audio.go are for. Two candidate
// bindings were identified during planning, and the spike should compare them
// on one specific axis: whether the binding vendors the C sources or requires
// a system library. A binding that vendors libopus removes the need to
// cross-compile libopus separately, which is worth a lot given the zig cc
// decision. Record the outcome in the spike's conclusion, then revise
// fnd:terminal-audio-constraints with the measured figures.

const available = true

// OpenCapture is not wired yet; see spk:audio-toolchain-viability.
func OpenCapture(Options) (Capture, error) { return nil, ErrNotImplemented }

// OpenPlayback is not wired yet; see spk:audio-toolchain-viability.
func OpenPlayback(Options) (Playback, error) { return nil, ErrNotImplemented }

// NewEncoder is not wired yet; see spk:audio-toolchain-viability.
//
// When implemented it must enable DTX when Options.DTX is set and default to
// DefaultBitrate when Options.Bitrate is zero.
func NewEncoder(Options) (Encoder, error) { return nil, ErrNotImplemented }

// NewDecoder is not wired yet; see spk:audio-toolchain-viability.
//
// When implemented it must treat an empty packet as a DTX gap and conceal it,
// rather than returning an error.
func NewDecoder(Options) (Decoder, error) { return nil, ErrNotImplemented }
