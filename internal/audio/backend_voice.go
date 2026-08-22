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
//
// # What the partial spike run settled, and what it did not
//
// spk:audio-toolchain-viability ran on 2026-08-21 with no fleet ARM device and
// no zig on PATH, so it settled the binding axis and left every hardware
// measurement open. Its conclusion is still pending: nothing below says the
// audio path works on the fleet, only which candidate is worth taking there.
//
// Capture and playback: github.com/gen2brain/malgo v0.11.26 wins the axis
// outright. It vendors miniaudio.c and miniaudio.h in the module, uses no
// pkg-config, and its only linux LDFLAGS are -ldl -lpthread -lm, which glibc
// already provides. ALSA and PulseAudio are dlopen'd by soname at runtime
// (libasound.so.2, libpulse.so.0), so building it needs no audio dev package
// and no per-target sysroot — precisely the property adr:002's zig cc choice
// is meant to buy. Its resolved arm64 cgo CFLAGS carry no ARM32-only flags, so
// zig cc has nothing obvious to reject.
//
// Opus: no candidate satisfies the axis on arm64, and that is the finding that
// matters, because arm64 is the fleet.
//
//   - gopkg.in/hraban/opus.v2, and the newer github.com/hraban/opus, are
//     "#cgo pkg-config: opus opusfile" — a system libopus, libopusfile and
//     libogg resolved through pkg-config, which is exactly the per-target
//     sysroot the zig cc decision exists to avoid. Neither builds at all
//     without them.
//   - layeh.com/gopus appears to vendor libopus, and does, but the file that
//     compiles the vendored tree is constrained to "amd64,cgo 386,cgo". On
//     arm64 the package silently selects its pkg-config file instead, so the
//     vendoring buys nothing on the target that matters. Do not conclude this
//     binding works because it built on your laptop; that is the amd64 path.
//     The vendored codec is libopus 1.1.2 (2015) with an x86 float config.h,
//     and the module has not moved since 2021.
//
// So a libopus-based voice build still has to obtain libopus for arm64 some
// other way: vendor the C into this repository and compile it from this file
// with -DFIXED_POINT -DDISABLE_FLOAT_API -DOPUS_BUILD -DVAR_ARRAYS, or build it
// once per target. Which of those is chosen is a build-system decision and
// belongs to ts:build-release-matrix, not here.
//
// # One finding that is bigger than the binding choice
//
// github.com/tphakala/go-opus v1.0.0 is a cgo-free pure-Go port of libopus
// 1.6.1, held bit-exact against the C reference, with a complete decoder and a
// complete encoder. Both fnd:terminal-audio-constraints and adr:002 rest on
// "no viable pure-Go Opus encoder exists", so the literal premise no longer
// holds. It is not a drop-in for walkie, for two reasons that bite exactly
// where this design leans hardest:
//
//   - its encoder is forced CELT-only and not configurable, which is not the
//     mode libopus picks for 16 kHz mono speech near 20 kbps — it would pick
//     SILK or hybrid;
//   - its DTX triggers only on exact digital silence, not on a VAD verdict, so
//     it will effectively never fire on a live microphone. DTX collapsing
//     bandwidth during speech pauses is the property fnd:terminal-audio-
//     constraints leans on for the constrained links in vis:terminal-mesh-comms.
//
// It also requires go 1.26 while this module is on 1.25.12. So CGO stays
// unavoidable for walkie's operating point today, and this file's shape stands.
// Whether the premise sentence in fnd:terminal-audio-constraints should be
// narrowed is a knowledge change and /eka-discuss's call, not this file's.

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
