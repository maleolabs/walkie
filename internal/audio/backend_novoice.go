//go:build !voice

package audio

// This file is the default build's audio backend: there is deliberately no
// backend at all.
//
// adr:002-runtime-stack keeps the default binary pure Go and fully static, so
// nothing in this file may import a C-backed library. Every constructor
// returns ErrUnavailable, and callers degrade rather than fail.
//
// That behaviour is the concrete form of the "degrade rather than fail"
// principle in vis:terminal-mesh-comms: a device that cannot install libopus,
// or an embedded board with no sound hardware, is still a completely useful
// walkie client for text, presence and files. Capability is per-device, not
// per-deployment.

const available = false

// OpenCapture reports ErrUnavailable: the default build has no input device
// support compiled in.
func OpenCapture(Options) (Capture, error) { return nil, ErrUnavailable }

// OpenPlayback reports ErrUnavailable: the default build has no output device
// support compiled in.
func OpenPlayback(Options) (Playback, error) { return nil, ErrUnavailable }

// NewEncoder reports ErrUnavailable: the default build carries no Opus encoder.
func NewEncoder(Options) (Encoder, error) { return nil, ErrUnavailable }

// NewDecoder reports ErrUnavailable: the default build carries no Opus decoder.
//
// Note that a client on this build must still surface an arriving voice note
// to the user as an unplayable message rather than hiding it or treating it as
// an error — see sto:voice-note. Use the null backend in null.go to exercise
// that path in tests.
func NewDecoder(Options) (Decoder, error) { return nil, ErrUnavailable }
