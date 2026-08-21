// Package voicenote is the asynchronous voice-note flow: capture, encode,
// deliver, play back.
//
// Owning work item:
//
//	eka get walkie/sto:voice-note
//
// # This is stage 1 of two, and the split is the point
//
// req:voice-communication defines voice in two stages. This package is stage 1:
// a short spoken message delivered through the coordinator like an attachment.
// NO LATENCY REQUIREMENT APPLIES. There is no jitter buffer, no packet-loss
// concealment and no real-time path here, which is precisely why this stage is
// in the MVP and real-time calling is not.
//
// Stage 2 — live calls over the UDP data plane with RTP framing, and the
// sub-100 ms target that applies to direct paths only — is phase 2 in
// plan:roadmap-v1. Do not start building a jitter buffer in this package.
//
// # Gated on a spike
//
// This work item is blocked by spk:audio-toolchain-viability, which must prove
// on real fleet ARM hardware that the capture library and Opus encoder actually
// work there. plan:roadmap-v1 records the consequence of failure in advance: this
// item is canceled and the MVP ships text and presence only. Do not begin here
// before the spike concludes.
//
// # Interaction
//
// Toggle-driven: tap to start capture, tap to send. Hold-to-talk only where the
// Kitty keyboard protocol is detected. Recording must show a visible level
// indicator, so a user can tell the microphone is live before speaking.
//
// # Degradation is a feature, and it is testable now
//
// A recipient on a build without the "voice" tag must receive a clear indication
// that a voice note arrived and cannot be played locally — not an error, not
// silence. That path can and should be tested today using the null backend in
// internal/audio, before any real codec exists.
//
// A voice note addressed to an offline device queues and delivers on reconnect
// like any other message. It is an attachment on a delivery path that must
// already work, which is why sto:offline-queue comes first.
package voicenote
