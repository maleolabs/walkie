// Package tui is walkie's terminal interface.
//
// Owning work item:
//
//	eka get walkie/sto:terminal-ui
//
// # The hardest constraint in the project lands here
//
// vis:terminal-mesh-comms requires that a working session be reachable in no
// more than three commands, by a reader who is not technical, with no
// configuration file to hand-edit. The user base is explicitly mixed
// technical and non-technical.
//
// Discoverability is therefore a requirement of this package, not a nicety:
// every command must be findable from inside the interface, without reading
// external documentation.
//
// # What must always be on screen
//
//   - Presence and custom status, without issuing a command.
//   - Connection state, including while degraded or reconnecting.
//
// From phase 2, also the active call path — direct or relayed — and its measured
// latency. req:voice-communication requires that distinction be visible to the
// user rather than hidden, which is what replaced an unenforceable blanket
// latency promise with a conditional target plus an observable measurement.
//
// # Environment
//
// Usable at 80 columns. Degrades legibly with no colour support. Runs over SSH
// on a headless device with no desktop environment present. Some fleet devices
// cannot install GUI dependencies at all, so none of these is a stretch goal.
//
// # Audio is optional here
//
// Check audio.Available and degrade legibly. A build without the "voice" tag
// must still surface an arriving voice note as an unplayable message — never
// hide it, never present it as an error. See internal/audio and sto:voice-note.
//
// # Push-to-talk is a toggle
//
// Tap to start, tap to send. A terminal does not report key-release events, so
// hold-to-talk is not portably implementable; it is available only where the
// Kitty keyboard protocol is detected, as an enhancement. This is a constraint
// discovered by investigation in fnd:terminal-audio-constraints, not a product
// preference, so do not "fix" it by polling.
package tui
