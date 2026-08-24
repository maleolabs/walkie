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
//
// # What is built here (sto:terminal-ui, slice 1)
//
// The core Bubbletea model and its seams:
//
//   - [Model] renders four regions: a status line that is ALWAYS on screen
//     carrying the connection state from control.Machine's subscription
//     (criterion 3 — degraded and the reconnecting states included), a device
//     list fed by presenceview showing presence verdicts and custom status
//     without issuing a command (criterion 2), a per-conversation message pane
//     fed by messagehub snapshots, and an input line. Every message line shows
//     BOTH timestamps, sent then received, unmerged — skew stays legible — and
//     a locally filed send renders its missing coordinator stamp as "-",
//     never as a fabricated time.
//   - keymap.go is the SINGLE SOURCE for key bindings and help (criterion 6):
//     Update dispatches through the same table the help overlay renders, so
//     the two cannot drift. Keymap() exports the table for programmatic
//     enumeration by ts:docs-quickstart-runbook.
//   - The model consumes only the exposed seams — messagehub.Hub,
//     presenceview.View, control.State/Change — as plain Go values and
//     channels; it never imports generated protobuf types and never reaches
//     into coordinator internals. Assembly (dialing, identity, outbox,
//     history, ack discipline) lives in cmd/walkie.
//   - Models are tested without a terminal: drive Update with synthetic
//     messages and assert on View() strings. No test sleeps; no real clock is
//     involved (the model renders stamps carried on messages and takes no
//     time source of its own).
//
// Slice 2 gated the constrained-terminal rules this package designs within:
// the hard 80-column floor (below it View refuses legibly rather than
// rendering a corrupted layout), the no-colour contract (nothing load-bearing
// rides on colour or on any non-ASCII glyph — pinned by test), and the
// headless-over-SSH audit (no clipboard, notification or desktop
// assumptions; the composer cursor is ASCII). What a test here cannot do is
// run bubbletea's Program against a real TTY: input decoding, alt-screen
// repaint and SSH behaviour stay human-verification territory.
package tui
