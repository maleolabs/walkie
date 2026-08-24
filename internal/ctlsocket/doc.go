// Package ctlsocket exposes a local Unix-domain control socket, so a running
// walkie client can be driven and observed from the shell.
//
// Owning work item:
//
//	eka get walkie/ts:control-socket
//
// # Why this ships in the MVP
//
// Slash commands, webhooks, the subprocess plugin protocol and auto-update are
// all deferred to phase 4 in plan:roadmap-v1. This package is in the MVP anyway,
// deliberately, because it is the substrate all of them would need — and on its
// own it already makes walkie scriptable, which covers most of the practical
// demand for automation.
//
// Delivering it first is what lets the plugin runtime be postponed honestly
// rather than merely dropped.
//
// # The wire protocol
//
// One JSON object per line, both directions (newline-delimited JSON). No
// multi-line framing, no length prefixes, no binary layer: a shell script with
// no walkie-specific tooling must be able to send a command with a plain
// redirect and read events with a line-oriented read.
//
// Every line the server writes carries a "type" field, so a script that both
// subscribes and sends commands on one connection can tell the two streams
// apart with jq:
//
//	{"type":"response", ...}  — one per command line, in order
//	{"type":"event", ...}     — asynchronous, only while subscribed
//
// A request may carry an optional "id" of any JSON type; it is echoed
// verbatim on the response so pipelined commands can be matched to their
// replies.
//
// # Command surface (small on purpose)
//
//	cmd        fields            effect
//	---------  ----------------  ---------------------------------------------
//	send       to?, body         send a message; empty/omitted "to" broadcasts
//	status     status            set this device's custom status ("" clears)
//	subscribe                    attach this connection to the event stream
//	presence                     respond with the full roster snapshot
//
// Responses are {"type":"response","ok":true} or
// {"type":"response","ok":false,"error":"..."}.
//
// There is DELIBERATELY nothing here that executes anything: no shell, no exec
// endpoint, no PTY, no plugin dispatch. adr:005-no-remote-command-execution
// declines remote command execution as a design decision, delegating it to
// Tailscale SSH; building an execution path behind a local socket would turn
// that declined decision into an accidental one, and being local-only is not a
// licence to widen the surface — anything that can reach the socket can drive
// the client, and on a shared machine that is a real boundary.
//
// # Event vocabulary
//
// Events are emitted only to subscribed connections. The vocabulary is small,
// closed, and wire-stable on purpose: ts:docs-quickstart-runbook documents it
// for users, and phase 4's webhooks and plugins are meant to build on exactly
// these names.
//
//	snapshot  — sent once immediately after a successful "subscribe":
//	            {"conn":{"state":"online"},"presence":[...roster...]}
//	            It re-syncs a fresh or gap-recovering reader without any
//	            extra query command.
//	conn      — a control-plane state change:
//	            {"from":"connecting","to":"online","reason":"handshake completed",
//	             "at":"2026-08-24T01:02:03.123456789Z"}
//	            State spellings are control.State.String — wire-stable.
//	presence  — one roster change:
//	            {"device":"alpha","online":true,"last_seen":null,"status":""}
//	            last_seen is RFC3339Nano or null when never observed offline;
//	            it mirrors presenceview.Device.
//	message   — a message was filed locally (sent or received):
//	            {"id":"<ULID>","from":"alpha","to":"beta",
//	             "conversation":"dm:beta"}
//	            "to" is "" for broadcasts ("conversation":"broadcast").
//	            The BODY IS DELIBERATELY ABSENT: a socket reader learns that
//	            traffic happened, not what it says — content stays behind the
//	            owner's explicit `walkie history` query. Metadata-only keeps
//	            the event stream safe to pipe into casual scripts.
//	gap       — the slow-subscriber policy fired for THIS connection:
//	            {"dropped":N}
//	            N events addressed to this subscriber were discarded before
//	            delivery. Loss is reported, never silent.
//
// All timestamps are RFC3339Nano UTC strings — jq-friendly, no division
// arithmetic, sortable as text.
//
// # Slow-subscriber policy (acceptance criterion 5)
//
// Chosen policy: a BOUNDED PER-SUBSCRIBER QUEUE THAT DROPS AND REPORTS A GAP.
// The alternative — disconnecting slow subscribers — was rejected because the
// expected subscriber population is shell pipelines, which stall routinely and
// indefinitely (`tail -f | grep` paused in a pager); killing their connection
// turns ordinary terminal use into a reliability bug. Dropping keeps the
// client's main loop free (the producer never blocks) and memory bounded by
// MaxQueuedEvents per subscriber, and the gap event makes the loss explicit
// and recoverable: after a gap, re-send "subscribe" (or "presence") to resync.
// This mirrors the house rule that bounded retention degrades loudly rather
// than silently: unbounded queues and producer-blocking are both refused.
//
// Connection-state changes are the one vocabulary entry with no refresh that
// would heal a drop (compare internal/control's no-drop rationale), which is
// why subscribe always delivers a snapshot first and why the gap event tells
// the reader to resync rather than trusting the stream alone.
//
// # Constraints
//
//   - Local only. Unix domain socket only — no TCP fallback, no abstract
//     namespace widening. This socket is never reachable over the network in
//     any form.
//   - Owner-only permissions (0700 directory, 0600 socket).
//   - Unix socket paths have a platform length limit of roughly 104 bytes.
//     Prefer a short path under the user's runtime or state directory.
//   - Windows has no Unix-domain socket permission model to speak of; the
//     package still COMPILES there (the release matrix includes windows/amd64)
//     and [Listen] degrades to a clear error rather than breaking the build.
//   - No message content in logs. Errors name failure classes, never bodies.
package ctlsocket
