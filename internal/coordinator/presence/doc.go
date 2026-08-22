// Package presence tracks which devices are reachable right now.
//
// Owning work item:
//
//	eka get walkie/sto:device-presence
//
// # The constraint that must not be relaxed
//
// Presence is SERVER-AUTHORITATIVE, derived from a heartbeat the coordinator
// observes, with a liveness TTL. A client may never assert its own online state.
// No code path, no convenience method, no "optimistic" local update.
//
// The reason is concrete: a device that is killed, loses power, or has its
// process reaped cannot send a goodbye. Client-announced presence would leave it
// showing as online indefinitely. req:device-presence states the consequence
// plainly — in an emergency-communication tool, a stale "online" indicator is
// worse than no indicator, because it invites someone to rely on a device that
// is not there.
//
// The defining test for this package is therefore not a happy path: SIGKILL a
// client and assert it goes offline within the TTL, having announced nothing.
// Write that test first. It is only practical against an injectable clock — see
// internal/clock and ts:test-harness.
//
// # How the constraint is held, not merely stated
//
// The [Tracker] makes client-asserted liveness UNREPRESENTABLE rather than
// rejected at one call site: its only inputs are events the server itself
// witnesses — ObserveHeartbeat, ConnectionEstablished, ConnectionLost — plus
// TTL expiry. There is no flag, method or payload anywhere on the surface a
// caller could use to mark a device online. Trusting client presence would
// require inventing a new API, which is exactly the review checkpoint
// criterion 5 wants. See the Tracker type comment for the full contract,
// including restart semantics: nothing is online after startup until it is
// observed alive again, and the schema holds no "online" column to resurrect.
//
// # Also in scope
//
// A user-set custom status message that survives that user's reconnect, and a
// last-seen timestamp for offline devices.
//
// Status and liveness are different KINDS of thing and the package layout
// says so: tracker.go owns the liveness FACT side, status.go owns the
// user-authored LABEL side, view.go exposes the read model (Entry, Change,
// Snapshot, Subscribe) that slice 2's broadcast hub consumes. A label never
// implies liveness; liveness never clears a label.
package presence
