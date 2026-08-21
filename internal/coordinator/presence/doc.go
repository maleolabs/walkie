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
// # Also in scope
//
// A user-set custom status message that survives that user's reconnect, and a
// last-seen timestamp for offline devices.
package presence
