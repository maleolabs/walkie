// Package presenceview is the client's typed view of who is online.
//
// Owning work item:
//
//	eka get walkie/sto:device-presence
//
// # Where this sits
//
// The coordinator broadcasts PresenceUpdate envelopes on the control plane;
// the client's control-plane connection (ts:reconnect-resume owns that loop)
// hands each received update to [View.Apply], and sto:terminal-ui renders from
// [View.Snapshot] or subscribes via [View.Subscribe]. This package is the seam
// between those two future consumers — deliberately UI-free (no drawing, no
// TUI types) and connection-free (no dialing, no reconnect state machine), so
// both can evolve without touching this logic.
//
// # What a client may believe about presence
//
// Everything in a View arrived as a coordinator broadcast. A client NEVER
// asserts its own liveness, locally or to the server (req:device-presence
// criterion 5): its own entry here is whatever the coordinator last said,
// same as everyone else's. The distinction matters — a client that edited its
// own row optimistically would show itself online while its own socket was
// half-dead, which is precisely the stale-online failure mode this whole item
// exists to prevent.
//
// Updates are idempotent statements of current state, not deltas: applying
// them out of order across DIFFERENT devices converges, and a duplicate is
// harmless. That property is inherited from the tracker's Change model and is
// what lets the transport layer drop and redeliver freely.
package presenceview
