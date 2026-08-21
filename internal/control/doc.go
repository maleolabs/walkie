// Package control is the client's control-plane connection to the coordinator.
//
// Owning work item:
//
//	eka get walkie/ts:reconnect-resume
//
// # Why this is a separate plane
//
// arc:system-overview splits the control plane from the data plane for one
// concrete reason: a control-plane interruption must never tear down an active
// call. A WebSocket reconnecting is a routine event; a conversation dropping
// because of it would not be. The data plane therefore carries its own keepalive
// and its own lifetime, and nothing in this package may assume otherwise.
//
// # Reconnect behaviour
//
// Exponential backoff with FULL JITTER, capped. Not a fixed interval — that
// would make the entire fleet reconnect in lockstep every time the coordinator
// restarts. ts:reconnect-resume makes this testable rather than aspirational:
// twenty simulated clients reconnecting after a restart must not cluster into a
// single spike.
//
// An application-level heartbeat with ping and pong, because a socket can stay
// open long after the peer is gone. TCP alone will not tell you.
//
// Session resumption by acknowledged queue position rather than a full
// re-handshake. See internal/coordinator/queue for the other half.
//
// # State machine
//
// disconnected -> connecting -> handshaking -> online -> degraded -> disconnected
//
// The current state must be visible to the user at all times, including while
// degraded or reconnecting. That is an acceptance criterion, not polish: a user
// deciding whether to rely on this tool in an emergency needs to know whether it
// is actually connected.
//
// # Testing
//
// All of the above is verified through the injectable network and clock in
// internal/testnet and internal/clock. No test in this package may sleep in real
// time or depend on a live coordinator.
package control
