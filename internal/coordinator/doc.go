// Package coordinator is walkie's server: the process that owns exactly what
// the tailnet does not provide.
//
// Owning work item:
//
//	eka get walkie/ts:coordinator-skeleton
//
// See arc:system-overview for the component picture this implements.
//
// # What it owns
//
//   - Identity resolution, via the tailnet. See the tsauth subpackage.
//   - Presence, derived from an observed heartbeat with a liveness TTL.
//   - The offline queue: per-recipient store-and-forward.
//   - Relay fallback for peers that cannot reach each other directly. Phase 2 —
//     see plan:roadmap-v1. In the MVP everything already passes through the
//     coordinator, so there is no direct path to fall back from yet.
//   - Observability, bound to the tailnet interface only.
//
// # What it deliberately does not own
//
// Credentials, sessions, a user table, NAT traversal, or encryption of direct
// traffic. Every one of those is either provided by the tailnet or scoped out by
// decision. fnd:tailnet-capability-baseline is the evidence; adr:004 is the
// decision. If you find yourself adding a login, stop and re-read them.
//
// # Deployment shape
//
// The coordinator joins the tailnet as its own node, so it needs no host
// networking and no Tailscale sidecar in its container.
//
// # Scale posture
//
// Fewer than 20 devices. That number is what licenses a single process, a single
// SQLite file, no broker, no sharding and no consensus. Every proposal to add
// one of those must first argue against this number — the constraint is recorded
// in vis:terminal-mesh-comms precisely so it can be pointed at.
package coordinator
