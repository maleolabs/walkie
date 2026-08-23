// Package queue is the coordinator's per-recipient store-and-forward inbox.
//
// Owning work item:
//
//	eka get walkie/sto:offline-queue
//
// This is one of only two capabilities the tailnet does not provide — the other
// is presence. See fnd:tailnet-capability-baseline.
//
// # Delivery semantics
//
// At-least-once on the wire, deduplicated at the receiver by message ULID.
// Exactly-once is NOT attempted at the transport layer; correctness comes from
// making redelivery harmless. See internal/message for where the ULID comes from.
//
// Resumption is by POSITION, not by replay. Each message gets a monotonic
// per-recipient position; the client reports the last position it acknowledged
// and the coordinator sends what follows. req:offline-delivery requires this be
// verified by counting what crosses the wire, not merely by what the user ends up
// seeing — a correct-looking screen can hide a full replay.
//
// # Retention is always bounded
//
// Both a time-to-live and a size cap are required. An unbounded queue is not a
// feature; it is a disk-exhaustion path on a staging server.
//
// Reaching the size cap must produce an EXPLICIT refusal, to the sender and to
// the logs. Not a silent drop, not unbounded growth, not a crash. Expiry by TTL
// must be observable in the logs too. Both are acceptance criteria on
// sto:offline-queue.
//
// # Encryption
//
// Payloads resting here are outside the WireGuard tunnel's protection, which is
// exactly why adr:004-security-model puts them in scope for the sealed box. See
// internal/crypto and [AtRest] (sealed.go) for the construction and for who
// decrypts what: bodies seal TO THE RECIPIENT's pinned public key, so this
// package — and the coordinator holding it — can store what it cannot read.
//
// State under ts:queue-sealed-box: ACTIVE. The swap happened at exactly the two
// boundary points this shape reserved (sealForStorage before the INSERT,
// deliveryFromStorage after the SELECT), without touching this logic or the
// schema. Production wires [NewSealed] over [NewKeystoreSealer]: Enqueue seals
// to the recipient's pinned key, Resume ships opaque SealedDelivery frames,
// and the recipient's client opens each box with its own identity key — a box
// that fails authentication drops whole THERE, loudly, never partially
// processed, and its position is acked so a forfeited message cannot freeze
// the ack high-water behind it. A recipient with no pinned key yet rests
// plaintext under the documented bootstrap rule (sealed.go): logged per hold,
// never dropped, never encrypted to nothing; pins apply forward only.
package queue
