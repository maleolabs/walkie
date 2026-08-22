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
// exactly why adr:004-security-model puts them in scope for the sealed box. This
// package stores ciphertext; see internal/crypto and ts:queue-sealed-box. A test
// should read the stored bytes directly and assert they are not plaintext.
//
// State under sto:offline-queue (this item): bodies are stored OPAQUE — the
// queue marshals the stamped envelope verbatim into one BLOB and never opens
// it on any path other than replay, and nothing indexes or searches on
// content. The bytes are still PLAINTEXT until ts:queue-sealed-box lands;
// that item swaps ciphertext for plaintext at exactly the two boundary points
// this shape provides (marshal-before-store, unmarshal-after-load) without
// touching this logic or the schema.
package queue
