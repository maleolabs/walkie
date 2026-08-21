// Package crypto implements walkie's sealed box and its trust-on-first-use
// keystore.
//
// Owning work item:
//
//	eka get walkie/ts:queue-sealed-box
//
// # Scope is narrow on purpose
//
// adr:004-security-model applies application-level encryption to exactly two
// places, and no others: messages resting in the coordinator's offline queue,
// and payloads taking the relay-fallback path. Those are the coordinator's two
// blind spots — the points where traffic leaves the WireGuard tunnel's
// protection.
//
// Direct live traffic is NOT encrypted here. It relies on WireGuard, and adding
// a second layer inside the tunnel was considered and rejected as defence-in-
// depth theatre: it encrypts the same hop twice, adds certificate lifecycle
// management, and leaves both real blind spots untouched.
//
// Resist widening this scope without revising the ADR first. Full end-to-end
// encryption of every message was declined for the MVP because it immediately
// drags in key rotation, multi-device identity and encrypted history search.
//
// # Construction
//
// X25519 key agreement with XChaCha20-Poly1305 AEAD, from the audited standard
// extension libraries. Write no custom cryptography. The AEAD supplies message
// authentication, so no separate signature scheme is introduced.
//
// # Trust on first use, and the warning that makes it mean something
//
// Each device generates an X25519 identity keypair on first run and stores it
// with owner-only permissions. Public keys distribute through the coordinator,
// and the first key seen for a peer is pinned.
//
// A CHANGED key for an already-known peer must warn loudly and require explicit
// confirmation. Silently accepting it would make the fingerprint meaningless,
// which would make the entire trust model decorative. A re-installed device
// legitimately triggers this, and that is correct — the MVP has no key rotation
// and no multi-device identity, both recorded as gaps in adr:004.
//
// Fingerprints must be readable aloud, because that is how the team verifies
// them out of band.
package crypto
