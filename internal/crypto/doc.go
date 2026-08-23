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
//
// # What losing the key costs, and what is NOT implemented
//
// Losing a device's identity key forfeits every message queued for that
// device. Queued payloads are sealed to the recipient's public key; there is
// no backup, no escrow and no recovery path, and that is the accepted cost
// recorded in adr:004-security-model (consequence 3: queued messages are
// transient by design). The key file is stored unencrypted on purpose —
// encrypting it would require a passphrase, and adr:004 puts no password
// anywhere in walkie; owner-only permissions are the control.
//
// Key rotation and multi-device identity are NOT implemented (adr:004
// consequence 4, a recorded gap with no phase). A re-installed device
// generates a fresh key and will correctly trigger the changed-key warning on
// every peer — that friction is the trust model working.
//
// Both statements belong in front of users: ts:docs-quickstart-runbook
// carries them into the quickstart/runbook, and they are stated here because
// criterion 7 puts them on this item.
//
// # Phase 2 note: the relay fallback
//
// adr:001's relay fallback is not built in the MVP. When it lands, payloads
// crossing the coordinator seal with this same construction — [Seal] against
// the destination peer's pinned key, opened by its holder — so nothing here
// changes shape. No relay code belongs in this package; the seam it will
// consume is exactly the one the offline queue consumes today.
package crypto
