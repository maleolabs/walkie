// Package history persists and queries the client's message history.
//
// Owning work item:
//
//	eka get walkie/sto:message-history
//
// History survives a client restart and is displayed in per-conversation order
// on reopen. Both timestamps are retained — the sender's send time and the
// coordinator's receive time — because clock skew across the fleet is expected
// and req:text-messaging requires the skew stay legible rather than being
// silently resolved.
//
// Storage goes through internal/store, which means the pure-Go SQLite rule
// applies here too.
//
// # A limitation that must be documented, not just known
//
// Local history is stored UNENCRYPTED. adr:004-security-model names this as a
// deliberate MVP limitation: a stolen, unlocked device exposes its history, and
// full-disk encryption is the mitigation, outside walkie.
//
// sto:message-history makes stating this in user-facing documentation an
// acceptance criterion. A limitation that lives only in a decision record is a
// limitation users will discover the hard way.
//
// Create the store file with restrictive permissions, and keep .gitignore's
// database patterns in step with whatever filename is chosen.
package history
