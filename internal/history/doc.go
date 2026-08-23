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
// # The schema could imply a global order; it does not provide one
//
// Rows carry a seq that sorts across conversations, because SQLite wants a
// primary key and this device's arrival sequence is the honest one. That is
// LOCAL ARRIVAL ORDER — when this device observed each message — and it is
// load-bearing only WITHIN one conversation, which is the only ordering
// req:text-messaging provides. It is not a global total order over messages:
// two devices' seq values are incomparable, and the phase-2 direct data plane
// removes even the single choke point that makes one device's arrivals look
// canonical today. Display on reopen orders by conversation; nothing may
// depend on cross-conversation seq order. See internal/message's package
// comment for the full warning.
//
// Retention is bounded both ways — a TTL measured from local storage time and
// a total-row size cap — with expiry quiet-and-logged and cap overflow evicting
// the oldest rows loudly. Logs carry counts and bounds, never message content.
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
// Create the store file with restrictive permissions — 0600 on the file AND
// 0700 on its parent directory, both enforced on every open — and keep
// .gitignore's database patterns in step with whatever filename is chosen.
package history
