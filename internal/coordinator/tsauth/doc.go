// Package tsauth resolves the tailnet identity behind a connection.
//
// Owning work item:
//
//	eka get walkie/ts:coordinator-skeleton
//
// # This package replaces an entire subsystem
//
// The coordinator authenticates a caller by asking the local Tailscale daemon
// who owns the connection's remote address. That is the whole authentication
// story. There is no password, no token, no session store and no user table
// anywhere in walkie.
//
// fnd:tailnet-capability-baseline identified this as the single largest scope
// reduction available in the design, and adr:004-security-model adopted it.
// Authorization is the tailnet ACL, which walkie inherits and cannot compensate
// for — a permissive ACL is an operations problem, documented in the runbook
// that ts:docs-quickstart-runbook produces.
//
// If a future change starts introducing credentials here, that is a design
// regression and needs an ADR revision, not an implementation.
//
// # Implementation notes
//
// A connection whose identity cannot be resolved is REFUSED, and the refusal is
// logged — see the acceptance criteria on ts:coordinator-skeleton. Never fall
// back to treating an unresolvable peer as anonymous-but-allowed.
//
// The import path for the Tailscale local client has moved between releases.
// Verify the current one against the version pinned in go.mod rather than
// copying an older example.
package tsauth
