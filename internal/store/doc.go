// Package store owns walkie's SQLite persistence: driver setup, schema
// migration, and the connection both the client and the coordinator build on.
//
// Owning work items:
//
//	eka get walkie/ts:coordinator-skeleton   the coordinator's store
//	eka get walkie/sto:message-history       the client's history
//	eka get walkie/sto:offline-queue         the client's outbox
//
// # The one rule that must not be relaxed
//
// Use a PURE-GO SQLite driver. Never a CGO one.
//
// adr:002-runtime-stack confines CGO to the audio module behind the "voice"
// build tag so the default binary stays pure Go and fully static across every
// target including linux/arm. Persistence is the easiest place to break that by
// accident, because the best-known Go SQLite driver is CGO-based and swapping it
// in "just to get things working" compiles fine on a development laptop and then
// fails the cross-compile — or worse, silently produces a binary that needs a C
// runtime on an embedded device.
//
// `make check-cgo` catches this, and CI runs it. Do not work around it.
//
// # Files on disk are sensitive
//
// The coordinator's queue and the client's history both hold message content,
// and adr:004-security-model notes plainly that local history is NOT encrypted
// at rest in the MVP — a stolen unlocked device exposes it. Create files with
// restrictive permissions, and keep them matched by .gitignore.
package store
