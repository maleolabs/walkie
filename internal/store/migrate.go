package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// schemaMigrationsDDL bootstraps the version ledger. It runs before the
// migration sequence itself, because the sequence needs the table to know what
// has already been applied — the classic chicken-and-egg resolved by keeping
// the ledger outside the sequence it records.
const schemaMigrationsDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version    INTEGER PRIMARY KEY,
	name       TEXT NOT NULL,
	applied_at TEXT NOT NULL
)`

// migration is one step of the embedded schema sequence.
type migration struct {
	// version is the schema version this step produces. Versions are
	// contiguous, strictly increasing integers; each is applied at most once,
	// in its own transaction.
	version int64
	// name is a human-readable label recorded alongside the version, so an
	// operator inspecting schema_migrations can see what ran without reading
	// source.
	name string
	// stmts run verbatim, in order, inside the step's transaction.
	stmts []string
}

// migrations is the embedded migration sequence, applied by [Open] on first
// start and extended on every later start (criterion 5 of
// ts:coordinator-skeleton: created and migrated with no manual step).
//
// It is EMPTY of domain tables on purpose, and that is not a placeholder:
//
//   - presence tables belong to sto:device-presence,
//   - queue tables belong to sto:offline-queue,
//   - key-material storage belongs to ts:queue-sealed-box.
//
// Pre-creating their tables here would couple this item to schemas its owners
// have not designed yet, and criterion 6 (no password/token/session/user
// record anywhere in the store) is checked against the actual schema — the
// fewer tables that exist for no reason, the easier that audit stays honest.
//
// # How to append a migration (owning items)
//
// Append to this slice with the next unused version, a short name, and the
// statements. Then never touch that entry again: a migration already applied
// somewhere must not be edited or renumbered, because databases that ran it
// cannot be distinguished from databases that ran something else. To change
// the schema further, append another step. The apply logic enforces this:
// versions must stay contiguous from 1, and a database whose recorded version
// is beyond the sequence's end (a step was deleted or renumbered) refuses to
// open rather than guess.
var migrations = []migration{
	{
		version: 1,
		name:    "create-presence-tables",
		// sto:device-presence. Two tables because presence is two kinds of
		// thing and the schema should teach that to the next reader:
		//
		//   - presence_liveness holds SERVER-DERIVED FACTS only — when a
		//     device was last observed alive. There is deliberately NO
		//     "online" column anywhere: online state lives only in the
		//     tracker's memory, so a coordinator restart cannot resurrect a
		//     stale "online" row — not by careful code, but because the
		//     database cannot represent it. Nobody is online until they are
		//     observed alive again.
		//
		//   - presence_status holds USER-AUTHORED LABELS only — the custom
		//     status message, which req:device-presence requires to survive
		//     the setting device's reconnect (and a coordinator restart).
		//
		// No foreign key between them on purpose: a device may have a status
		// before its first heartbeat, and liveness rows outlive status
		// clears. Timestamps are RFC3339Nano UTC strings from the injected
		// clock, matching schema_migrations.applied_at.
		stmts: []string{
			`CREATE TABLE presence_liveness (
	device    TEXT PRIMARY KEY,
	last_seen TEXT NOT NULL
)`,
			`CREATE TABLE presence_status (
	device     TEXT PRIMARY KEY,
	status     TEXT NOT NULL,
	updated_at TEXT NOT NULL
)`,
		},
	},
	{
		version: 2,
		name:    "create-offline-queue-tables",
		// sto:offline-queue. Two tables, split the same way the queue's own
		// package splits its concerns:
		//
		//   - queue_inbox holds RETAINED MESSAGES. The body column is
		//     deliberately OPAQUE bytes and must stay that way: it holds the
		//     stamped envelope verbatim today (plaintext) and will hold its
		//     sealed-box ciphertext once ts:queue-sealed-box lands — a swap
		//     of column CONTENT, never of schema. Nothing indexes or searches
		//     on it; the only keys are recipient and position. adr:004 puts
		//     these resting payloads in scope for application encryption
		//     precisely because they sit outside the WireGuard tunnel.
		//
		//   - queue_cursor holds per-recipient DELIVERY STATE: next_position
		//     (the monotonic counter — persisted, not derived from MAX(row),
		//     so positions never restart after a full drain or TTL eviction,
		//     which would silently hide new messages behind an old acked
		//     position) and acked_position (the high-water mark QueueAck
		//     advances). Both surviving restart is sto:offline-queue's point:
		//     queued messages and acknowledged positions outlive the process.
		//
		// Timestamps are UTC with a FIXED 9-digit fraction. Why not
		// RFC3339Nano like migration 1: Nano trims trailing zeros, and a
		// trailing-zero-less "…00Z" sorts lexicographically AFTER
		// "…00.5Z" — string comparison would mis-order rows. The queue is
		// the first table that must compare timestamps in SQL (TTL expiry),
		// so it needs a layout where lexicographic order IS chronological
		// order. Zero-padded fixed width gives exactly that.
		stmts: []string{
			`CREATE TABLE queue_inbox (
	recipient   TEXT    NOT NULL,
	position    INTEGER NOT NULL,
	message_id  TEXT    NOT NULL,
	body        BLOB    NOT NULL,
	enqueued_at TEXT    NOT NULL,
	PRIMARY KEY (recipient, position)
)`,
			`CREATE TABLE queue_cursor (
	recipient      TEXT PRIMARY KEY,
	next_position  INTEGER NOT NULL,
	acked_position INTEGER NOT NULL
)`,
		},
	},
	{
		version: 3,
		name:    "create-outbox-table",
		// sto:offline-queue, client half. The outbox holds messages composed
		// while disconnected until a reconnect transmits them
		// (req:offline-delivery criterion 3). Same opaqueness rule as
		// queue_inbox: envelope BLOB stores the marshaled envelope verbatim;
		// message_id is identity metadata for removal, not content.
		//
		// seq is an AUTOINCREMENT rowid because FIFO order IS the contract:
		// the outbox drains in composition order, and plain rowid reuse after
		// deletes could reorder survivors. AUTOINCREMENT forbids reuse, so
		// order survives any pattern of insert-and-remove.
		//
		// This table lives in the shared embedded sequence, so a coordinator
		// database carries it unused and vice versa — the known cost of one
		// migration sequence for one codebase; see the migrations comment
		// above. Plaintext local storage is the accepted posture here: local
		// history encryption is a recorded gap with no phase, and file
		// permissions stay restrictive via store.Open either way.
		stmts: []string{
			`CREATE TABLE outbox_message (
	seq        INTEGER PRIMARY KEY AUTOINCREMENT,
	message_id TEXT    NOT NULL UNIQUE,
	envelope   BLOB    NOT NULL,
	queued_at  TEXT    NOT NULL
)`,
		},
	},
}

// migrate brings the database from its recorded version up to len-aware
// current, using the embedded sequence. Called by Open.
func (s *Store) migrate() error {
	return applyMigrations(s.db, s.clk, migrations)
}

// applyMigrations applies list to db, recording each applied step in
// schema_migrations with a timestamp taken from clk.
//
// Transaction promise: each step is all-or-nothing. A statement that fails
// mid-step rolls back every statement of that step AND its ledger row, leaving
// the database at the previous clean version — never half-migrated. Steps that
// already ran are skipped, so Open is idempotent across restarts.
//
// Why the ledger is read as a single MAX rather than a set: steps apply in
// order and each commits atomically, so a ledger produced by this code is
// always the contiguous prefix 1..N. The only way reality can diverge from
// that shape is an edit of already-shipped history, and both possible edits
// are caught below — a gapped or renumbered list fails the contiguity check;
// a shortened list leaves the recorded maximum beyond the sequence's end.
func applyMigrations(db *sql.DB, clk clock.Clock, list []migration) error {
	if _, err := db.Exec(schemaMigrationsDDL); err != nil {
		return fmt.Errorf("store: ensure schema_migrations: %w", err)
	}

	var recorded int64
	if err := db.QueryRow(
		`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`,
	).Scan(&recorded); err != nil {
		return fmt.Errorf("store: read schema_migrations: %w", err)
	}

	// Sequence sanity before anything runs: these are programmer errors in
	// the embedded list, not runtime conditions. Versions must be contiguous
	// from 1 — a gap means an entry was deleted or renumbered, and both are
	// forbidden edits of already-shipped history. Contiguity also turns the
	// "two branches appended version N" merge accident into a loud failure
	// here instead of a silently reordered schema somewhere downstream.
	for i, m := range list {
		if m.version != int64(i+1) {
			return fmt.Errorf("store: migration %d (%q): embedded sequence must be contiguous from 1 (position %d)",
				m.version, m.name, i+1)
		}
	}

	// A database already BEYOND this list's end means the sequence was
	// shortened after databases ran it — the other forbidden edit. Refuse
	// rather than open with a schema no shipped version of the code explains.
	if len(list) > 0 && recorded > list[len(list)-1].version {
		return fmt.Errorf("store: recorded schema version %d exceeds embedded sequence end %d; the embedded sequence was edited after being applied",
			recorded, list[len(list)-1].version)
	}

	for _, m := range list {
		if m.version <= recorded {
			continue
		}
		if err := applyOne(db, clk, m); err != nil {
			return err
		}
	}
	return nil
}

// applyOne runs a single migration inside one transaction and records it.
func applyOne(db *sql.DB, clk clock.Clock, m migration) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("store: migration %d (%q): begin: %w", m.version, m.name, err)
	}
	defer tx.Rollback() // no-op after Commit; keeps failure paths from leaking txns

	for i, stmt := range m.stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("store: migration %d (%q) statement %d: %w", m.version, m.name, i+1, err)
		}
	}

	// Timestamps come from the injected clock, not time.Now: ts:test-harness
	// makes every written time deterministic under test, and UTC RFC3339 keeps
	// the column comparable across machines.
	recordedAt := clk.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
		m.version, m.name, recordedAt,
	); err != nil {
		return fmt.Errorf("store: migration %d (%q): record: %w", m.version, m.name, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: migration %d (%q): commit: %w", m.version, m.name, err)
	}
	return nil
}
