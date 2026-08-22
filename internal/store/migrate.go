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
var migrations = []migration{}

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
