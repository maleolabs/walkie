package store

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	// Registers the pure-Go "sqlite" database/sql driver. adr:002-runtime-stack
	// confines CGO to the audio build tag; this import is the whole reason the
	// default binary stays static. Never replace it with a CGO driver — see the
	// package comment and `make check-cgo`.
	_ "modernc.org/sqlite"

	"github.com/maleolabs/walkie/internal/clock"
)

// fileMode is the permission enforced on the database file on every Open.
//
// Why enforced rather than set once at creation: os.OpenFile's mode is filtered
// by the process umask, so a "0600 at creation" promise silently degrades on
// hosts with a permissive umask. os.Chmod is not umask-filtered, so applying it
// on every open both creates the file owner-only and tightens any pre-existing
// over-permissive file — including one created by an older walkie version.
//
// Why it matters: the coordinator's queue will hold message content here once
// sto:offline-queue lands, and adr:004-security-model records that local
// storage is NOT encrypted beyond the sealed box on queued payloads — file
// permissions are part of the real defence, not ceremony.
const fileMode os.FileMode = 0o600

// Store is a handle on one SQLite database file: driver setup, migration state,
// and the connection later items build their tables and queries on.
//
// It is safe for concurrent use through [Store.DB] (database/sql serialises on
// the single pooled connection; see SetMaxOpenConns below). Individual domain
// operations — queue enqueue/dequeue, presence upsert — arrive with their owning
// items (sto:offline-queue, sto:device-presence) as methods here or on top of
// DB(); this type deliberately does not predict their shapes.
type Store struct {
	db *sql.DB
	// clk is retained because every timestamp this package ever writes must
	// come from the injected clock (ts:test-harness: no test sleeps in real
	// time). Migration applied_at uses it today; future items' TTL and
	// retention columns will use it through this handle too.
	clk clock.Clock
}

// Open opens (creating if necessary) the SQLite database at path, migrates it
// to the current embedded schema version, and returns a handle.
//
// Durability promise: commits are durable across process crash AND power loss.
// modernc.org/sqlite defaults to journal_mode=DELETE with synchronous=FULL,
// which fsyncs the journal and database on every commit. That is slower than
// WAL+NORMAL and deliberately kept until a measured need says otherwise: this
// file will hold the offline queue (sto:offline-queue), whose contract treats
// silent loss of acknowledged messages as a defect, and the fleet writes at
// human messaging rates where the difference is unmeasurable.
//
// Ordering promise: all writes go through one database/sql connection
// (SetMaxOpenConns(1)), so write order is exactly call order within the
// process. SQLite itself guarantees commit atomicity; readers observe only
// committed state. One connection also means no self-inflicted SQLITE_BUSY:
// with a pool, our own concurrent writers would contend on SQLite's single
// writer lock and busy_timeout would just convert that into waits. The fleet
// is under 20 devices (arc:system-overview scale posture), so pooling buys
// nothing worth its nondeterminism.
//
// No manual step: first start creates the file, applies every embedded
// migration in order inside transactions, and records each applied version;
// later starts apply only what is new. See migrate.go for the policy.
func Open(path string, clk clock.Clock) (*Store, error) {
	if clk == nil {
		return nil, fmt.Errorf("store: open %s: clk must not be nil", path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("store: resolve %s: %w", path, err)
	}

	// Create the file ourselves before SQLite touches it. Left to its own
	// devices SQLite creates the file with permissions subject to the umask;
	// see fileMode for why that is not acceptable here.
	f, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return nil, fmt.Errorf("store: create %s: %w", abs, err)
	}
	f.Close()
	if err := os.Chmod(abs, fileMode); err != nil {
		return nil, fmt.Errorf("store: enforce %#o on %s: %w", fileMode, abs, err)
	}

	// Pragmas ride the DSN, not a one-off Exec, because they are
	// per-connection in SQLite and the DSN form applies to every connection
	// the pool ever opens — correct even if the pool size changes later.
	//
	//   - busy_timeout: how long a writer waits on an EXTERNAL writer (a second
	//     process, e.g. an operator's sqlite3 shell) before failing. Internal
	//     contention cannot happen while the pool is one connection.
	//   - foreign_keys: OFF by default in SQLite for legacy reasons. Every
	//     table this seam will gain (queue rows referencing recipients,
	//     presence rows) wants referential integrity ON from birth; enabling
	//     it after those tables exist would leave orphaned rows behind any
	//     bug that shipped meanwhile.
	dsn := (&url.URL{
		Scheme: "file",
		Path:   abs,
		RawQuery: url.Values{
			"_pragma": []string{"busy_timeout(5000)", "foreign_keys(ON)"},
		}.Encode(),
	}).String()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", abs, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &Store{db: db, clk: clk}

	// sql.Open is lazy; Ping forces the first real connection so a bad path,
	// corrupt file or rejected pragma fails here rather than at first use.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: ping %s: %w", abs, err)
	}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
//
// Durability promise: returns only after in-flight use has finished and the
// connection is closed; SQLite has committed everything acknowledged before
// the last operation returned, so nothing is lost by closing.
func (s *Store) Close() error {
	return s.db.Close()
}

// SchemaVersion reports the highest recorded migration version, or 0 for a
// fresh database. Read-only; useful to health checks and tests, which assert
// on it instead of sleeping out a migration.
func (s *Store) SchemaVersion() (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v)
	return v, err
}

// DB exposes the underlying handle.
//
// This is the seam's deliberate escape hatch: sto:offline-queue,
// sto:device-presence and ts:queue-sealed-box build their own statements and
// transactions on it without this package predicting their APIs. It inherits
// every promise documented on [Open] — one connection, durable commits,
// foreign keys on — and every method added elsewhere must keep them: run
// multi-statement work in a transaction, take timestamps from a clock.Clock
// (ts:test-harness), never weaken the pragmas per-session.
func (s *Store) DB() *sql.DB { return s.db }
