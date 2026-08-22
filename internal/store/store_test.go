package store

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// testClock returns a Fake at a fixed instant. Never seed a fake from the wall
// clock: half of determinism is that the start time is chosen, not observed.
func testClock() *clock.Fake {
	return clock.NewFake(time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC))
}

// mustOpen opens a Store at a fresh path in dir or fails the test.
func mustOpen(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path, testClock())
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	return s
}

func TestOpenCreatesAndMigratesOnFirstStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coord.db")
	s := mustOpen(t, path)
	defer s.Close()

	// The file exists with exactly the enforced mode — criterion-checked by
	// test, not inspection, because umask-dependent creation would otherwise
	// pass on developer laptops and fail open on fleet hosts.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := fi.Mode().Perm(); got != fileMode {
		t.Fatalf("fresh database file mode = %#o, want %#o", got, fileMode)
	}

	// The ledger exists and reports the embedded sequence's end; the
	// mechanism runs on every open. Asserted against len(migrations) rather
	// than a literal so appending a domain migration (sto:device-presence did
	// exactly that) keeps this a test of the MECHANISM, not of a count.
	v, err := s.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != int64(len(migrations)) {
		t.Fatalf("fresh SchemaVersion = %d, want %d (len(migrations))", v, len(migrations))
	}
	var n int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`,
	).Scan(&n); err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if n != 1 {
		t.Fatalf("schema_migrations table missing on first start")
	}
}

func TestOpenTightensPermissionsOfExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coord.db")

	// Simulate a file created loose — by an older build, another tool, or a
	// permissive umask. Open must tighten it rather than inherit it.
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	s := mustOpen(t, path)
	defer s.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := fi.Mode().Perm(); got != fileMode {
		t.Fatalf("existing file mode after Open = %#o, want tightened to %#o", got, fileMode)
	}
}

func TestOpenFailsWhenParentDirectoryMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-dir", "coord.db")
	if _, err := Open(path, testClock()); err == nil {
		t.Fatal("Open into a missing directory succeeded; want error")
	}
}

func TestOpenRejectsNilClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coord.db")
	if _, err := Open(path, nil); err == nil {
		t.Fatal("Open with nil clock succeeded; want error")
	}
}

func TestReopenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coord.db")

	s1 := mustOpen(t, path)
	v1, err := s1.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	s2 := mustOpen(t, path)
	defer s2.Close()

	v2, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion after reopen: %v", err)
	}
	if v1 != v2 {
		t.Fatalf("reopen changed schema version: %d -> %d", v1, v2)
	}

	// The handle stays usable for ordinary work after a reopen.
	var one int
	if err := s2.DB().QueryRow(`SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("post-reopen query: got (%d, %v), want (1, nil)", one, err)
	}
}

// rawOpen gives migrate_test a bare handle without running Open's migration,
// so sequence behaviour can be exercised against controlled lists.
func rawOpen(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
