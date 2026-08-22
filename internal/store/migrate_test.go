package store

import (
	"database/sql"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// tableExists reports whether a table of that name exists in db.
func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&n); err != nil {
		t.Fatalf("query sqlite_master for %q: %v", name, err)
	}
	return n == 1
}

func TestApplyMigrationsAppliesInOrderAndRecordsVersions(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")
	clk := testClock()

	list := []migration{
		{version: 1, name: "create-first", stmts: []string{`CREATE TABLE first (id INTEGER PRIMARY KEY)`}},
		{version: 2, name: "create-second", stmts: []string{`CREATE TABLE second (id INTEGER PRIMARY KEY)`}},
	}
	if err := applyMigrations(db, clk, list); err != nil {
		t.Fatalf("applyMigrations: %v", err)
	}

	if !tableExists(t, db, "first") || !tableExists(t, db, "second") {
		t.Fatal("not every migration's table exists after apply")
	}

	// The ledger records exactly what ran, stamped from the injected clock —
	// asserted against the fake's exact formatted time, so the test proves
	// where written timestamps come from rather than merely that one exists.
	wantAt := clk.Now().UTC().Format(time.RFC3339Nano)
	rows, err := db.Query(`SELECT version, name, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	defer rows.Close()

	type entry struct{ version int64; name, appliedAt string }
	var got []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.version, &e.name, &e.appliedAt); err != nil {
			t.Fatalf("scan ledger row: %v", err)
		}
		got = append(got, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}

	want := []entry{
		{1, "create-first", wantAt},
		{2, "create-second", wantAt},
	}
	if len(got) != len(want) {
		t.Fatalf("ledger has %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ledger[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestApplyMigrationsIsIdempotentAcrossRuns(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")
	clk := testClock()
	list := []migration{
		{version: 1, name: "one", stmts: []string{`CREATE TABLE t (id INTEGER PRIMARY KEY)`}},
	}

	for i := 0; i < 3; i++ {
		if err := applyMigrations(db, clk, list); err != nil {
			t.Fatalf("run %d: applyMigrations: %v", i+1, err)
		}
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if n != 1 {
		t.Fatalf("ledger has %d rows after three identical runs, want 1", n)
	}
}

func TestApplyMigrationsSkipsAppliedAndExtendsSequence(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")
	clk := testClock()

	first := []migration{
		{version: 1, name: "one", stmts: []string{`CREATE TABLE t (id INTEGER PRIMARY KEY)`}},
	}
	if err := applyMigrations(db, clk, first); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	// A later build ships the same step plus a new one; only the new one runs.
	extended := append(first, migration{
		version: 2, name: "two", stmts: []string{`ALTER TABLE t ADD COLUMN note TEXT`},
	})
	if err := applyMigrations(db, clk, extended); err != nil {
		t.Fatalf("extended apply: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if n != 2 {
		t.Fatalf("ledger has %d rows, want 2", n)
	}
	if !tableExists(t, db, "t") {
		t.Fatal("base table missing")
	}
}

func TestApplyMigrationsRefusesEditedHistory(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")
	clk := testClock()

	// Databases out there ran the full three-step sequence.
	shipped := []migration{
		{version: 1, name: "one", stmts: []string{`CREATE TABLE t1 (id INTEGER PRIMARY KEY)`}},
		{version: 2, name: "two", stmts: []string{`CREATE TABLE t2 (id INTEGER PRIMARY KEY)`}},
		{version: 3, name: "three", stmts: []string{`CREATE TABLE t3 (id INTEGER PRIMARY KEY)`}},
	}
	if err := applyMigrations(db, clk, shipped); err != nil {
		t.Fatalf("shipped apply: %v", err)
	}

	// A later build shortens the sequence: step 3 is deleted or renumbered
	// away. The recorded maximum of 3 now exceeds the sequence end of 2,
	// which must refuse loudly — silently skipping would leave databases whose
	// history no longer matches any sequence the code ever shipped.
	edited := shipped[:2]
	if err := applyMigrations(db, clk, edited); err == nil {
		t.Fatal("shortened sequence accepted; want refusal")
	}

	// And the refusal touched nothing: every applied step and its ledger row
	// survive exactly as they were.
	for _, table := range []string{"t1", "t2", "t3"} {
		if !tableExists(t, db, table) {
			t.Errorf("table %q missing after refused open", table)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if n != 3 {
		t.Fatalf("ledger has %d rows after refusal, want the original 3", n)
	}
}

func TestApplyMigrationsRefusesGappedSequence(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")

	// A gap means an entry was deleted or two branches both appended a step;
	// either way it is an edit of shipped history and refuses before running.
	list := []migration{
		{version: 1, name: "one", stmts: []string{`CREATE TABLE a (id INTEGER PRIMARY KEY)`}},
		{version: 3, name: "three", stmts: []string{`CREATE TABLE c (id INTEGER PRIMARY KEY)`}},
	}
	if err := applyMigrations(db, clock.Real(), list); err == nil {
		t.Fatal("gapped sequence accepted; want refusal")
	}
}

func TestApplyMigrationsRefusesNonIncreasingVersions(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")

	list := []migration{
		{version: 2, name: "two", stmts: []string{`CREATE TABLE a (id INTEGER PRIMARY KEY)`}},
		{version: 1, name: "one", stmts: []string{`CREATE TABLE b (id INTEGER PRIMARY KEY)`}},
	}
	if err := applyMigrations(db, clock.Real(), list); err == nil {
		t.Fatal("non-increasing versions accepted; want refusal")
	}
}

func TestApplyMigrationsRollsBackFailedStepAtomically(t *testing.T) {
	db := rawOpen(t, t.TempDir()+"/m.db")
	clk := testClock()

	list := []migration{
		{version: 1, name: "good", stmts: []string{`CREATE TABLE good (id INTEGER PRIMARY KEY)`}},
		{version: 2, name: "broken", stmts: []string{
			`CREATE TABLE half_done (id INTEGER PRIMARY KEY)`,
			`THIS IS NOT SQL`,
		}},
	}
	err := applyMigrations(db, clk, list)
	if err == nil {
		t.Fatal("broken migration accepted; want error")
	}

	// The failed step is fully absent — including its ledger row — while the
	// previously applied step survives untouched. Half-migrated is the one
	// state Open must never be able to produce.
	if tableExists(t, db, "half_done") {
		t.Fatal("failed step's first statement survived; rollback was not atomic")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version=2`).Scan(&n); err != nil {
		t.Fatalf("count failed step's ledger row: %v", err)
	}
	if n != 0 {
		t.Fatalf("failed step recorded in ledger %d times, want 0", n)
	}
	if !tableExists(t, db, "good") {
		t.Fatal("earlier clean step lost by a later failure")
	}
}
