package store

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// Criterion 6 of ts:coordinator-skeleton, checked against REALITY: "No
// password, token, session identifier or user record exists anywhere in the
// store."
//
// Why a test rather than a code reading: the invariant's whole value is that
// it holds of the LIVE schema — every migration this package will ever gain
// (queue rows from sto:offline-queue, presence from sto:device-presence, key
// material from ts:queue-sealed-box) lands here and gets audited the moment
// `go test` runs, including in CI where nobody re-reads DDL by hand.
//
// The check is deliberately blunt — substring match over every table, view,
// index and column name — because credential-shaped names are exactly what a
// design regression would look like. adr:004-security-model removed the
// subsystem these words describe; a name matching below means someone is
// rebuilding it and owes an ADR revision first.
func TestSchemaContainsNoCredentialShapedNames(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "coord.db"))
	defer s.Close()

	forbidden := []string{"password", "token", "session", "user"}

	// Collect first, then inspect: the pool holds exactly one connection,
	// so a PRAGMA issued while the sqlite_master cursor is still open would
	// deadlock against itself.
	type obj struct{ kind, name string }
	var objects []obj
	rows, err := s.DB().Query(`SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view', 'index')`)
	if err != nil {
		t.Fatalf("enumerate schema objects: %v", err)
	}
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			t.Fatalf("scan schema object: %v", err)
		}
		objects = append(objects, obj{kind, name})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema objects: %v", err)
	}

	// Sanity: the audit ran over a real schema, not an empty database.
	if len(objects) == 0 {
		t.Fatal("no schema objects found; the audit audited nothing")
	}

	check := func(kind, name string) {
		lower := strings.ToLower(name)
		for _, word := range forbidden {
			if strings.Contains(lower, word) {
				t.Errorf("%s %q matches forbidden credential shape %q (criterion 6, adr:004)", kind, name, word)
			}
		}
	}

	for _, o := range objects {
		check(o.kind, o.name)

		if o.kind != "table" {
			continue
		}
		colRows, err := s.DB().Query(fmt.Sprintf(`PRAGMA table_info(%q)`, o.name))
		if err != nil {
			t.Fatalf("table_info(%s): %v", o.name, err)
		}
		for colRows.Next() {
			var cid int
			var colName, colType string
			var notNull int
			var dflt any
			var pk int
			if err := colRows.Scan(&cid, &colName, &colType, &notNull, &dflt, &pk); err != nil {
				t.Fatalf("scan column of %s: %v", o.name, err)
			}
			check("column of "+o.kind+" "+o.name, colName)
		}
		colRows.Close()
		if err := colRows.Err(); err != nil {
			t.Fatalf("iterate columns of %s: %v", o.name, err)
		}
	}
}
