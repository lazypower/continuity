// Command fixturedrift compares the SCHEMA of committed migration golden
// fixtures against freshly-minted ones, and exits non-zero on any divergence.
//
// The regen workflow re-pulls the real released binaries and mints fresh images
// into a temp dir; this checker then asserts the committed goldens still match
// what those binaries produce. Drift means one of:
//   - a committed golden was hand-edited or corrupted;
//   - a release asset was re-uploaded with a different schema;
//   - the seeder changed the table/index structure (vs just data).
//
// It compares the applied-migration ledger (schema_versions) and the structural
// DDL (sqlite_master), NOT row data — data carries fixed-timestamp seed values
// that are stable but beside the point. Schema is the migration-safety invariant.
//
// Usage:
//
//	go run ./scripts/fixturedrift -committed <dir> -fresh <dir>
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// schemas mirrors the distinct shipped versions the generator mints.
var schemas = []int{5, 7, 8}

func main() {
	committed := flag.String("committed", "internal/store/testdata/migration", "dir with committed golden fixtures")
	fresh := flag.String("fresh", "", "dir with freshly-minted fixtures to compare against")
	flag.Parse()

	if *fresh == "" {
		fmt.Fprintln(os.Stderr, "usage: fixturedrift -committed <dir> -fresh <dir>")
		os.Exit(2)
	}

	var drift bool
	for _, v := range schemas {
		rel := filepath.Join(fmt.Sprintf("v%d", v), "continuity.db")
		a := filepath.Join(*committed, rel)
		b := filepath.Join(*fresh, rel)

		cs, err := schemaFingerprint(a)
		if err != nil {
			fmt.Fprintf(os.Stderr, "v%d: read committed: %v\n", v, err)
			os.Exit(1)
		}
		fs, err := schemaFingerprint(b)
		if err != nil {
			fmt.Fprintf(os.Stderr, "v%d: read fresh: %v\n", v, err)
			os.Exit(1)
		}
		if cs != fs {
			drift = true
			fmt.Printf("DRIFT v%d:\n--- committed (%s)\n%s\n--- fresh (%s)\n%s\n", v, a, cs, b, fs)
		} else {
			fmt.Printf("ok v%d: committed golden matches freshly-minted schema\n", v)
		}
	}

	if drift {
		fmt.Fprintln(os.Stderr, "\nfixture drift detected — regenerate with `make migration-fixtures` and commit, or investigate a changed release asset")
		os.Exit(1)
	}
	fmt.Println("\nno drift: all committed goldens match the real released binaries")
}

// schemaFingerprint returns a stable string of a DB's applied-migration ledger
// plus its structural DDL.
func schemaFingerprint(path string) (string, error) {
	db, err := sql.Open("sqlite", path+"?mode=ro")
	if err != nil {
		return "", err
	}
	defer db.Close()

	out := "schema_versions:\n"
	rows, err := db.Query(`SELECT version, description FROM schema_versions ORDER BY version`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var v int
		var desc string
		if err := rows.Scan(&v, &desc); err != nil {
			rows.Close()
			return "", err
		}
		out += fmt.Sprintf("  %d: %s\n", v, desc)
	}
	rows.Close()

	out += "sqlite_master:\n"
	rows, err = db.Query(`
		SELECT name, COALESCE(sql, '') FROM sqlite_master
		WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			return "", err
		}
		out += fmt.Sprintf("  %s: %s\n", name, ddl)
	}
	return out, nil
}
