package store

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestOpenMemory(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	if db.Path != ":memory:" {
		t.Errorf("Path = %q, want :memory:", db.Path)
	}
}

func TestSchemaVersion(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	v, err := db.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != headVersion() {
		t.Errorf("SchemaVersion = %d, want head %d", v, headVersion())
	}
}

func TestTablesExist(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	tables := []string{"schema_versions", "mem_nodes", "sessions", "observations"}
	for _, table := range tables {
		var name string
		err := db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&name)
		if err != nil {
			t.Errorf("table %q not found: %v", table, err)
		}
	}
}

func TestMemNodesConstraints(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// Valid insert
	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://test', 'leaf', 'profile', 1000, 1000)
	`)
	if err != nil {
		t.Fatalf("valid insert failed: %v", err)
	}

	// Invalid node_type
	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://test2', 'invalid', 'profile', 1000, 1000)
	`)
	if err == nil {
		t.Error("expected error for invalid node_type, got nil")
	}

	// Invalid category
	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://test3', 'leaf', 'invalid', 1000, 1000)
	`)
	if err == nil {
		t.Error("expected error for invalid category, got nil")
	}
}

func TestSessionsConstraints(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// Valid insert
	_, err = db.Exec(`
		INSERT INTO sessions (session_id, started_at, status)
		VALUES ('sess-001', 1000, 'active')
	`)
	if err != nil {
		t.Fatalf("valid insert failed: %v", err)
	}

	// Invalid status
	_, err = db.Exec(`
		INSERT INTO sessions (session_id, started_at, status)
		VALUES ('sess-002', 1000, 'invalid')
	`)
	if err == nil {
		t.Error("expected error for invalid status, got nil")
	}
}

// TestMigrate_RejectsFutureSchemaVersion pins the forward-compat guard. A
// continuity binary built at schema head H must refuse to operate against a
// DB whose schema_versions table records a version > H — a newer binary
// previously migrated that DB, and silently ignoring the newer invariants
// would risk on-disk corruption (CHECK constraints we don't understand,
// foreign-key shapes that may have changed, etc.).
//
// The scenario this test reproduces: user upgrades continuity, runs `serve`
// once (which applies a future migration), then downgrades to the older
// binary. Without the guard, the older binary would proceed quietly; with
// the guard, the older binary fails fast with a clear remediation message.
func TestMigrate_RejectsFutureSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "future.db")

	// Step 1: produce a DB at current head so subsequent reopens are valid.
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}
	current, err := db.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if current != headVersion() {
		t.Fatalf("initial schema version = %d, want head %d", current, headVersion())
	}
	db.Close()

	// Step 2: stamp a fake "future" migration into schema_versions. This is
	// what a newer binary would have left behind after applying its own
	// migrations.
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	const futureVersion = 9999
	_, err = raw.Exec(
		`INSERT INTO schema_versions (version, description) VALUES (?, ?)`,
		futureVersion, "from a future continuity build",
	)
	if err != nil {
		raw.Close()
		t.Fatalf("stamp future version: %v", err)
	}
	raw.Close()

	// Step 3: reopen with the current binary. The guard MUST fire.
	_, err = Open(dbPath)
	if err == nil {
		t.Fatal("Open succeeded; the guard must refuse a future schema version")
	}

	var typed *ErrSchemaTooNew
	if !errors.As(err, &typed) {
		t.Fatalf("error is not *ErrSchemaTooNew: %v", err)
	}
	if typed.Found != futureVersion {
		t.Errorf("ErrSchemaTooNew.Found = %d, want %d", typed.Found, futureVersion)
	}
	if typed.Supported != headVersion() {
		t.Errorf("ErrSchemaTooNew.Supported = %d, want head %d", typed.Supported, headVersion())
	}

	// Operator-facing message: must name both versions and offer a path
	// forward. This contract drives what shows up on stderr when serve
	// fails to start, and agents/operators read it. Pin the substrings
	// rather than the exact text so phrasing tweaks remain frictionless.
	msg := err.Error()
	for _, substr := range []string{
		"9999",                               // the version we found
		fmt.Sprintf("max %d", headVersion()), // the version we support
		"upgrade continuity",                 // explicit remediation
		"restore a backup",                   // alternative remediation
	} {
		if !strings.Contains(msg, substr) {
			t.Errorf("error message missing %q\nfull message: %s", substr, msg)
		}
	}
}

// TestMigrate_AcceptsHeadVersion is the regression guard for the
// forward-compat check: a DB at exactly head must NOT be rejected. The guard
// uses `> head` rather than `>= head` — this test pins that boundary.
func TestMigrate_AcceptsHeadVersion(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	v, err := db.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != headVersion() {
		t.Fatalf("schema version = %d, want head %d (test setup bug)", v, headVersion())
	}

	// A second migrate against the head-version DB is exactly the path
	// every restart of `continuity serve` takes. Must be a no-op, not a
	// "too new" rejection.
	if err := db.migrate(); err != nil {
		t.Errorf("migrate against head-version DB should succeed; got %v", err)
	}
}

func TestMigrationsIdempotent(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// Running migrate again should be a no-op
	if err := db.migrate(); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	v, err := db.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != headVersion() {
		t.Errorf("SchemaVersion after re-migrate = %d, want head %d", v, headVersion())
	}
}

func TestMomentsCategory(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// moments is a valid category after migration 6
	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://user/moments/first-gift', 'leaf', 'moments', 1000, 1000)
	`)
	if err != nil {
		t.Fatalf("moments category insert failed: %v", err)
	}
}

// Migration 9 (issue #24) added feedback and reference as first-class categories.
// These tests pin the CHECK constraint behavior so a future rewrite of the table
// can't silently drop them.
func TestFeedbackCategory(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://user/feedback/terse-summaries', 'leaf', 'feedback', 1000, 1000)
	`)
	if err != nil {
		t.Fatalf("feedback category insert failed: %v", err)
	}
}

func TestReferenceCategory(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://user/reference/linear-ingest', 'leaf', 'reference', 1000, 1000)
	`)
	if err != nil {
		t.Fatalf("reference category insert failed: %v", err)
	}
}

// TestMigration9PreservesRetractionColumns confirms the v9 table swap carries
// the v8 retraction columns through without data loss. If the v9 schema ever
// drifts from the column list in CREATE TABLE, this catches it.
func TestMigration9PreservesRetractionColumns(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// Write a retracted-style row to exercise all v8 columns and the new categories
	// in one shot.
	_, err = db.Exec(`
		INSERT INTO mem_nodes (
			uri, node_type, category,
			tombstoned_at, tombstone_reason, superseded_by,
			created_at, updated_at
		) VALUES (
			'mem://user/feedback/retracted-rule', 'leaf', 'feedback',
			2000, 'wrong write', 'mem://user/feedback/replacement',
			1000, 1000
		)
	`)
	if err != nil {
		t.Fatalf("insert with v8 retraction columns failed: %v", err)
	}

	var (
		tombstonedAt    int64
		tombstoneReason string
		supersededBy    string
	)
	err = db.QueryRow(`
		SELECT tombstoned_at, tombstone_reason, superseded_by
		FROM mem_nodes WHERE uri = 'mem://user/feedback/retracted-rule'
	`).Scan(&tombstonedAt, &tombstoneReason, &supersededBy)
	if err != nil {
		t.Fatalf("read back retraction columns: %v", err)
	}
	if tombstonedAt != 2000 || tombstoneReason != "wrong write" || supersededBy != "mem://user/feedback/replacement" {
		t.Errorf("retraction columns mangled: got (%d, %q, %q)", tombstonedAt, tombstoneReason, supersededBy)
	}
}

func TestSessionToneColumn(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// tone column exists and is nullable after migration 7
	_, err = db.Exec(`
		INSERT INTO sessions (session_id, started_at, status, tone)
		VALUES ('sess-tone', 1000, 'active', 'flow state, sharp pivots')
	`)
	if err != nil {
		t.Fatalf("session with tone insert failed: %v", err)
	}

	// null tone is also valid
	_, err = db.Exec(`
		INSERT INTO sessions (session_id, started_at, status)
		VALUES ('sess-no-tone', 1000, 'active')
	`)
	if err != nil {
		t.Fatalf("session without tone insert failed: %v", err)
	}
}

func TestWALMode(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	var mode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	// In-memory databases may use "memory" mode instead of WAL
	if mode != "wal" && mode != "memory" {
		t.Errorf("journal_mode = %q, want wal or memory", mode)
	}
}

func TestForeignKeysEnabled(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	var fk int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&fk)
	if err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d, want 1", fk)
	}
}
