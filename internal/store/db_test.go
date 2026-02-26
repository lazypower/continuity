package store

import (
	"testing"
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
	if v != 5 {
		t.Errorf("SchemaVersion = %d, want 5", v)
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
	if v != 5 {
		t.Errorf("SchemaVersion after re-migrate = %d, want 5", v)
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
