package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// DB wraps a sql.DB connection to the continuity SQLite database.
type DB struct {
	*sql.DB
	Path string
}

// DefaultDBPath returns the default database path: ~/.continuity/continuity.db
func DefaultDBPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home dir: %w", err)
	}
	return filepath.Join(home, ".continuity", "continuity.db"), nil
}

// Open opens (or creates) the SQLite database at the given path,
// configures pragmas, and runs migrations.
func Open(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	// Tighten permissions on existing installs — MkdirAll/Open only set
	// permissions on creation, so pre-existing dirs/files need explicit chmod.
	hardenPermissions(dir, path)

	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db := &DB{DB: sqlDB, Path: path}
	if err := db.configurePragmas(); err != nil {
		sqlDB.Close()
		return nil, err
	}
	if err := db.migrate(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// OpenNoMigrate opens the database WITHOUT running migrations. It is for
// inspection/cleanup commands (`snapshot list`, `snapshot prune`) that must not
// mutate schema as a side effect of being run. Opening with Open() would apply
// any pending risky migration — which creates a safety snapshot — so a `prune`
// against a not-yet-migrated DB would manufacture a snapshot and immediately
// delete it, silently discarding the only rollback point. Managing snapshot
// files is not a reason to upgrade the operator's schema.
//
// Callers that read sidecar tables (e.g. migration_snapshots) must tolerate
// those tables being absent — a never-migrated DB has none.
func OpenNoMigrate(path string) (*DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	hardenPermissions(dir, path)

	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db := &DB{DB: sqlDB, Path: path}
	if err := db.configurePragmas(); err != nil {
		sqlDB.Close()
		return nil, err
	}
	return db, nil
}

// OpenMemory opens an in-memory SQLite database for testing.
func OpenMemory() (*DB, error) {
	sqlDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open sqlite memory: %w", err)
	}

	db := &DB{DB: sqlDB, Path: ":memory:"}
	if err := db.configurePragmas(); err != nil {
		sqlDB.Close()
		return nil, err
	}
	if err := db.migrate(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// hardenPermissions tightens file/directory permissions for existing installs.
// MkdirAll/OpenFile only set permissions on creation — this fixes pre-existing files.
func hardenPermissions(dir, dbPath string) {
	if info, err := os.Stat(dir); err == nil && info.Mode().Perm()&0077 != 0 {
		_ = os.Chmod(dir, 0700)
	}
	for _, f := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if info, err := os.Stat(f); err == nil && info.Mode().Perm()&0077 != 0 {
			_ = os.Chmod(f, 0600)
		}
	}
}

func (db *DB) configurePragmas() error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA mmap_size=268435456", // 256MB
		"PRAGMA busy_timeout=5000",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("pragma %q: %w", p, err)
		}
	}
	return nil
}
