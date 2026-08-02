package store

import (
	"database/sql"
	"fmt"
	"net/url"
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

	sqlDB, err := sql.Open("sqlite", dsn(path))
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

	sqlDB, err := sql.Open("sqlite", dsn(path))
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

	// A plain ":memory:" DSN gives every pooled connection its OWN empty
	// database — migrations land on one connection and any other goroutine
	// (the server's telemetry recorder, extraction workers) draws a second
	// connection and sees no tables. Pin the pool to a single connection so
	// the in-memory DB is one database, matching the single-writer reality
	// of the on-disk path.
	sqlDB.SetMaxOpenConns(1)

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

// connPragmas are CONNECTION-scoped and must be carried in the DSN so every
// pooled connection gets them.
//
// They used to be issued as db.Exec("PRAGMA ...") after opening. database/sql
// runs that on whichever pooled connection happens to serve the call, and every
// connection opened afterwards silently reverted to SQLite's defaults. The
// visible symptom was boot-time writers losing to each other: with
// busy_timeout defaulting to 0, a connection that found the write lock held
// returned SQLITE_BUSY instantly instead of waiting, so the observation
// retention sweep, the snapshot retention tick, and the metrics rollup could
// each fail while the background vector backfill held the lock — each one
// logging and giving up. Retention failing that way is the quiet version of
// issue #72: growth stops being bounded and only a log line says so.
//
// foreign_keys is the more dangerous half. SQLite defaults it OFF, so most
// connections were not enforcing referential integrity at all.
//
// journal_mode is deliberately absent: WAL is persisted in the database file,
// so it is set once in configurePragmas rather than per connection.
const connPragmas = "_pragma=busy_timeout(5000)" +
	"&_pragma=foreign_keys(1)" +
	"&_pragma=synchronous(NORMAL)" +
	"&_pragma=mmap_size(268435456)" // 256MB

// dsn builds the connection string for an on-disk database. The path is escaped
// through url.URL so paths containing spaces, '?' or '#' cannot corrupt the
// query string.
func dsn(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = connPragmas
	return u.String()
}

// configurePragmas applies the pragmas that are NOT connection-scoped.
// Connection-scoped ones ride the DSN — see connPragmas.
func (db *DB) configurePragmas() error {
	// WAL is recorded in the database header, so this persists for every
	// connection and every future process.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("pragma journal_mode: %w", err)
	}
	return nil
}
