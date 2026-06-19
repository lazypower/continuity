package store

import (
	"database/sql"
	"errors"
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

	// FAIL CLOSED on an interrupted restore (Findings 1, 2, 4). A torn restore
	// leaves a marker in the sidecar; the DB on disk may be missing, torn, or
	// mid-swap. We must NEVER auto-resume here: a marker that a crash, corruption,
	// OR an attacker can write would otherwise drive destructive file moves on a
	// routine open (e.g. `continuity profile`). Instead we refuse to open and
	// return ErrRestoreInterrupted; recovery happens only under explicit operator
	// intent via `continuity snapshot restore --confirm`. A corrupt/partial marker
	// is ALSO ErrRestoreInterrupted (not erased, not fabricated over).
	if err := detectRestoreInterrupted(path); err != nil {
		return nil, err
	}

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

// ErrDBMissing is returned by OpenNoMigrate when the target file does not
// exist. Restore relies on this to FAIL CLOSED rather than fabricate an empty
// DB when the live database is missing.
var ErrDBMissing = errors.New("store: database file does not exist")

// OpenNoMigrate opens the SQLite database at path READ-ONLY and configures
// read-side pragmas, but does NOT run migrate(). It is the inspection-only
// open used by snapshot integrity checks, lineage fingerprinting, and the
// restore/cleanup commands — none of which should advance the schema OR mutate
// the DB they are examining. The caller MUST Close the returned *DB.
//
// Read-only by construction (?mode=ro&immutable=0): modernc/SQLite refuses to
// create a missing file in mode=ro, but the failure surfaces lazily on first
// query, not at sql.Open. To FAIL CLOSED with a clear, eager error we stat the
// file first and return ErrDBMissing when it is absent. This is what stops
// restore from silently materializing an empty DB over a missing live one.
func OpenNoMigrate(path string) (*DB, error) {
	// FAIL CLOSED on an interrupted restore, exactly like Open (Findings 1, 2,
	// 4). The inspection-only path is reached by non-server commands too; it must
	// never read through a half-restored DB beside a pending marker. Recovery is
	// the operator's explicit job. (Snapshot-image inspection inside the sidecar
	// has no marker of its own, so integrity/lineage checks are unaffected.)
	if err := detectRestoreInterrupted(path); err != nil {
		return nil, err
	}

	// Existence gate: a missing live DB must fail closed, never be fabricated.
	// (file:... DSNs and :memory: are not used with OpenNoMigrate.)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrDBMissing, path)
		}
		return nil, fmt.Errorf("stat db (no migrate): %w", err)
	}

	// Open read-only so an inspection can never advance schema or write WAL.
	dsn := "file:" + path + "?mode=ro"
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite (no migrate): %w", err)
	}
	db := &DB{DB: sqlDB, Path: path}
	if err := db.configureReadOnlyPragmas(); err != nil {
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

// configureReadOnlyPragmas applies only the pragmas that are valid against a
// mode=ro connection. journal_mode/synchronous are writes to DB-level state and
// would fail (or be silently ignored) on a read-only handle, so they are
// omitted — an inspection-only open must not attempt to mutate journaling.
func (db *DB) configureReadOnlyPragmas() error {
	pragmas := []string{
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
