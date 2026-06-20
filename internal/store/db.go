package store

import (
	"database/sql"
	"errors"
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

	// lock is the SHARED advisory lock held for this writable connection's
	// lifetime (nil for :memory:/URI opens and for OpenNoMigrate, which is
	// read-only inspection). Closed by Close so the flock is released when the
	// connection goes away. See snapshot_lock.go for the lock discipline.
	lock *dbLockHandle

	// migratingUnderExclusive is set by Open() ONLY for the risky-upgrade
	// transition (Finding 1, Round 6): Open detected a pending risky migration,
	// closed the first conn, released SHARED, acquired EXCLUSIVE, and opened THIS
	// fresh conn under the held exclusive lock. migrate() then runs the
	// restore-point + destructive DDL WITHOUT re-acquiring the lock (the in-process
	// RWMutex is not re-entrant and the flock fd is already exclusive). It is the
	// flag that replaces the old shared→exclusive downgrade-with-a-live-handle
	// dance: no *sql.DB handle exists across the lock transition.
	migratingUnderExclusive bool
}

// Close closes the underlying sql.DB FIRST, then releases the SHARED advisory
// lock (if held). Ordering matters (Finding 3, Round 6): sql.DB.Close() can block
// until in-flight queries drain and the underlying SQLite file handles are
// actually closed. If we dropped the flock first, a concurrent Restore could win
// the EXCLUSIVE lock and rename the DB triplet while those SQLite handles were
// still alive — the very swap-under-a-live-handle the bar forbids. The lock must
// OUTLIVE the last live handle, so the flock/RWMutex release happens only after
// sql.DB.Close() returns. Releasing the lock here is what bounds a writable open's
// SHARED hold to the connection lifetime, so a Restore's EXCLUSIVE acquire can
// proceed once every writer has fully closed.
func (db *DB) Close() error {
	var closeErr error
	if db.DB != nil {
		// Close the underlying handles FIRST so the lock is not dropped while a
		// SQLite handle to this path is still open.
		closeErr = db.DB.Close()
	}
	if db.lock != nil {
		db.lock.release()
		db.lock = nil
	}
	return closeErr
}

// hookAfterSharedReleasedBeforeExclusive is a TEST-ONLY seam (nil in production)
// fired inside openRiskyUpgradeUnderExclusive after the SHARED lock is released
// and before EXCLUSIVE is acquired — the precise window Finding 1's race targets.
// See TestOpen_RiskyUpgrade_NoHandleAcrossLockTransition.
var hookAfterSharedReleasedBeforeExclusive func()

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
//
// LOCK DISCIPLINE (Finding 5, Round 5): a writable open takes a SHARED advisory
// lock held for the connection's lifetime so a Restore (EXCLUSIVE) can never
// swap the DB triplet out from under an active SQLite connection. The
// interrupted-restore fail-closed gate runs BEFORE any chmod, and the shared
// lock + a re-check run before hardenPermissions/sql.Open, so a pending-restore
// (or exclusive-restore-in-progress) Open is no-touch: it never chmod's the DB
// before failing closed.
func Open(path string) (*DB, error) {
	// FAIL CLOSED on an interrupted restore BEFORE touching the DB (Findings 1, 2,
	// 4, 5). A torn restore leaves a marker in the sidecar; the DB on disk may be
	// missing, torn, or mid-swap. We must NEVER auto-resume here, and we must not
	// chmod first: a marker that a crash, corruption, OR an attacker can write
	// would otherwise drive destructive file moves on a routine open (e.g.
	// `continuity profile`). Recovery happens only under explicit operator intent
	// via `continuity snapshot restore --confirm`. A corrupt/partial marker is ALSO
	// ErrRestoreInterrupted. (If the DB dir does not exist there can be no marker,
	// so this no-touch check before MkdirAll is safe.)
	if err := detectRestoreInterrupted(path); err != nil {
		return nil, err
	}

	// Ensure the parent dir exists so the lock file (which lives beside the DB)
	// can be created. MkdirAll only CREATES a missing dir — it never chmod's an
	// existing DB, so the no-touch property for a pending-restore Open is preserved
	// (the existing-DB chmod is hardenPermissions, below, which runs only AFTER the
	// lock + interrupted re-check). A missing dir also means no marker can exist.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	// SHARED lock for this writable connection's lifetime. If a Restore holds the
	// EXCLUSIVE lock, LOCK_SH blocks until it releases — and we re-check for an
	// interrupted restore afterward so we never proceed through a half-restored DB.
	// A new writable open while EXCLUSIVE is held therefore cannot reach sql.Open
	// (nor chmod the DB) until the restore is done.
	lock, lerr := acquireSharedLock(path)
	if lerr != nil {
		return nil, fmt.Errorf("acquire db lock: %w", lerr)
	}
	// Re-check after acquiring shared: a restore that completed WHILE we waited may
	// have left (or cleared) a marker. Fail closed BEFORE hardenPermissions so a
	// pending-restore open never chmod's the DB.
	if err := detectRestoreInterrupted(path); err != nil {
		lock.release()
		return nil, err
	}

	// Tighten permissions on existing installs — MkdirAll/Open only set
	// permissions on creation, so pre-existing dirs/files need explicit chmod.
	// Runs only after the lock + interrupted re-check (no-touch on a pending open).
	hardenPermissions(dir, path)

	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		lock.release()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db := &DB{DB: sqlDB, Path: path, lock: lock}
	if err := db.configurePragmas(); err != nil {
		db.Close()
		return nil, err
	}

	// NO-OPEN-HANDLE-ACROSS-LOCK-TRANSITION (Finding 1, Round 6).
	//
	// Determine — through the SHARED-locked conn — whether opening this DB would
	// run a RISKY (destructive table-rebuild) migration. If it would, the migration
	// must run under EXCLUSIVE; but the dangerous shared→exclusive UPGRADE must
	// never happen with a live SQLite handle open. A concurrent restore could win
	// that gap, rename the DB triplet aside, and the migration would then write to
	// the moved-aside inode while the live path holds the restored DB. So we:
	//
	//   1. probe risky-pending under SHARED,
	//   2. if pending: CLOSE this conn + RELEASE shared so NO handle is open,
	//   3. acquire EXCLUSIVE (bounded wait, fail closed), then re-check the
	//      interrupted-restore marker — a restore that won the gap left a marker,
	//   4. open a FRESH conn under the held exclusive lock and run the
	//      restore-point + DDL there (migrate() with migratingUnderExclusive set),
	//   5. ATOMICALLY downgrade the flock EX→SH on the SAME fd (no cross-process
	//      window) and hand the connection that lifetime SHARED hold.
	//
	// Non-risky / fresh / ineligible opens keep SHARED + this conn (unchanged).
	risky, _, rerr := db.riskyUpgradePending()
	if rerr != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", rerr)
	}
	if !risky {
		// Normal case: migrate under the lifetime SHARED lock with this conn.
		if err := db.migrate(); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
		return db, nil
	}

	return openRiskyUpgradeUnderExclusive(path, db)
}

// openRiskyUpgradeUnderExclusive performs the risky-migration open so that NO
// open *sql.DB handle exists across the shared→exclusive lock transition
// (Finding 1, Round 6). On entry `shared` is the SHARED-locked conn Open created;
// it is CLOSED and its SHARED lock RELEASED before any exclusive acquire, so the
// dangerous upgrade never races a concurrent restore that could rename the DB
// triplet out from under a live handle. The destructive DDL runs only on a FRESH
// conn opened AFTER EXCLUSIVE is held; the lock is then atomically downgraded
// EX→SH on the same fd for the returned connection's lifetime.
func openRiskyUpgradeUnderExclusive(path string, shared *DB) (*DB, error) {
	// Step 2: close the conn and release SHARED so NO handle to this path is open
	// during the lock transition. shared.Close() closes sql.DB FIRST then releases
	// the flock/RWMutex (Finding 3 ordering), so the handle is provably gone before
	// the exclusive acquire below.
	if err := shared.Close(); err != nil {
		return nil, fmt.Errorf("migrate: close shared conn before exclusive upgrade: %w", err)
	}

	// TEST SEAM (Finding 1): fires in the exact window the race exploits — AFTER the
	// shared conn is closed + SHARED released and BEFORE EXCLUSIVE is acquired. A
	// test installs a hook that, e.g., holds a foreign EXCLUSIVE lock and "restores"
	// (renames the triplet) here, proving (1) the upgrade BLOCKS on the foreign
	// exclusive rather than racing, and (2) when it resumes it operates on the LIVE
	// path / fails closed, never on a stale moved-aside inode. nil in production.
	if hookAfterSharedReleasedBeforeExclusive != nil {
		hookAfterSharedReleasedBeforeExclusive()
	}

	// Step 3: acquire EXCLUSIVE with no open handle in flight. Bounded wait, fail
	// closed (ErrDBLocked) — never proceed while another process holds the DB.
	ex, lerr := acquireExclusiveLock(path)
	if lerr != nil {
		return nil, fmt.Errorf("migrate: acquire exclusive for risky upgrade: %w", lerr)
	}

	// A restore could have WON the gap between our shared release and this exclusive
	// acquire: it would have renamed the triplet and left an interrupted/cleared
	// marker. Re-check the marker UNDER exclusive so we never migrate a half-restored
	// or moved-aside DB. Fail closed (release exclusive first).
	if err := detectRestoreInterrupted(path); err != nil {
		ex.release()
		return nil, err
	}

	// Step 4: open a FRESH conn AFTER exclusive is held. Every byte the migration
	// writes lands on the inode the live path resolves to right now, under the lock.
	sqlDB, oerr := sql.Open("sqlite", path)
	if oerr != nil {
		ex.release()
		return nil, fmt.Errorf("migrate: reopen under exclusive: %w", oerr)
	}
	db := &DB{DB: sqlDB, Path: path, lock: ex, migratingUnderExclusive: true}
	if err := db.configurePragmas(); err != nil {
		db.Close() // closes conn then releases exclusive
		return nil, err
	}
	if err := db.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	// Step 5: the destructive DDL is done. Atomically downgrade the flock EX→SH on
	// the SAME fd so there is NO cross-process window in which the DB is unlocked
	// (a separate process taking EXCLUSIVE/SHARED can only do so after this single
	// in-kernel transition). The in-process RWMutex is NOT atomically downgradable,
	// so we drop its write lock and take its read lock around the flock downgrade;
	// that leaves at most an IN-PROCESS window, which is harmless: a same-process
	// "restore"/migration would itself need the in-process write lock AND the flock,
	// and the flock never leaves exclusive→shared for a foreign process. A REAL
	// second process is excluded throughout by the unbroken flock hold.
	db.migratingUnderExclusive = false
	if err := db.downgradeExclusiveToShared(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: downgrade exclusive→shared after upgrade: %w", err)
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
	//
	// Build the file: URI by percent-escaping the path component (Round 7,
	// Finding 7). Concatenating a raw filesystem path lets URI-reserved bytes
	// ('?', '#', '%') reinterpret the DSN — '?' would start the query string (so
	// the rest of the path becomes options and mode=ro could be dropped or a
	// different file opened), '#' a fragment, '%' an escape. roFileURI escapes the
	// path so SQLite opens exactly the intended file, read-only.
	dsn := roFileURI(path)
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

// roFileURI builds a read-only SQLite "file:" URI for an on-disk path, with the
// path component percent-escaped so URI-reserved bytes ('?', '#', '%', spaces,
// …) cannot reinterpret the DSN (Round 7, Finding 7). modernc/SQLite parses the
// file: URI form; we use url.URL so each path segment is escaped per RFC 3986,
// then append the read-only query. For an absolute path this yields
// "file:///escaped/path?mode=ro"; SQLite resolves the leading empty authority to
// the local file. A relative path (tests/:memory: do not reach here) is escaped
// the same way and resolved relative to the process CWD by SQLite.
func roFileURI(path string) string {
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	return u.String()
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
