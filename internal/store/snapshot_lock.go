package store

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// =========================================================================
// Cross-process shared/exclusive DB lock (OS flock + in-process RWMutex).
//
// This REPLACES the prior hand-rolled PID-stamped serve-lock / op-lock. The
// fifth cross-model adversarial review (Codex) found the hand-rolled lock the
// recurring source of concurrency bugs:
//
//   - The "atomic" PID lockfile created a zero-length sentinel at the final
//     path before the PID was rename-published. A peer observing that window
//     treated the file as stale, removed it, and both processes ended up
//     "holding" the lock (Finding 1).
//   - Restore only excluded serve and risky migrations; ordinary writable
//     opens (dedup/remember/retract/import/extract via openDB()/store.Open)
//     held NO lock, so `snapshot restore --confirm` could rename the DB triplet
//     out from under an active SQLite connection (Finding 5).
//
// The fix is a proper advisory lock keyed to the canonical DB path:
//
//   - CROSS-PROCESS: flock(2) on a per-DB lockfile (<resolvedDB>.lock). flock
//     is kernel-managed (no zero-length window) and AUTO-RELEASES on process
//     death, so the PID-liveness / stale-reclaim / zero-length machinery the
//     prior round's bug came from is GONE.
//   - IN-PROCESS: flock semantics across goroutines of ONE process are
//     unreliable (it is per-open-file-description; two goroutines could open
//     two fds and both "share" an exclusive lock). A process-local RWMutex
//     registry keyed by the canonical DB path provides the goroutine-level
//     guarantee. SHARED = RLock + LOCK_SH; EXCLUSIVE = Lock + LOCK_EX.
//
// LOCK DISCIPLINE (enforced by callers):
//   - Every WRITABLE DB open (store.Open, used by serve AND openDB() CLI
//     commands) takes a SHARED lock held for the open's lifetime.
//   - Restore takes an EXCLUSIVE lock for its whole operation.
//   - A risky migration takes EXCLUSIVE across restore-point creation + the
//     migration loop.
//   - Exclusive acquire is NON-BLOCKING with a bounded wait: if shared/other
//     holders exist it waits a bounded window then FAILS CLOSED.
//   - A new writable open while EXCLUSIVE is held fails closed before sql.Open.
//
// flock is advisory and orthogonal to the marker-based ErrRestoreInterrupted
// crash detector (that detects a crashed restore; this excludes live writers).
// =========================================================================

// dbLockSuffix is appended to the canonical DB path to derive the per-DB lock
// file. Keyed to the SAME canonicalDBPath the sidecar/backup names use, so the
// lock and the data it guards are always the same real DB.
const dbLockSuffix = ".lock"

// exclusiveWaitAttempts / exclusiveWaitInterval bound how long an EXCLUSIVE
// acquire (restore, risky migration) waits for shared/other holders to clear
// before failing closed. ~5s total: long enough for a healthy writable open to
// finish its migration/work, short enough that an operator is not left hanging.
const (
	exclusiveWaitAttempts = 50
	exclusiveWaitInterval = 100 * time.Millisecond
)

// ErrDBLocked is returned when an EXCLUSIVE acquire (restore / risky migration)
// cannot be taken within the bounded window because another process holds a
// shared or exclusive lock on the DB. The caller fails closed: never swap the
// DB under a live writer.
var ErrDBLocked = errors.New("store: database is in use by another continuity process")

// dbLockPath derives the per-DB lock file from any spelling of the DB path,
// routed through the single canonicalDBPath derivation so the lock is keyed to
// the same real DB as the sidecar/backups. Returns ErrSnapshotUnsupportedPath
// for :memory:/URI/DSN paths that cannot host a sidecar lock.
func dbLockPath(dbPath string) (string, error) {
	if !snapshotEligiblePath(dbPath) {
		return "", ErrSnapshotUnsupportedPath
	}
	resolved, err := canonicalDBPath(dbPath)
	if err != nil {
		return "", err
	}
	return resolved + dbLockSuffix, nil
}

// dbLockRegistry holds one *sync.RWMutex per canonical DB lock path, providing
// the IN-PROCESS shared/exclusive gate that flock cannot give across goroutines
// of a single process. Keyed by lock path so distinct DBs never contend.
var dbLockRegistry sync.Map // map[string]*sync.RWMutex

func dbLockMutex(path string) *sync.RWMutex {
	m, _ := dbLockRegistry.LoadOrStore(path, &sync.RWMutex{})
	return m.(*sync.RWMutex)
}

// dbLockHandle is returned by an acquire and released exactly once. It bundles
// the in-process mutex release with the kernel flock release (closing the fd
// drops the flock). Release is idempotent.
type dbLockHandle struct {
	once      sync.Once
	exclusive bool
	mu        *sync.RWMutex
	f         *os.File
}

// release drops the kernel flock (by closing the fd) then the in-process
// mutex. Idempotent: a second call is a no-op. flock auto-releases on close,
// and on process death, so even a leaked handle cannot wedge a peer.
func (h *dbLockHandle) release() {
	if h == nil {
		return
	}
	h.once.Do(func() {
		if h.f != nil {
			// Closing the fd releases the flock held on this open file
			// description. We do not call flockUnlock separately: close is the
			// canonical drop and avoids a double-unlock race.
			_ = h.f.Close()
		}
		if h.mu != nil {
			if h.exclusive {
				h.mu.Unlock()
			} else {
				h.mu.RUnlock()
			}
		}
	})
}

// openLockFile opens (creating if absent) the per-DB lock file for flock. The
// file itself only exists to anchor the kernel lock; its contents are never
// read or trusted (the bug class this round removes). 0600 so it is not group/
// world accessible.
func openLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
}

// acquireSharedLock takes a SHARED (read) lock on the DB: RLock the in-process
// mutex AND flock LOCK_SH the lock file NON-BLOCKING with a bounded wait. Many
// shared holders may coexist (many concurrent writable opens across processes);
// they only exclude an EXCLUSIVE holder. If a cross-process EXCLUSIVE holder (a
// restore in progress) is present, the shared acquire waits the bounded window
// then FAILS CLOSED with ErrDBLocked rather than hanging — so `serve` "refuses to
// start if it cannot get its shared open (exclusive restore in progress)" and a
// writable CLI open does not block forever behind a stuck restore.
//
// The in-process RLock is taken first; it blocks only on a same-process EXCLUSIVE
// holder (the RWMutex is the goroutine-level gate). A restore in this same process
// holds the registry Lock, so a concurrent shared acquire here waits on it — that
// is correct in-process serialization, and restores are bounded operations.
//
// Ineligible paths (:memory:/URI/DSN) cannot host a lock file; they return a
// no-op handle so in-memory/test DBs are unaffected.
func acquireSharedLock(dbPath string) (*dbLockHandle, error) {
	path, err := dbLockPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return &dbLockHandle{}, nil // no lock for :memory:/URI
		}
		return nil, err
	}
	mu := dbLockMutex(path)
	mu.RLock()

	f, oerr := openLockFile(path)
	if oerr != nil {
		mu.RUnlock()
		return nil, fmt.Errorf("store: open db lock file: %w", oerr)
	}

	for attempt := 0; attempt < exclusiveWaitAttempts; attempt++ {
		ok, lerr := flockSharedNB(f)
		if lerr != nil {
			_ = f.Close()
			mu.RUnlock()
			return nil, fmt.Errorf("store: shared-lock db: %w", lerr)
		}
		if ok {
			return &dbLockHandle{exclusive: false, mu: mu, f: f}, nil
		}
		// A cross-process EXCLUSIVE holder (restore) is present. Wait briefly and
		// retry; the kernel auto-releases a dead holder so we make progress.
		time.Sleep(exclusiveWaitInterval)
	}
	_ = f.Close()
	mu.RUnlock()
	return nil, ErrDBLocked
}

// acquireExclusiveLock takes an EXCLUSIVE (write) lock on the DB: Lock the
// in-process mutex AND flock LOCK_EX the lock file NON-BLOCKING with a bounded
// wait. Used by Restore (whole operation) and a risky migration (restore-point
// creation + migration loop). If shared/other holders exist it waits the
// bounded window then FAILS CLOSED with ErrDBLocked — never swap the DB under a
// live writer.
//
// The in-process mutex.Lock is taken first (it blocks until same-process
// holders clear); then the cross-process flock is attempted non-blocking so a
// FOREIGN holder yields a bounded wait + clear failure rather than an unbounded
// block. A crashed exclusive holder's flock auto-releases (kernel), so the next
// acquirer is never wedged.
func acquireExclusiveLock(dbPath string) (*dbLockHandle, error) {
	path, err := dbLockPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return &dbLockHandle{}, nil
		}
		return nil, err
	}
	mu := dbLockMutex(path)
	mu.Lock()

	f, oerr := openLockFile(path)
	if oerr != nil {
		mu.Unlock()
		return nil, fmt.Errorf("store: open db lock file: %w", oerr)
	}

	for attempt := 0; attempt < exclusiveWaitAttempts; attempt++ {
		ok, lerr := flockExclusiveNB(f)
		if lerr != nil {
			_ = f.Close()
			mu.Unlock()
			return nil, fmt.Errorf("store: exclusive-lock db: %w", lerr)
		}
		if ok {
			return &dbLockHandle{exclusive: true, mu: mu, f: f}, nil
		}
		// A cross-process shared/exclusive holder is present. Wait briefly and
		// retry; the kernel auto-releases a dead holder so we make progress.
		time.Sleep(exclusiveWaitInterval)
	}
	_ = f.Close()
	mu.Unlock()
	return nil, ErrDBLocked
}

// acquireMigrateExclusive upgrades a writable Open's lifetime SHARED lock to
// EXCLUSIVE for the risky-migration span, then returns a release func that
// DOWNGRADES back to SHARED so the connection keeps its lifetime hold.
//
// Why a dance: flock is per-open-file-description, so the SHARED fd this
// connection already holds would BLOCK a fresh EXCLUSIVE acquire (even in the
// same process — verified: two fds conflict). And the in-process RWMutex is not
// re-entrant, so an RLock holder cannot take Lock. We therefore RELEASE the
// shared hold, take EXCLUSIVE (bounded-wait, fail-closed), run the migration,
// and on release RE-ACQUIRE shared for the rest of the connection's life.
//
// The brief gap between releasing shared and taking exclusive is safe: a
// concurrent Restore (also EXCLUSIVE) or another migrating Open simply contends
// on the exclusive lock — whoever wins runs, the others wait/fail closed. A
// Restore can never swap the DB "under" this connection here because no SQLite
// statements run in that gap; the connection's queries resume only after we are
// back to (re-acquired) shared.
//
// Callers must hold db.lock (the lifetime shared lock). :memory:/URI opens have
// no lock; for them this is a no-op.
// acquireExclusiveLockForOwner takes the EXCLUSIVE lock for a *DB, correctly
// handling whether that DB already holds a lifetime SHARED lock. When it does,
// this performs the release-exclusive-reacquire-shared dance (so it cannot
// self-deadlock on its own shared fd / RLock); when it does not (a test handle,
// or a :memory: DB), it takes a standalone exclusive lock released directly.
// It is the single entry point migrate() and createRestorePoint() both use.
func acquireExclusiveLockForOwner(db *DB) (func(), error) {
	return db.acquireMigrateExclusive()
}

func (db *DB) acquireMigrateExclusive() (func(), error) {
	if db.lock == nil || db.lock.mu == nil {
		// No lifetime lock (e.g. :memory:): nothing to upgrade. Take a standalone
		// exclusive lock for cross-process serialization where a real path exists,
		// otherwise a no-op.
		ex, err := acquireExclusiveLock(db.Path)
		if err != nil {
			return nil, err
		}
		return ex.release, nil
	}

	// Release the lifetime SHARED hold (both flock fd and in-process RLock) so the
	// fresh EXCLUSIVE acquire is not blocked by our own shared fd / RLock.
	db.lock.release()
	db.lock = nil

	ex, err := acquireExclusiveLock(db.Path)
	if err != nil {
		// Failed to take exclusive: restore the SHARED lifetime hold so the
		// connection is not left lock-less, then surface the failure.
		if sh, serr := acquireSharedLock(db.Path); serr == nil {
			db.lock = sh
		}
		return nil, err
	}

	return func() {
		ex.release()
		// Re-acquire the lifetime SHARED hold for the rest of the connection.
		if sh, serr := acquireSharedLock(db.Path); serr == nil {
			db.lock = sh
		}
	}, nil
}
