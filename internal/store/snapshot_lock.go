package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// serveLockSuffix is appended to the canonical DB path to derive the DEDICATED
// serve-exclusion lock file (Round 7, Finding 4). It is SEPARATE from the DB
// shared/exclusive lock so it does NOT block ordinary CLI commands (which only
// take the DB shared lock): ONLY `serve` contends on it, EXCLUSIVELY, so a second
// serve for the same DB refuses to start. This restores the single-serve
// invariant the flock model lost — with multiple serves all taking the DB SHARED
// lock, each successful boot ticked RecordSuccessfulBoot, so N concurrent serves
// = N ticks and the restore point could expire early. With the dedicated lock,
// only one serve runs per DB, so boot retention counts independent serve sessions
// again, not concurrent starts.
const serveLockSuffix = ".serve.lock"

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
	// bridge is a SECOND lock-file handle the windows EX→SH downgrade holds a
	// SHARED sub-range lock on so an inter-process lock is held continuously across
	// the non-atomic downgrade (Round 7, Finding 3). nil on unix (the atomic
	// flock downgrade needs no bridge) and until a downgrade runs. Closed by
	// release alongside f.
	bridge *os.File
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
		if h.bridge != nil {
			// The windows downgrade's second (bridge) handle, if any. Closing it
			// releases its shared sub-range lock.
			_ = h.bridge.Close()
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

// acquireExclusiveLockForOwner takes a STANDALONE EXCLUSIVE lock for a *DB that
// does NOT hold a lifetime SHARED lock (a test handle from openWritableNoMigrate,
// or a :memory: DB). It is the single entry point createRestorePoint() uses when
// it does not already hold the exclusive lock.
//
// CONTRACT (Finding 1, Round 6): the caller's *DB must NOT hold a lifetime SHARED
// lock. The old "upgrade a live shared open to exclusive then downgrade back"
// dance is GONE — it released SHARED while a live *sql.DB conn was open, the exact
// no-open-handle-across-lock-transition violation this round fixes. The risky-
// migration open now closes its conn and releases SHARED in Open() BEFORE any
// exclusive acquire (see openRiskyUpgradeUnderExclusive), so the only callers that
// reach here are lock-less. A *DB that DOES hold a lifetime lock is a contract
// violation and fails closed rather than re-introducing the dangerous dance.
func acquireExclusiveLockForOwner(db *DB) (func(), error) {
	if db.lock != nil && db.lock.mu != nil {
		return nil, fmt.Errorf(
			"internal: acquireExclusiveLockForOwner called on a DB holding a lifetime " +
				"shared lock; the risky-upgrade path must close the conn and release " +
				"shared before acquiring exclusive (no open handle across lock transition)")
	}
	// No lifetime lock (e.g. :memory: or a test handle): take a standalone
	// exclusive lock for cross-process serialization where a real path exists,
	// otherwise a no-op.
	ex, err := acquireExclusiveLock(db.Path)
	if err != nil {
		return nil, err
	}
	return ex.release, nil
}

// ErrServeLockHeld is returned by AcquireServeLock when another `serve` process
// already holds the dedicated serve lock for this DB. The second serve fails
// closed (refuses to start) rather than coexisting and double-ticking boot
// retention (Round 7, Finding 4).
var ErrServeLockHeld = errors.New("store: another continuity serve is already running for this database")

// ServeLock is the handle returned by AcquireServeLock. Release (or Close) drops
// the dedicated serve lock; the kernel also auto-releases it on process death, so
// a crashed serve never wedges the next one.
type ServeLock struct {
	f *os.File
}

// Release drops the dedicated serve lock. Idempotent.
func (s *ServeLock) Release() {
	if s == nil || s.f == nil {
		return
	}
	_ = s.f.Close()
	s.f = nil
}

// AcquireServeLock takes the DEDICATED, serve-only EXCLUSIVE lock for dbPath
// (Round 7, Finding 4). It is NON-BLOCKING: if another serve already holds it the
// call FAILS CLOSED immediately with ErrServeLockHeld rather than waiting, so a
// second serve for the same DB refuses to start. This lock is SEPARATE from the
// DB shared/exclusive lock store.Open/Restore use, so it does NOT serialize or
// block ordinary CLI commands — only other serves contend on it. serve still
// takes the DB SHARED lock (via store.Open) for restore-exclusion; this lock only
// makes serve sessions mutually exclusive so boot retention counts sessions, not
// concurrent starts.
//
// Ineligible paths (:memory:/URI/DSN) cannot host a lock file → a no-op handle.
func AcquireServeLock(dbPath string) (*ServeLock, error) {
	if !snapshotEligiblePath(dbPath) {
		return &ServeLock{}, nil
	}
	resolved, err := canonicalDBPath(dbPath)
	if err != nil {
		return nil, err
	}
	path := resolved + serveLockSuffix
	// Ensure the parent dir exists so the lock file can be created (a fresh DB's
	// dir is created by store.Open, but AcquireServeLock may run first).
	if mkErr := os.MkdirAll(filepath.Dir(path), 0o700); mkErr != nil {
		return nil, fmt.Errorf("store: create serve lock dir: %w", mkErr)
	}
	f, oerr := openLockFile(path)
	if oerr != nil {
		return nil, fmt.Errorf("store: open serve lock file: %w", oerr)
	}
	ok, lerr := flockExclusiveNB(f)
	if lerr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("store: serve-lock db: %w", lerr)
	}
	if !ok {
		_ = f.Close()
		return nil, ErrServeLockHeld
	}
	return &ServeLock{f: f}, nil
}

// downgradeExclusiveToShared converts the DB's currently-held EXCLUSIVE lock into
// the lifetime SHARED hold the returned connection keeps (Finding 1, Round 6),
// with NO cross-process unlocked window. It is called by openRiskyUpgradeUnderExclusive
// after the destructive DDL has run, to hand the connection a normal shared
// lifetime lock without ever releasing the flock to a foreign process.
//
// Cross-process: flockDowngradeToShared applies LOCK_SH to the SAME fd that holds
// LOCK_EX — a single in-kernel transition with no gap a second process can exploit.
// In-process: the RWMutex is not atomically downgradable, so we drop its write lock
// and take its read lock around the flock downgrade. That leaves at most an
// IN-PROCESS window, which is harmless: a same-process restore/migration would
// itself need the in-process write lock AND the flock, and the flock is never
// released to a foreign process across this call. The handle's flags flip from
// exclusive to shared so DB.Close() releases the correct in-process lock.
func (db *DB) downgradeExclusiveToShared() error {
	h := db.lock
	if h == nil || h.mu == nil || h.f == nil {
		// No real lock to downgrade (:memory:/URI/ineligible). Nothing to do —
		// these opens never carried a flock anyway.
		return nil
	}
	if !h.exclusive {
		return fmt.Errorf("internal: downgradeExclusiveToShared on a non-exclusive lock")
	}
	// Cross-process downgrade with NO unlocked window (Round 7, Finding 3). On
	// unix this is the atomic flock EX→SH on the same fd. On windows — which has
	// no atomic EX→SH — it acquires a SHARED lock on a SECOND handle BEFORE
	// releasing the EXCLUSIVE one, so an inter-process lock is held CONTINUOUSLY
	// across the transition and a concurrent restore can never grab EXCLUSIVE in a
	// gap while the migrated SQLite conn is still live. The handle h.f keeps is
	// updated to whichever fd now holds the shared lock.
	if err := flockDowngradeToShared(h); err != nil {
		return fmt.Errorf("store: flock downgrade ex->sh: %w", err)
	}
	// Transition the in-process RWMutex from write to read. The cross-process flock
	// stays held (shared) across this, so no foreign process can slip in; only same-
	// process goroutines could observe the brief gap, and they are gated by the same
	// flock + the in-process lock they would themselves need.
	h.mu.Unlock()
	h.mu.RLock()
	h.exclusive = false
	return nil
}
