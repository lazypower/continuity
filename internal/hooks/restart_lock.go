package hooks

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The bounce critical section (re-validate identity -> signal -> respawn) must be
// serialized across EVERY caller: `continuity restart`'s bare path and the
// session-start hook auto-bounce can otherwise run concurrently (two session
// hooks, or a hook racing a manual restart) and both confirm the same pid, both
// SIGTERM, then two respawns fight over the port. The lock lives here, in the
// shared kill-path layer, so it wraps ONLY that critical section and both callers
// are protected by one lock at one location.
//
// Lock file: ~/.continuity/restart.lock, created O_CREATE|O_EXCL with the owner
// pid written inside. On acquire, a pre-existing lock is reaped only if its owner
// process is no longer alive (pid liveness), with a generous time-based backstop
// for the can't-tell case. Everything degrades safely (proceed, or a clear
// error) on FS errors — never a panic — because locking is a safety nicety for a
// single-user localhost tool, not a correctness gate.

// restartLockStaleAfter bounds how long a lock whose owner we CANNOT classify
// (unreadable pid, stat that races) is honored. A real bounce (graceful stop +
// respawn + health poll) finishes well within this; an older one is assumed
// orphaned and reclaimed. Owner-pid liveness is the primary signal; this is only
// the backstop.
const restartLockStaleAfter = 2 * time.Minute

// lockOwnerAlive reports whether the process that owns the lock (pid) is still
// running. Injectable so tests can drive the "owner alive" vs "owner dead" reap
// branches without spawning real processes. The production implementation is
// platform-specific (see restart_lock_unix.go / restart_lock_windows.go).
var lockOwnerAlive = osProcessAlive

// errRestartLockHeld is returned when another live restart/bounce holds the lock.
// Callers distinguish it from FS errors: the CLI surfaces a clean message, the
// hook warns and skips (a concurrent bounce is already handling the work).
type errRestartLockHeld struct {
	path string
	pid  int
}

func (e *errRestartLockHeld) Error() string {
	if e.pid > 0 {
		return fmt.Sprintf("another continuity restart/bounce is in progress (pid %d, lock: %s)", e.pid, e.path)
	}
	return fmt.Sprintf("another continuity restart/bounce is in progress (lock: %s)", e.path)
}

// IsRestartLockHeld reports whether err indicates the restart/bounce lock is held
// by another live invocation (as opposed to a generic failure). Callers use this
// to decide between "refuse with a clean message" (CLI) and "warn and skip"
// (hook) versus surfacing an unexpected error.
func IsRestartLockHeld(err error) bool {
	var held *errRestartLockHeld
	return asLockHeld(err, &held)
}

func asLockHeld(err error, target **errRestartLockHeld) bool {
	for err != nil {
		if h, ok := err.(*errRestartLockHeld); ok {
			*target = h
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// acquireRestartLock takes the per-user advisory restart lock. It returns a
// release function (safe to defer) on success. If the lock is held by a LIVE
// owner it returns an *errRestartLockHeld (use IsRestartLockHeld). A stale lock
// (owner dead, or unclassifiable + older than restartLockStaleAfter) is reaped
// and re-acquired. FS errors that prevent locking degrade to a no-op lock so a
// legitimate bounce is never blocked by an inability to lock.
func acquireRestartLock() (func(), error) {
	home, err := os.UserHomeDir()
	if err != nil {
		// Can't locate the lock dir; proceed without locking rather than block a
		// legitimate restart. Locking is a safety nicety, not a correctness gate.
		return func() {}, nil
	}
	dir := filepath.Join(home, ".continuity")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return func() {}, nil
	}
	path := filepath.Join(dir, "restart.lock")
	return acquireRestartLockAt(path)
}

// acquireRestartLockAt is acquireRestartLock against an explicit path (so tests
// can point it at a temp dir without juggling HOME).
func acquireRestartLockAt(path string) (func(), error) {
	tryCreate := func() (*os.File, error) {
		return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	}

	f, err := tryCreate()
	if err != nil {
		if !os.IsExist(err) {
			// Unexpected FS error (not "already exists"): don't block the bounce.
			return func() {}, nil
		}
		// Lock exists. Reclaim it only if it is clearly stale.
		if !lockIsStale(path) {
			return nil, &errRestartLockHeld{path: path, pid: readLockOwner(path)}
		}
		_ = os.Remove(path)
		f, err = tryCreate()
		if err != nil {
			if os.IsExist(err) {
				// Someone raced us to re-create it — treat as held.
				return nil, &errRestartLockHeld{path: path, pid: readLockOwner(path)}
			}
			return func() {}, nil // unexpected FS error: don't block the bounce
		}
	}

	_, _ = fmt.Fprintf(f, "pid %d\n", os.Getpid())
	_ = f.Close()

	var released bool
	return func() {
		if released {
			return
		}
		released = true
		_ = os.Remove(path)
	}, nil
}

// lockIsStale decides whether an existing lock may be reaped. Primary signal:
// the owner pid is no longer alive. Backstop: we can't read/classify the owner
// (corrupt file, pid 0) AND the file is older than restartLockStaleAfter. A lock
// whose owner is alive, or which we can't classify and is recent, is NOT stale.
func lockIsStale(path string) bool {
	pid := readLockOwner(path)
	if pid > 0 {
		// We know the owner; liveness is authoritative.
		return !lockOwnerAlive(pid)
	}
	// Unknown owner (unreadable/corrupt/missing pid). Fall back to age.
	info, err := os.Stat(path)
	if err != nil {
		// Can't even stat it; let the re-create attempt sort it out. Treat as stale
		// so we don't wedge forever on an unreadable lock.
		return true
	}
	return time.Since(info.ModTime()) >= restartLockStaleAfter
}

// readLockOwner parses the owner pid from a "pid N\n" lock file. Returns 0 when
// the file is missing, unreadable, or doesn't contain a parseable pid.
func readLockOwner(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 || fields[0] != "pid" {
		return 0
	}
	pid, err := strconv.Atoi(fields[1])
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}
