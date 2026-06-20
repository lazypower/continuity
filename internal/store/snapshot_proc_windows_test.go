//go:build windows

package store

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWindowsExReleaseThenSharedAcquire is the Round 19 Finding 3 regression for
// Windows. The prior EX→SH "bridge" downgrade took a SHARED lock on a sub-range
// (byte 1) that OVERLAPPED the [0,2) EXCLUSIVE range the same process held, which
// LockFileEx rejects with ERROR_LOCK_VIOLATION — so risky migrations could not
// complete their downgrade on Windows at all. The bridge is removed; the risky
// upgrade now RELEASES exclusive and then RE-ACQUIRES shared with no open handle
// across the gap. This test exercises that exact lock cycle on a single lock file:
// take EXCLUSIVE non-blocking, release it (close the handle), then take SHARED
// non-blocking on a fresh handle. With the removed-bridge model both steps
// succeed; the old overlapping-sub-range bridge would have failed the transition.
func TestWindowsExReleaseThenSharedAcquire(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "continuity.db.lock")

	ex, err := openLockFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if ok, lerr := flockExclusiveNB(ex); lerr != nil || !ok {
		t.Fatalf("acquire exclusive: ok=%v err=%v", ok, lerr)
	}
	// Release exclusive by closing the handle (the canonical drop, as dbLockHandle
	// does). No bridge, no overlapping sub-range lock.
	if cerr := ex.Close(); cerr != nil {
		t.Fatalf("close exclusive handle: %v", cerr)
	}

	// Re-acquire SHARED on a fresh handle — this must now succeed (the regression was
	// that the bridge's overlapping byte-1 SHARED lock failed with ERROR_LOCK_VIOLATION).
	sh, err := openLockFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()
	ok, lerr := flockSharedNB(sh)
	if lerr != nil {
		t.Fatalf("acquire shared after exclusive release errored: %v", lerr)
	}
	if !ok {
		t.Fatal("shared acquire after exclusive release was refused; the EX→SH transition is broken on Windows")
	}

	_ = sh.Close()
	_ = os.Remove(lockPath)
}
