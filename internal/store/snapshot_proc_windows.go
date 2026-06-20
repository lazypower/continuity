//go:build windows

package store

import (
	"os"

	"golang.org/x/sys/windows"
)

// =========================================================================
// LockFileEx/UnlockFileEx-based advisory DB lock primitives (windows).
//
// Windows has no flock(2); LockFileEx provides the equivalent shared/exclusive
// byte-range lock on a file handle. Like flock, the lock is released when the
// handle is closed AND when the owning process exits, so a crashed holder never
// wedges a peer. We lock a single fixed byte range over the whole lock file.
// =========================================================================

// lockRangeLen is the byte range locked on the lock file. The range is fixed
// and the lock file is dedicated, so locking [0, lockRangeLen) is equivalent to
// locking the whole file for every continuity process. It spans TWO bytes so the
// EX→SH downgrade can bridge through a sub-range (byte 1) without ever fully
// unlocking — see flockDowngradeToShared (Round 7, Finding 3): a foreign
// EXCLUSIVE acquirer must lock the WHOLE [0,2) range, so a shared hold on byte 1
// kept across the downgrade is enough to keep it out continuously.
const lockRangeLen = 2

// bridgeOffset / bridgeLen name the sub-range (byte 1) the windows downgrade
// holds SHARED on a SECOND handle BEFORE releasing the exclusive [0,2) lock, so
// an inter-process lock is held CONTINUOUSLY across the non-atomic EX→SH
// transition. A foreign exclusive acquirer locks all of [0,2) and therefore
// still conflicts with this byte while the primary range is momentarily between
// exclusive-release and shared-relock (Round 7, Finding 3).
const (
	bridgeOffset = 1
	bridgeLen    = 1
)

// flockShared takes a shared lock, BLOCKING until granted (LockFileEx without
// LOCKFILE_FAIL_IMMEDIATELY waits for a conflicting exclusive holder).
func flockShared(f *os.File) error {
	ol := new(windows.Overlapped)
	return windows.LockFileEx(windows.Handle(f.Fd()), 0, 0, lockRangeLen, 0, ol)
}

// flockSharedNB attempts a shared lock NON-BLOCKING. Returns (true, nil) when
// granted, (false, nil) when an exclusive holder conflicts, (false, err) on any
// other error.
func flockSharedNB(f *os.File) (bool, error) {
	ol := new(windows.Overlapped)
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, lockRangeLen, 0, ol,
	)
	if err == nil {
		return true, nil
	}
	if err == windows.ERROR_LOCK_VIOLATION || err == windows.ERROR_IO_PENDING {
		return false, nil
	}
	return false, err
}

// flockExclusiveNB attempts an exclusive lock NON-BLOCKING. Returns (true, nil)
// when granted, (false, nil) when another holder conflicts
// (ERROR_LOCK_VIOLATION / ERROR_IO_PENDING with FAIL_IMMEDIATELY), and
// (false, err) on any other error.
func flockExclusiveNB(f *os.File) (bool, error) {
	ol := new(windows.Overlapped)
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, lockRangeLen, 0, ol,
	)
	if err == nil {
		return true, nil
	}
	if err == windows.ERROR_LOCK_VIOLATION || err == windows.ERROR_IO_PENDING {
		return false, nil
	}
	return false, err
}

// hookWindowsDowngradeGap is a TEST-ONLY seam (nil in production) fired inside
// the windows EX→SH downgrade in the window between the exclusive release and the
// shared re-lock. See TestFlockDowngrade_NoForeignExclusiveInGap (windows).
var hookWindowsDowngradeGap func(lockPath string)

// flockDowngradeToShared converts the exclusive lock held on h.f down to shared
// with NO fully-unlocked cross-process window (Round 7, Finding 3). Windows has
// no single-call atomic EX→SH transition the way flock(2) does, so a naive
// unlock-then-relock would open a gap in which a concurrent restore could grab
// EXCLUSIVE while the migrated SQLite conn is still live and rename the DB out
// from under it (the round-6 race, back on Windows). We close that gap by keeping
// an inter-process lock held CONTINUOUSLY:
//
//  1. On a SECOND handle to the same lock file, take a SHARED lock on the bridge
//     sub-range (byte 1). We hold EXCLUSIVE on the whole [0,2) range, but a
//     SHARED request on a sub-range from a DIFFERENT handle of the same process
//     is compatible with our own exclusive lock on Windows, so this succeeds.
//  2. Release the EXCLUSIVE lock on the primary [0,2) range.
//  3. Re-take a SHARED lock on the primary [0,2) range on the original handle.
//
// Between steps 2 and 3 the primary range is momentarily unlocked, but a foreign
// EXCLUSIVE acquirer must lock the WHOLE [0,2) range and therefore still
// conflicts with the bridge SHARED lock on byte 1 — so no foreign exclusive can
// be granted at any instant. The bridge handle is parked on h.bridge and closed
// by release(). The in-process RWMutex (held write by the caller) covers
// same-process goroutines as before.
func flockDowngradeToShared(h *dbLockHandle) error {
	f := h.f
	// Step 1: SHARED on the bridge sub-range via a second handle, BEFORE releasing
	// exclusive — so an inter-process lock is held continuously.
	bridge, err := openLockFile(f.Name())
	if err != nil {
		return err
	}
	bridgeOL := new(windows.Overlapped)
	bridgeOL.Offset = bridgeOffset
	if err := windows.LockFileEx(windows.Handle(bridge.Fd()), 0, 0, bridgeLen, 0, bridgeOL); err != nil {
		_ = bridge.Close()
		return err
	}

	// Step 2: release EXCLUSIVE on the primary range. The bridge SHARED lock on
	// byte 1 still excludes any foreign EXCLUSIVE acquirer of [0,2).
	relOL := new(windows.Overlapped)
	if err := windows.UnlockFileEx(windows.Handle(f.Fd()), 0, lockRangeLen, 0, relOL); err != nil {
		_ = bridge.Close()
		return err
	}

	// TEST SEAM (Round 7, Finding 3): fires in the exact window between releasing
	// the EXCLUSIVE primary lock and re-taking SHARED on it — the gap an
	// unlock-before-relock regression would expose. A test asserts a foreign
	// EXCLUSIVE acquire is STILL refused here (the bridge byte holds it out). nil in
	// production.
	if hookWindowsDowngradeGap != nil {
		hookWindowsDowngradeGap(f.Name())
	}

	// Step 3: re-take SHARED on the primary range on the original handle.
	shOL := new(windows.Overlapped)
	if err := windows.LockFileEx(windows.Handle(f.Fd()), 0, 0, lockRangeLen, 0, shOL); err != nil {
		_ = bridge.Close()
		return err
	}

	h.bridge = bridge
	return nil
}

// openNoFollow opens path for reading without following a final-component
// symlink (Round 7, Findings 1 & 2). Windows reparse points are the symlink
// analogue; FILE_FLAG_OPEN_REPARSE_POINT opens the link itself rather than its
// target, so a subsequent regular-file check (see hashFileNoFollow) fails closed
// on a redirection a forged marker planted, matching the unix O_NOFOLLOW path.
func openNoFollow(path string) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(
		p,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}

// openControlFileNoFollow opens a sidecar control file without following a
// reparse point (the Windows symlink analogue) so a planted symlink/junction at
// manifest.json / restore.in-progress.json fails the caller's regular-file check
// rather than redirecting the read (Round 9, Finding 6). Windows named pipes are
// not created at arbitrary filesystem paths the way unix FIFOs are, so the unix
// O_NONBLOCK concern does not apply; opening the reparse point itself is enough.
func openControlFileNoFollow(path string) (*os.File, error) {
	return openNoFollow(path)
}

// platformFsyncDir is a NO-OP on Windows (Round 13, Finding 1). Calling File.Sync
// on a DIRECTORY handle errors on Windows (CreateFile + FlushFileBuffers on a
// directory is unsupported), so the unix-style open+Sync would make every
// fatal-on-failure caller — restore-point publication (fsyncSnapshotDir), the
// restore move-aside/publish dir-fsyncs, and the recovery scrub — spuriously ABORT
// on Windows. NTFS metadata durability is handled by the OS/filesystem rather than
// an explicit directory-handle flush, and FILE fsync (fsyncFile) still runs on
// Windows for the snapshot/manifest BYTES, so returning nil here keeps directory
// fsync from being a false durability failure while file-level durability is
// preserved. Mirrors the unix platformFsyncDir which performs the real Sync.
func platformFsyncDir(dir string) error {
	return nil
}
