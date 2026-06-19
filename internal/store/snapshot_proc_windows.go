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
// locking the whole file for every continuity process.
const lockRangeLen = 1

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
