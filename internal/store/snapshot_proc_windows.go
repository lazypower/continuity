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
//
// NOTE (Round 19, Finding 3): this was previously TWO bytes to give the EX→SH
// downgrade a "bridge" sub-range to hold across the transition. The downgrade /
// bridge is REMOVED (the bridge SHARED lock on byte 1 overlapped this EXCLUSIVE
// range and failed with ERROR_LOCK_VIOLATION, breaking risky migrations on
// Windows). The risky upgrade now closes its conn, releases exclusive, and
// re-acquires shared with no open handle across the gap, so a single locked byte
// is sufficient.
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

// NOTE (Round 19, Finding 3): the Windows flockDowngradeToShared bridge is
// REMOVED. It attempted a SHARED lock on a sub-range (byte 1) of the [0,2)
// EXCLUSIVE range held on the same lock file; LockFileEx rejects a shared request
// that overlaps a range the process already holds EXCLUSIVE with
// ERROR_LOCK_VIOLATION, so the downgrade failed and risky migrations could not
// complete on Windows. The risky upgrade now closes its conn, releases exclusive,
// and re-acquires shared with no open SQLite handle across the gap
// (openRiskyUpgradeUnderExclusive), so no in-kernel atomic downgrade is needed and
// the path is identical on unix and Windows.

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

// createExclNoFollow CREATES a brand-new file at path, failing if anything already
// exists there, without following a reparse point (the Windows symlink analogue)
// (Round 19, Finding 1). CREATE_NEW is the O_CREATE|O_EXCL analogue: it fails with
// ERROR_FILE_EXISTS if any entry (regular file, directory, OR reparse point) is
// already present, so we can never clobber a planted file. FILE_FLAG_OPEN_REPARSE_POINT
// ensures that if a reparse point somehow occupies the path the create acts on the
// link itself (and CREATE_NEW then fails) rather than following it. The returned
// file is a fresh, owned, 0-byte regular file. Used by the VACUUM-INTO temp
// reservation so the reserved path is a real file we created (not removed-then-
// recreated), closing the remove→open symlink-plant window.
func createExclNoFollow(path string) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(
		p,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.CREATE_NEW, // fail if the path already exists (O_EXCL analogue)
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
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
