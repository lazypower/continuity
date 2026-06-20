//go:build !windows

package store

import (
	"errors"
	"os"
	"syscall"
)

// =========================================================================
// flock(2)-based advisory DB lock primitives (unix).
//
// These replace the hand-rolled PID-liveness machinery. flock is kernel-managed
// (no zero-length sentinel window) and auto-releases on close AND on process
// death, so a crashed holder never wedges a peer. The lock is held on the
// open-file-description of the passed *os.File; closing it releases the lock.
// =========================================================================

// flockShared takes a shared (LOCK_SH) advisory lock, BLOCKING until it can be
// granted (a cross-process exclusive holder must release first). Retained for
// callers/tests that want the blocking form.
func flockShared(f *os.File) error {
	return flockRetryEINTR(f, syscall.LOCK_SH)
}

// flockSharedNB attempts a shared (LOCK_SH) advisory lock NON-BLOCKING. Returns
// (true, nil) when granted, (false, nil) when a cross-process EXCLUSIVE holder is
// present (EWOULDBLOCK), and (false, err) on any other error. The bounded-wait
// retry loop lives in acquireSharedLock so a writable open does not hang forever
// behind an in-progress exclusive restore but instead fails closed.
func flockSharedNB(f *os.File) (bool, error) {
	err := flockRetryEINTR(f, syscall.LOCK_SH|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return false, nil
	}
	return false, err
}

// flockExclusiveNB attempts an exclusive (LOCK_EX) advisory lock NON-BLOCKING.
// Returns (true, nil) when granted, (false, nil) when another process holds a
// shared or exclusive lock (EWOULDBLOCK), and (false, err) on any other error.
// The bounded-wait retry loop lives in acquireExclusiveLock.
func flockExclusiveNB(f *os.File) (bool, error) {
	err := flockRetryEINTR(f, syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return false, nil
	}
	return false, err
}

// flockRetryEINTR calls flock(2) and retries on EINTR (a signal can interrupt a
// blocking flock). The fd comes from *os.File so it stays valid for the call.
func flockRetryEINTR(f *os.File, how int) error {
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

// openNoFollow opens path for reading WITHOUT following a final-component
// symlink (O_NOFOLLOW). A symlink at path makes open(2) fail with ELOOP, which
// is exactly the fail-closed behaviour recovery wants: it must never hash or
// open a file through a redirection a forged marker planted. Used by
// hashFileNoFollow so the recovery destructive paths never follow a symlink the
// way the symlink-following hashFile would (Round 7, Findings 1 & 2).
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

// createExclNoFollow CREATES a brand-new file at path with O_CREATE|O_EXCL and
// O_NOFOLLOW (Round 19, Finding 1). O_EXCL fails if anything already exists at the
// path (so we can never clobber a planted file), and O_NOFOLLOW makes a
// pre-planted SYMLINK at the final component fail the create with ELOOP rather
// than being followed to write through the link. The returned file is provably a
// fresh, regular, 0-byte file we own. Used by the VACUUM-INTO temp reservation so
// the reserved path is a real file we created (not removed-then-recreated), which
// closes the remove→open symlink-plant window. 0600.
func createExclNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
}

// openControlFileNoFollow opens a sidecar control file for reading without
// following a final-component symlink AND non-blocking (Round 9, Finding 6).
// O_NOFOLLOW makes a symlink fail with ELOOP; O_NONBLOCK ensures a FIFO planted at
// the path returns from open(2) immediately (a blocking O_RDONLY open of a FIFO
// with no writer would HANG forever) so the caller's fstat regular-file check can
// reject it as a corrupt sidecar. A regular file is unaffected by O_NONBLOCK.
func openControlFileNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

// platformFsyncDir fsyncs a directory handle on unix — REAL durability (Round 13,
// Finding 1). It opens the directory and Sync()s the handle so a rename/creation
// inside it survives power loss. On unix this is supported and meaningful, so its
// failure is propagated and the fatal-on-failure callers (restore-point publication,
// restore move-aside/publish, recovery scrub) abort rather than report a non-durable
// success. The windows build supplies a NO-OP instead (directory-handle fsync is
// unsupported there).
func platformFsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if serr := d.Sync(); serr != nil {
		d.Close()
		return serr
	}
	return d.Close()
}
