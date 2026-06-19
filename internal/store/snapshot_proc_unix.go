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

// flockDowngradeToShared ATOMICALLY converts an exclusive (LOCK_EX) lock held on
// f's open-file-description down to shared (LOCK_SH), WITHOUT an intervening
// unlocked window (Finding 1, Round 6). flock(2) permits applying LOCK_SH to an
// fd already holding LOCK_EX as a single in-kernel transition: the lock is never
// fully released, so no other process can slip an exclusive acquire in between.
// This is what lets a risky migration run under EXCLUSIVE and then hand the
// connection a lifetime SHARED hold on the SAME fd with no cross-process gap.
func flockDowngradeToShared(f *os.File) error {
	// Blocking form (no LOCK_NB): downgrading EX→SH never has to wait — we already
	// hold the stronger lock — so this returns immediately, and retrying on EINTR
	// keeps a signal from spuriously failing the transition.
	return flockRetryEINTR(f, syscall.LOCK_SH)
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
