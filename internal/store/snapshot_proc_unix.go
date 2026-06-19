//go:build !windows

package store

import (
	"errors"
	"os"
	"syscall"
)

// processAlive reports whether a process with the given PID is currently
// alive. On unix we send signal 0, which performs error checking without
// delivering a signal: a nil error (or EPERM) means the process exists; ESRCH
// means it does not. Used by the serve lock to distinguish a live serve from
// a stale lockfile left by a crashed one.
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	// EPERM: process exists but we may not signal it — still alive.
	return errors.Is(err, syscall.EPERM)
}
