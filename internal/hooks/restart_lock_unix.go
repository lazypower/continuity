//go:build !windows

package hooks

import (
	"os"
	"syscall"
)

// osProcessAlive reports whether pid names a live process. On unix, signal 0 is
// the canonical liveness probe: it performs error checking (ESRCH = no such
// process, EPERM = exists but not ours) without actually delivering a signal.
// EPERM means the process exists under another uid, which still counts as alive.
func osProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	return err == syscall.EPERM
}
