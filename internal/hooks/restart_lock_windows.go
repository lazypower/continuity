//go:build windows

package hooks

import "os"

// osProcessAlive reports whether pid names a live process on Windows. The bare
// bounce path is unsupported on Windows, so this lock is effectively unused
// there; FindProcess on Windows opens a handle and fails for a non-existent pid,
// which is a good-enough liveness check to keep the shared code compiling.
func osProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = proc.Release()
	return true
}
