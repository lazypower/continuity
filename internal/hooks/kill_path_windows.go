//go:build windows

package hooks

import "fmt"

// osExeMatch is indeterminate on Windows (no /proc); the bounce path is
// unsupported there anyway, so this is never reached in practice.
func osExeMatch(pid int, wantExe string) (bool, error) {
	return false, nil
}

// osStopPID is unsupported on Windows (no POSIX signals / detached respawn).
func osStopPID(pid int) error {
	return fmt.Errorf("bare bounce is not supported on Windows")
}
