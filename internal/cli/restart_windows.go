//go:build windows

package cli

import "fmt"

// platformServiceState reports no service management on Windows.
func platformServiceState() serviceState {
	return serviceState{}
}

func platformServiceRestart() error {
	return fmt.Errorf("restart is not supported on Windows")
}

func platformServiceStart() error {
	return fmt.Errorf("restart is not supported on Windows")
}

// stopPID is unsupported on Windows (no POSIX signals); bare restart is not
// available there.
func stopPID(pid int) error {
	return fmt.Errorf("restart is not supported on Windows")
}

func respawnServer() error {
	return fmt.Errorf("restart is not supported on Windows")
}
