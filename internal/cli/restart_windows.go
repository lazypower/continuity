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
