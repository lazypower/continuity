//go:build darwin

package cli

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// platformServiceState reports how (if at all) the continuity service is managed
// on macOS: whether a LaunchAgent plist is installed and whether launchd has it
// loaded. "managerActive" gates the kickstart path; an installed-but-unloaded
// plist is treated as bare (or start-via-load).
func platformServiceState() serviceState {
	path, err := plistPath()
	if err != nil {
		return serviceState{kind: "launchd"}
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return serviceState{kind: "launchd"}
	}
	st := serviceState{installed: true, kind: "launchd"}
	out, err := exec.Command("launchctl", "list").CombinedOutput()
	if err == nil && strings.Contains(string(out), launchAgentLabel) {
		st.managerActive = true
	}
	return st
}

// platformServiceRestart bounces the loaded LaunchAgent. kickstart -k stops the
// service if running and (re)starts it, letting launchd manage the lifecycle —
// we never kill the PID ourselves in managed mode.
func platformServiceRestart() error {
	if _, err := exec.LookPath("launchctl"); err != nil {
		return fmt.Errorf("launchctl not found")
	}
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), launchAgentLabel)
	out, err := exec.Command("launchctl", "kickstart", "-k", target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("launchctl kickstart %s: %w\n%s", target, err, out)
	}
	return nil
}

// platformServiceStart loads (and thus starts, via RunAtLoad) the installed
// LaunchAgent. Used when the plist exists but launchd has it unloaded.
func platformServiceStart() error {
	if _, err := exec.LookPath("launchctl"); err != nil {
		return fmt.Errorf("launchctl not found")
	}
	path, err := plistPath()
	if err != nil {
		return err
	}
	// If it's already loaded, kickstart it; otherwise load it.
	if platformServiceState().managerActive {
		return platformServiceRestart()
	}
	if out, err := exec.Command("launchctl", "load", path).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl load %s: %w\n%s", path, err, out)
	}
	return nil
}
