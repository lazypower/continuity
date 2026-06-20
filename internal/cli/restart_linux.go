//go:build linux

package cli

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// platformServiceState reports how (if at all) the continuity service is managed
// on Linux: whether a systemd user unit is installed and whether systemd
// reports it active. An installed-but-inactive unit is treated as bare (or
// start-via-systemctl).
func platformServiceState() serviceState {
	path, err := unitPath()
	if err != nil {
		return serviceState{kind: "systemd"}
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return serviceState{kind: "systemd"}
	}
	st := serviceState{installed: true, kind: "systemd"}
	out, err := exec.Command("systemctl", "--user", "is-active", "continuity").CombinedOutput()
	if err == nil && strings.TrimSpace(string(out)) == "active" {
		st.managerActive = true
	}
	return st
}

// platformServiceRestart bounces the systemd user unit, letting systemd manage
// the lifecycle — we never kill the PID ourselves in managed mode.
func platformServiceRestart() error {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return fmt.Errorf("systemctl not found")
	}
	out, err := exec.Command("systemctl", "--user", "restart", "continuity").CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl --user restart continuity: %w\n%s", err, out)
	}
	return nil
}

// platformServiceStart starts the installed systemd user unit. Used when the
// unit exists but is not currently active.
func platformServiceStart() error {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return fmt.Errorf("systemctl not found")
	}
	out, err := exec.Command("systemctl", "--user", "start", "continuity").CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl --user start continuity: %w\n%s", err, out)
	}
	return nil
}
