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
	st := serviceState{installed: true, kind: "systemd", status: mgrUnknown}
	out, err := exec.Command("systemctl", "--user", "is-active", "continuity").CombinedOutput()
	state := strings.TrimSpace(string(out))
	// `systemctl is-active` exits non-zero for inactive/failed units, so a
	// non-nil err is EXPECTED and not itself a probe failure — branch on the
	// printed state. A recognized state ("active"/"inactive"/"failed"/...) is a
	// trustworthy answer; empty output (systemctl missing, no output) is a
	// genuine probe failure and stays unknown so we route through the manager,
	// never to a bare kill.
	switch {
	case state == "active":
		st.status = mgrActive
	case state == "":
		st.status = mgrUnknown
	default:
		// inactive, failed, activating, deactivating, etc. — definitively "not
		// active"; treat as a known inactive unit to (re)start via the manager.
		st.status = mgrInactive
	}
	_ = err
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
