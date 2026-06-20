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
	st.status = classifyIsActive(state, err)
	return st
}

// classifyIsActive maps the trimmed `systemctl --user is-active` output (and the
// command error) to a managerStatus. `is-active` exits non-zero for
// inactive/failed units, so a non-nil err is EXPECTED for those and is NOT itself
// a probe failure — we branch on the printed ActiveState.
//
// Only the KNOWN systemd ActiveState values are trusted. "active" -> mgrActive;
// the known not-running states -> mgrInactive. ANYTHING ELSE — empty output,
// unrecognized text, or diagnostics like "Failed to connect to bus..." — is a
// genuine probe failure and maps to mgrUnknown, which routes through the service
// manager and NEVER to a bare kill (the safe direction). An unrecognized state
// accompanied by err != nil is the canonical bus-error case and stays unknown.
func classifyIsActive(state string, err error) managerStatus {
	switch state {
	case "active":
		return mgrActive
	case "inactive", "failed", "activating", "deactivating", "reloading":
		// Known systemd ActiveState values that mean "not currently serving":
		// definitively not active -> (re)start via the manager.
		return mgrInactive
	default:
		// Empty output, unparseable text, or a D-Bus/connection diagnostic
		// (especially when err != nil): we could not trustworthily ask the
		// manager. Stay unknown so the caller defers to the manager, never bare.
		return mgrUnknown
	}
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
