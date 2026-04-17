//go:build linux

package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	systemdUnitName = "continuity.service"
)

func unitPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "systemd", "user", systemdUnitName), nil
}

func generateUnit() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve binary path: %w", err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks: %w", err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("home dir: %w", err)
	}
	logPath := filepath.Join(home, ".continuity", "serve.log")

	return fmt.Sprintf(`[Unit]
Description=Continuity - Persistent memory for AI coding agents
After=network.target

[Service]
Type=simple
ExecStart=%s serve
Restart=on-failure
RestartSec=5
StandardOutput=append:%s
StandardError=append:%s

[Install]
WantedBy=default.target
`, self, logPath, logPath), nil
}

func platformServiceStatus() (installed bool, status string) {
	path, err := unitPath()
	if err != nil {
		return false, ""
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, ""
	}

	out, err := exec.Command("systemctl", "--user", "is-active", "continuity").CombinedOutput()
	if err == nil {
		state := strings.TrimSpace(string(out))
		return true, fmt.Sprintf("  Status: %s", state)
	}
	return true, "  Status: installed but not active"
}

func platformServicePlan() (string, error) {
	// Check for systemctl
	if _, err := exec.LookPath("systemctl"); err != nil {
		return "", fmt.Errorf("systemctl not found — systemd is required for install-service on Linux")
	}

	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve binary: %w", err)
	}
	self, _ = filepath.EvalSymlinks(self)

	path, err := unitPath()
	if err != nil {
		return "", err
	}

	home, _ := os.UserHomeDir()
	logPath := filepath.Join(home, ".continuity", "serve.log")

	return fmt.Sprintf(`Continuity will install a system service:

  Platform:  Linux (systemd user unit)
  Binary:    %s
  Service:   %s
  Behavior:  Start on login, restart on failure (5s delay)
  Logs:      %s
`, self, path, logPath), nil
}

func platformServiceInstall() (string, error) {
	path, err := unitPath()
	if err != nil {
		return "", err
	}

	// Ensure unit directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", fmt.Errorf("create systemd user dir: %w", err)
	}

	// Ensure log directory exists
	home, _ := os.UserHomeDir()
	os.MkdirAll(filepath.Join(home, ".continuity"), 0755)

	content, err := generateUnit()
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write unit: %w", err)
	}

	// Reload and enable
	if out, err := exec.Command("systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
		return "", fmt.Errorf("daemon-reload: %w\n%s", err, out)
	}
	if out, err := exec.Command("systemctl", "--user", "enable", "--now", "continuity").CombinedOutput(); err != nil {
		return "", fmt.Errorf("enable service: %w\n%s", err, out)
	}

	return fmt.Sprintf(`Installed. Continuity is now running as a system service.
  Status:  systemctl --user status continuity
  Stop:    systemctl --user stop continuity
  Remove:  continuity uninstall-service`), nil
}

func platformUninstallPlan() (string, error) {
	path, err := unitPath()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`Will remove continuity system service:

  Service:  %s
  Action:   Disable and stop via systemctl, delete unit file
`, path), nil
}

func platformServiceRemove() error {
	// Disable and stop (ignore errors — may not be running)
	exec.Command("systemctl", "--user", "disable", "--now", "continuity").Run()

	path, err := unitPath()
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove unit: %w", err)
	}

	// Reload daemon
	exec.Command("systemctl", "--user", "daemon-reload").Run()
	return nil
}
