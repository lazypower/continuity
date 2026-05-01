//go:build darwin

package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	launchAgentLabel = "com.continuity.server"
	plistFilename    = "com.continuity.server.plist"
)

func plistPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", plistFilename), nil
}

func generatePlist() (string, error) {
	self, err := resolveBinaryPath()
	if err != nil {
		return "", err
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("home dir: %w", err)
	}
	logPath := filepath.Join(home, ".continuity", "serve.log")

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>%s</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>serve</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>%s</string>
    <key>StandardErrorPath</key>
    <string>%s</string>
</dict>
</plist>
`, launchAgentLabel, self, logPath, logPath), nil
}

func platformServiceStatus() (installed bool, status string) {
	path, err := plistPath()
	if err != nil {
		return false, ""
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, ""
	}

	// Check if loaded
	out, err := exec.Command("launchctl", "list").CombinedOutput()
	if err == nil && strings.Contains(string(out), launchAgentLabel) {
		return true, "  Status: loaded and running"
	}
	return true, "  Status: installed but not loaded"
}

func platformServicePlan() (string, error) {
	self, err := resolveBinaryPath()
	if err != nil {
		return "", err
	}

	path, err := plistPath()
	if err != nil {
		return "", err
	}

	home, _ := os.UserHomeDir()
	logPath := filepath.Join(home, ".continuity", "serve.log")

	return fmt.Sprintf(`Continuity will install a system service:

  Platform:  macOS (LaunchAgent)
  Binary:    %s
  Service:   %s
  Behavior:  Start on login, restart on crash
  Logs:      %s
`, self, path, logPath), nil
}

func platformServiceInstall() (string, error) {
	path, err := plistPath()
	if err != nil {
		return "", err
	}

	// Ensure LaunchAgents directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", fmt.Errorf("create LaunchAgents dir: %w", err)
	}

	// Ensure log directory exists
	home, _ := os.UserHomeDir()
	os.MkdirAll(filepath.Join(home, ".continuity"), 0755)

	content, err := generatePlist()
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write plist: %w", err)
	}

	// Load the service
	if out, err := exec.Command("launchctl", "load", path).CombinedOutput(); err != nil {
		return "", fmt.Errorf("launchctl load: %w\n%s", err, out)
	}

	return fmt.Sprintf(`Installed. Continuity is now running as a system service.
  Status:  launchctl list | grep continuity
  Stop:    launchctl unload %s
  Remove:  continuity uninstall-service`, path), nil
}

func platformUninstallPlan() (string, error) {
	path, err := plistPath()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`Will remove continuity system service:

  Service:  %s
  Action:   Unload from launchctl, delete plist file
`, path), nil
}

func platformServiceRemove() error {
	path, err := plistPath()
	if err != nil {
		return err
	}

	// Unload (ignore error — may not be loaded)
	exec.Command("launchctl", "unload", path).Run()

	// Remove plist
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove plist: %w", err)
	}
	return nil
}
