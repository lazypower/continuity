//go:build !windows

package hooks

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// autostartMarkerPath returns the path to the autostart marker file.
func autostartMarkerPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".continuity", "autostart"), nil
}

// AutostartEnabled returns true if the user has opted into hook-based auto-launch.
// Presence of ~/.continuity/autostart is the signal — no config parsing needed.
func AutostartEnabled() bool {
	path, err := autostartMarkerPath()
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}

// TryAutostart spawns `continuity serve` in the background if autostart is enabled
// and the server is not running. Returns true if the server becomes healthy within
// 3 seconds, false otherwise.
//
// The spawned process is fully detached (new session via Setsid) and persists after
// the hook exits. Port binding on :37777 is the lock — concurrent spawns are safe
// because the second process will fail to bind and exit.
func TryAutostart() bool {
	if !AutostartEnabled() {
		return false
	}

	self, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuity: autostart: resolve binary: %v\n", err)
		return false
	}

	// Log file for the spawned server
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuity: autostart: home dir: %v\n", err)
		return false
	}
	logPath := filepath.Join(home, ".continuity", "serve.log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuity: autostart: open log: %v\n", err)
		return false
	}
	// Tighten existing log files from previous installs (0644 → 0600)
	if info, err := logFile.Stat(); err == nil && info.Mode().Perm()&0077 != 0 {
		os.Chmod(logPath, 0600)
	}

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		logFile.Close()
		return false
	}

	cmd := exec.Command(self, "serve")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "continuity: autostart: spawn: %v\n", err)
		logFile.Close()
		devNull.Close()
		return false
	}

	// Don't wait on the child — it's fully detached
	logFile.Close()
	devNull.Close()

	// Poll health for up to 3 seconds
	client := NewClient()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if client.Healthy() {
			fmt.Fprintf(os.Stderr, "continuity: autostart: server launched (pid %d)\n", cmd.Process.Pid)
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Fprintf(os.Stderr, "continuity: autostart: server did not become healthy within 3s\n")
	return false
}
