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

	pid, err := SpawnDetachedServe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuity: autostart: %v\n", err)
		return false
	}

	// Poll health for up to 3 seconds
	client := NewClient()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if client.Healthy() {
			fmt.Fprintf(os.Stderr, "continuity: autostart: server launched (pid %d)\n", pid)
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Fprintf(os.Stderr, "continuity: autostart: server did not become healthy within 3s\n")
	return false
}

// SpawnDetachedServe launches `continuity serve` as a fully detached background
// process (new session via Setsid), logging to ~/.continuity/serve.log. It
// returns the spawned PID. Port binding is the lock: a redundant spawn will
// fail to bind and exit, so this is safe to call when a (now-stopped) server
// previously held the port. Used by both autostart and `continuity restart`.
func SpawnDetachedServe() (int, error) {
	self, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("resolve binary: %w", err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return 0, fmt.Errorf("home dir: %w", err)
	}
	logPath := filepath.Join(home, ".continuity", "serve.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return 0, fmt.Errorf("create state dir: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return 0, fmt.Errorf("open log: %w", err)
	}
	// Tighten existing log files from previous installs (0644 → 0600)
	if info, err := logFile.Stat(); err == nil && info.Mode().Perm()&0077 != 0 {
		os.Chmod(logPath, 0600)
	}

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		logFile.Close()
		return 0, fmt.Errorf("open devnull: %w", err)
	}
	defer logFile.Close()
	defer devNull.Close()

	cmd := exec.Command(self, "serve")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("spawn: %w", err)
	}
	// The child is fully detached (its own session via Setsid) and persists
	// after we exit. But we are still its PARENT until we exit, so if it crashes
	// on boot it becomes a ZOMBIE we own. A zombie answers signal-0 liveness
	// probes as ALIVE, which would mask a crash-on-boot during `continuity
	// restart`'s verify poll (the bare verifier would fall through to a soft
	// timeout instead of the hard verifyFailedDead). Reap it in the background so
	// a crashed child is collected promptly and ProcessAlive(pid) reports false
	// once it's gone. This is non-blocking: the goroutine waits, the caller (the
	// autostart hook) returns immediately and may exit at will — once the parent
	// exits, init/launchd reaps the orphan anyway.
	pid := cmd.Process.Pid
	go func() { _ = cmd.Wait() }()
	return pid, nil
}
