//go:build !windows

package hooks

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// bounceMarkerPath returns the path to the opt-in auto-bounce marker file. It
// mirrors autostartMarkerPath's convention (a bare file under ~/.continuity).
func bounceMarkerPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".continuity", "autostart-bounce"), nil
}

// serviceManaged reports whether a platform service definition for continuity
// is installed on disk (launchd plist on macOS, systemd user unit on Linux).
//
// The hook uses installed-on-disk as its proxy for "service-managed": if the
// operator set up a service, the hook must not bounce the process out from
// under the manager (that's `continuity restart`'s job). This is intentionally
// conservative — when in doubt, we warn rather than bounce. It does NOT shell
// out to launchctl/systemctl (cheap, hook-safe), and it does NOT import
// internal/cli (would be an import cycle).
func serviceManaged() bool {
	home, err := os.UserHomeDir()
	if err != nil {
		// Can't tell — assume managed so we warn instead of bouncing.
		return true
	}
	var path string
	switch runtime.GOOS {
	case "darwin":
		path = filepath.Join(home, "Library", "LaunchAgents", "com.continuity.server.plist")
	case "linux":
		path = filepath.Join(home, ".config", "systemd", "user", "continuity.service")
	default:
		// Unknown platform — be conservative and treat as managed.
		return true
	}
	_, err = os.Stat(path)
	return err == nil
}

// bounceBareServer stops a confirmed-continuity PID (SIGTERM, brief wait,
// escalate to SIGKILL only if needed) and respawns a detached serve. The caller
// has already confirmed via CompatibilityCheck/health that this PID is
// continuity — we never signal an unverified PID. Used only for the opt-in,
// bare-mode hook bounce; service-managed restarts go through `continuity
// restart`.
func bounceBareServer(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d", pid)
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process %d: %w", pid, err)
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM to %d: %w", pid, err)
	}
	// Wait briefly for graceful exit.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if proc.Signal(syscall.Signal(0)) != nil {
			break // process gone
		}
		time.Sleep(150 * time.Millisecond)
	}
	// Escalate to SIGKILL only if it's still alive.
	if proc.Signal(syscall.Signal(0)) == nil {
		_ = proc.Signal(syscall.SIGKILL)
		deadline = time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if proc.Signal(syscall.Signal(0)) != nil {
				break
			}
			time.Sleep(150 * time.Millisecond)
		}
	}

	if _, err := SpawnDetachedServe(); err != nil {
		return fmt.Errorf("respawn server: %w", err)
	}
	return nil
}
