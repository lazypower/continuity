//go:build !windows

package cli

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/lazypower/continuity/internal/hooks"
)

// stopPID gracefully stops a confirmed-continuity process: SIGTERM, brief wait,
// escalate to SIGKILL only if it does not exit. Used for bare (unmanaged)
// restarts where the caller has already confirmed via /api/health that this PID
// is continuity — we never signal an unverified PID.
func stopPID(pid int) error {
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
	// Wait up to 5s for graceful exit.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if proc.Signal(syscall.Signal(0)) != nil {
			return nil // process gone
		}
		time.Sleep(150 * time.Millisecond)
	}
	// Escalate: SIGKILL and confirm it's gone.
	_ = proc.Signal(syscall.SIGKILL)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if proc.Signal(syscall.Signal(0)) != nil {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("process %d did not exit after SIGTERM and SIGKILL", pid)
}

// respawnServer relaunches a detached `continuity serve`, reusing the shared
// spawn logic (Setsid, logs to ~/.continuity/serve.log). Port binding is the
// lock, so this is safe even if a slow-dying old process briefly lingers.
func respawnServer() error {
	_, err := hooks.SpawnDetachedServe()
	return err
}
