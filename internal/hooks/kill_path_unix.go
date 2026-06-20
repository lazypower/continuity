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

// osExeMatch best-effort confirms that the running pid's executable matches the
// exe the health payload advertised. See exeMatcher's contract for the tri-state
// return.
//
//   - Linux: read /proc/<pid>/exe (a symlink to the actual binary) and compare.
//     A definite, contradictory readlink result -> (false, err) = refuse. A
//     missing/empty wantExe or an unreadable link -> (false, nil) = indeterminate.
//   - Other unix (macOS, BSD): no cheap, reliable per-pid exe lookup without
//     extra deps -> (false, nil) = indeterminate. The strong field-identity +
//     same-pid revalidation in ConfirmKillTarget carries the decision there.
func osExeMatch(pid int, wantExe string) (bool, error) {
	if runtime.GOOS != "linux" {
		return false, nil // indeterminate on non-Linux unix
	}
	if wantExe == "" {
		return false, nil // nothing to compare against -> indeterminate
	}
	link := fmt.Sprintf("/proc/%d/exe", pid)
	actual, err := os.Readlink(link)
	if err != nil {
		// Can't read the link (process exiting, permissions). Indeterminate, not
		// a mismatch — don't refuse on an unreadable /proc.
		return false, nil
	}
	// Linux appends " (deleted)" when the on-disk binary was replaced (the exact
	// brew-upgrade case this whole feature targets). Strip it before comparing.
	actual = stripDeletedSuffix(actual)
	want := stripDeletedSuffix(wantExe)
	if filepath.Clean(actual) == filepath.Clean(want) {
		return true, nil
	}
	return false, fmt.Errorf("running exe %q != health-reported exe %q", actual, want)
}

func stripDeletedSuffix(p string) string {
	const suffix = " (deleted)"
	if len(p) > len(suffix) && p[len(p)-len(suffix):] == suffix {
		return p[:len(p)-len(suffix)]
	}
	return p
}

// osStopPID gracefully stops a confirmed-continuity pid: SIGTERM, brief wait,
// escalate to SIGKILL only if it does not exit. The caller (ConfirmKillTarget)
// has already established strong identity for this pid — we never reach here for
// an unverified process.
func osStopPID(pid int) error {
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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if proc.Signal(syscall.Signal(0)) != nil {
			return nil // process gone
		}
		time.Sleep(150 * time.Millisecond)
	}
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
