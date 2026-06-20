//go:build !windows

package hooks

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestAutostartEnabledFalseByDefault(t *testing.T) {
	// Point HOME to a temp dir with no marker file
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	if AutostartEnabled() {
		t.Error("AutostartEnabled() should be false with no marker file")
	}
}

func TestAutostartEnabledTrueWithMarker(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	markerDir := filepath.Join(tmp, ".continuity")
	os.MkdirAll(markerDir, 0755)
	os.WriteFile(filepath.Join(markerDir, "autostart"), []byte("enabled\n"), 0644)

	if !AutostartEnabled() {
		t.Error("AutostartEnabled() should be true when marker file exists")
	}
}

// TestStartAndReapCollectsCrashedChild pins the PRODUCTION reap: it calls the
// real startAndReap helper (the same start+background-Wait() path SpawnDetachedServe
// uses) with a short-lived command and asserts ProcessAlive(pid) reports false
// once the process exits. A crashed child must NOT linger as a zombie that
// signal-0 reports ALIVE — that would mask a crash-on-boot as a soft timeout in
// `continuity restart`.
//
// This fails if the production `go cmd.Wait()` reap inside startAndReap is
// removed: the exited child would remain a zombie we parent and ProcessAlive
// would keep returning true until the deadline trips. The companion
// TestZombieChildReportsAliveWithoutReap documents that failing baseline.
func TestStartAndReapCollectsCrashedChild(t *testing.T) {
	// Short-lived command standing in for a crash-on-boot.
	pid, err := startAndReap(exec.Command("sh", "-c", "exit 1"))
	if err != nil {
		t.Fatalf("startAndReap: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for ProcessAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("reaped child pid %d still reported alive; production reap did not collect the zombie", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestZombieChildReportsAliveWithoutReap documents the BUG the reap fixes: an
// UN-reaped exited child remains a zombie that ProcessAlive (signal 0) reports as
// alive. It is the inverse of the production behavior and exists so the regression
// is legible — if a future change made unwaited children disappear on their own,
// this would surface it.
func TestZombieChildReportsAliveWithoutReap(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	pid := cmd.Process.Pid
	// Intentionally do NOT Wait() — the exited child becomes a zombie we own.
	t.Cleanup(func() { _ = cmd.Wait() }) // reap at test end to avoid leaking it

	// Give it time to exit; without reaping it lingers as a zombie.
	time.Sleep(200 * time.Millisecond)
	if !ProcessAlive(pid) {
		t.Skip("platform reaped the unwaited child on its own; zombie-as-alive behavior not reproducible here")
	}
}

func TestTryAutostartSkipsWhenDisabled(t *testing.T) {
	// Point HOME to a temp dir with no marker file
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	// Also point to a dead server so Healthy() returns false
	t.Setenv("CONTINUITY_URL", "http://127.0.0.1:1")

	if TryAutostart() {
		t.Error("TryAutostart() should return false when autostart is disabled")
	}
}
