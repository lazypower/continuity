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

// TestReapedChildReportsNotAlive is the unit-level guard for Fix 1: a child we
// started and then crashes must NOT linger as a zombie that signal-0 reports as
// ALIVE. SpawnDetachedServe reaps its child via a background cmd.Wait(); this test
// reproduces that pattern with a process that exits immediately and asserts
// ProcessAlive(pid) eventually reports false.
//
// Without the reap, the crashed child stays a zombie owned by this (parent)
// process and proc.Signal(0) returns nil (ALIVE), which is exactly what masks a
// crash-on-boot as a soft timeout in `continuity restart`. The companion test in
// TestZombieChildReportsAliveWithoutReap documents that failing baseline.
func TestReapedChildReportsNotAlive(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	pid := cmd.Process.Pid
	// This is the SpawnDetachedServe reap behavior under test.
	go func() { _ = cmd.Wait() }()

	deadline := time.Now().Add(2 * time.Second)
	for ProcessAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("reaped child pid %d still reported alive; reap did not collect the zombie", pid)
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
