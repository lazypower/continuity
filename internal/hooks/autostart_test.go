//go:build !windows

package hooks

import (
	"os"
	"path/filepath"
	"testing"
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
