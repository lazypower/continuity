//go:build !windows

package hooks

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestServiceManagedDetection(t *testing.T) {
	// serviceManaged() keys off the platform service definition file existing on
	// disk. With a clean HOME and no service installed, it must report bare
	// (false) so the opt-in bounce path is reachable.
	t.Run("bare when no service file", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("HOME", tmp)
		if serviceManaged() {
			t.Error("serviceManaged() should be false with no service definition on disk")
		}
	})

	t.Run("managed when service file present", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("HOME", tmp)

		var path string
		switch runtime.GOOS {
		case "darwin":
			path = filepath.Join(tmp, "Library", "LaunchAgents", "com.continuity.server.plist")
		case "linux":
			path = filepath.Join(tmp, ".config", "systemd", "user", "continuity.service")
		default:
			t.Skipf("no service-file convention for %s", runtime.GOOS)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("service def\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if !serviceManaged() {
			t.Error("serviceManaged() should be true when a service definition exists on disk")
		}
	})
}
