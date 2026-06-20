//go:build !windows

package hooks

import (
	"os"
	"path/filepath"
	"runtime"
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
