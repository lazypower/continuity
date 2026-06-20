//go:build windows

package hooks

import (
	"fmt"
	"os"
	"path/filepath"
)

// bounceMarkerPath returns the path to the opt-in auto-bounce marker file.
// Auto-bounce itself is unsupported on Windows (no Setsid/detached respawn), but
// the path resolves so the decision logic stays uniform across platforms.
func bounceMarkerPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".continuity", "autostart-bounce"), nil
}

// serviceManaged reports true on Windows: there is no supported bare-mode
// detached respawn, so the hook must never attempt an auto-bounce — it only
// warns.
func serviceManaged() bool { return true }

// bounceBareServer is unsupported on Windows. It exists so the decision logic
// compiles cross-platform; it is never reached because serviceManaged() pins the
// action to warn-only.
func bounceBareServer(pid int) error {
	return fmt.Errorf("auto-bounce is not supported on Windows")
}
