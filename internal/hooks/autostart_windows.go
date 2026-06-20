//go:build windows

package hooks

import "fmt"

// AutostartEnabled returns false on Windows — autostart is not supported.
func AutostartEnabled() bool { return false }

// TryAutostart returns false on Windows — autostart is not supported.
func TryAutostart() bool { return false }

// SpawnDetachedServe is unsupported on Windows (no Setsid). It exists to keep
// callers (e.g. `continuity restart`) buildable across platforms.
func SpawnDetachedServe() (int, error) {
	return 0, fmt.Errorf("detached serve spawn is not supported on Windows")
}
