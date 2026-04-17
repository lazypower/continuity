//go:build windows

package hooks

// AutostartEnabled returns false on Windows — autostart is not supported.
func AutostartEnabled() bool { return false }

// TryAutostart returns false on Windows — autostart is not supported.
func TryAutostart() bool { return false }
