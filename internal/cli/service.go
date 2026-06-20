package cli

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// resolveBinaryPath returns a stable path to the running continuity binary,
// suitable for embedding in launchd plists or systemd unit files.
//
// On Homebrew installs, os.Executable() + EvalSymlinks resolves to a versioned
// Cellar path (e.g. /opt/homebrew/Cellar/continuity/0.3.0/bin/continuity) which
// breaks after `brew upgrade`. We prefer a PATH-resolved location when it points
// to the same underlying binary, since the symlink in /opt/homebrew/bin or
// /usr/local/bin remains valid across upgrades.
func resolveBinaryPath() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve binary path: %w", err)
	}
	pathLoc, _ := exec.LookPath("continuity")
	return resolveBinaryPathFrom(self, pathLoc)
}

// servicePATH returns the PATH string to bake into a generated launchd plist /
// systemd unit. A service started by launchd/systemd does NOT inherit the
// interactive login shell's PATH, so the LLM provider binaries (`claude`,
// `ollama`) that live in Homebrew / user-local dirs are invisible to the
// service and extraction silently fails (issue #41).
//
// We capture the install-time PATH (the shell that ran `install-service` DOES
// have the login PATH) and union it with a set of well-known locations so the
// result is robust even when install happens from a minimal environment. The
// captured entries come first (user intent wins), then any common dirs not
// already present are appended. Duplicates and empties are dropped while order
// is preserved.
func servicePATH() string {
	return buildServicePATH(os.Getenv("PATH"), os.Getenv("HOME"))
}

// buildServicePATH is the pure core of servicePATH, taking the install-time PATH
// and HOME explicitly so it is unit-testable. When installPATH is empty it still
// returns a usable PATH built solely from the common defaults.
func buildServicePATH(installPATH, home string) string {
	var ordered []string
	seen := map[string]bool{}
	add := func(dir string) {
		dir = strings.TrimSpace(dir)
		// Drop any entry carrying a control char (newline, CR, tab, NUL, …). A
		// newline in particular would let a PATH entry inject extra lines into the
		// generated systemd unit (Environment=PATH=...) or break the plist; rather
		// than try to escape it into a single line, refuse the malformed entry. We
		// keep the remaining (well-formed) entries so the service still has a
		// usable PATH.
		if dir == "" || seen[dir] || containsControlChar(dir) {
			return
		}
		seen[dir] = true
		ordered = append(ordered, dir)
	}

	// Captured install-time PATH first — preserves the user's own ordering.
	for _, dir := range filepath.SplitList(installPATH) {
		add(dir)
	}

	// Then well-known locations the provider binaries commonly live in, so the
	// service can resolve `claude`/`ollama` even from a minimal install env.
	defaults := []string{
		"/opt/homebrew/bin",
		"/usr/local/bin",
		"/usr/bin",
		"/bin",
		"/usr/sbin",
		"/sbin",
	}
	if home != "" {
		// Claude Code's local install + the conventional user bin dir.
		defaults = append(defaults,
			filepath.Join(home, ".claude", "local"),
			filepath.Join(home, ".local", "bin"),
		)
	}
	for _, dir := range defaults {
		add(dir)
	}

	return strings.Join(ordered, string(os.PathListSeparator))
}

// containsControlChar reports whether s contains an ASCII control character
// (including newline, CR, tab, and NUL). Such characters in a PATH entry would
// corrupt the generated systemd unit (line injection) or plist, so they are
// rejected in buildServicePATH.
func containsControlChar(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func resolveBinaryPathFrom(self, pathLoc string) (string, error) {
	selfReal, err := filepath.EvalSymlinks(self)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks: %w", err)
	}

	if pathLoc != "" {
		if pathLocReal, err := filepath.EvalSymlinks(pathLoc); err == nil && pathLocReal == selfReal {
			if abs, err := filepath.Abs(pathLoc); err == nil {
				return abs, nil
			}
			return pathLoc, nil
		}
	}

	return selfReal, nil
}

var installServiceCmd = &cobra.Command{
	Use:   "install-service",
	Short: "Install continuity as a system service",
	Long: `Installs continuity as a platform-native service that starts on login
and restarts on crash.

  macOS:  LaunchAgent plist in ~/Library/LaunchAgents/
  Linux:  systemd user unit in ~/.config/systemd/user/

Interactive — shows exactly what will be installed and asks for confirmation.`,
	RunE: runInstallService,
}

var uninstallServiceCmd = &cobra.Command{
	Use:   "uninstall-service",
	Short: "Remove continuity system service",
	Long:  "Stops and removes the continuity system service installed by install-service.",
	RunE:  runUninstallService,
}

func runInstallService(cmd *cobra.Command, args []string) error {
	// Check if already installed
	installed, status := platformServiceStatus()
	if installed {
		fmt.Println("Continuity service is already installed.")
		if status != "" {
			fmt.Println(status)
		}
		fmt.Println()
		if !promptYN("Reinstall? [y/N] ") {
			return nil
		}
		// Unload before reinstalling
		if err := platformServiceRemove(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to unload existing service: %v\n", err)
		}
	}

	// Show what we'll do
	plan, err := platformServicePlan()
	if err != nil {
		return err
	}
	fmt.Println(plan)

	if !installed {
		if !promptYN("Install? [y/N] ") {
			fmt.Println("Aborted.")
			return nil
		}
	}

	// Do it
	result, err := platformServiceInstall()
	if err != nil {
		return fmt.Errorf("install service: %w", err)
	}
	fmt.Println(result)
	return nil
}

func runUninstallService(cmd *cobra.Command, args []string) error {
	installed, _ := platformServiceStatus()
	if !installed {
		fmt.Println("No continuity service found. Nothing to remove.")
		return nil
	}

	plan, err := platformUninstallPlan()
	if err != nil {
		return err
	}
	fmt.Println(plan)

	if !promptYN("Uninstall? [y/N] ") {
		fmt.Println("Aborted.")
		return nil
	}

	if err := platformServiceRemove(); err != nil {
		return fmt.Errorf("uninstall service: %w", err)
	}
	fmt.Println("Continuity service removed.")
	return nil
}

// promptYN prints a prompt and reads y/n from stdin.
func promptYN(prompt string) bool {
	fmt.Print(prompt)
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
		return answer == "y" || answer == "yes"
	}
	return false
}
