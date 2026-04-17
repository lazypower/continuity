package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

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
