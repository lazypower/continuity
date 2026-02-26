package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Set via -ldflags at build time.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("continuity %s (commit: %s, built: %s)\n", Version, Commit, BuildDate)
	},
}

// VersionString returns a formatted version string for use in health checks etc.
func VersionString() string {
	return fmt.Sprintf("%s (%s)", Version, Commit)
}
