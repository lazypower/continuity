package cli

import (
	"fmt"

	"github.com/lazypower/continuity/internal/buildinfo"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("continuity %s (commit: %s, built: %s)\n",
			buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate)
	},
}

// VersionString returns a formatted version string for use in health checks etc.
// Thin alias over buildinfo so existing callers (e.g. serve.go) keep working
// after build metadata moved out of this package.
func VersionString() string {
	return buildinfo.VersionString()
}
