package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// stubRun returns a RunE that prints a not-yet-implemented message to stderr
// and exits 0 (hooks must never crash Claude Code).
func stubRun(name string) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stderr, "%s: not yet implemented\n", name)
	}
}

var hookCmd = &cobra.Command{
	Use:   "hook [event]",
	Short: "Handle Claude Code hook events",
	Args:  cobra.MinimumNArgs(1),
	Run:   stubRun("hook"),
}

var searchCmd = &cobra.Command{
	Use:   "search [query]",
	Short: "Search memories",
	Args:  cobra.MinimumNArgs(1),
	Run:   stubRun("search"),
}

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Show relational profile",
	Run:   stubRun("profile"),
}

var treeCmd = &cobra.Command{
	Use:   "tree [path]",
	Short: "Browse memory tree",
	Run:   stubRun("tree"),
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import from claude-mem database",
	Run:   stubRun("import"),
}
