package cli

import (
	"fmt"
	"os"

	"github.com/lazypower/continuity/internal/hooks"
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
	Use:   "hook",
	Short: "Handle Claude Code hook events",
}

var hookStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Handle SessionStart hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("start", os.Stdin)
	},
}

var hookSubmitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Handle UserPromptSubmit hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("submit", os.Stdin)
	},
}

var hookToolCmd = &cobra.Command{
	Use:   "tool",
	Short: "Handle PostToolUse hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("tool", os.Stdin)
	},
}

var hookStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Handle Stop hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("stop", os.Stdin)
	},
}

var hookEndCmd = &cobra.Command{
	Use:   "end",
	Short: "Handle SessionEnd hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("end", os.Stdin)
	},
}

func init() {
	hookCmd.AddCommand(hookStartCmd)
	hookCmd.AddCommand(hookSubmitCmd)
	hookCmd.AddCommand(hookToolCmd)
	hookCmd.AddCommand(hookStopCmd)
	hookCmd.AddCommand(hookEndCmd)
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
