package cli

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "continuity",
	Short: "Persistent memory for AI coding agents",
	Long:  "Continuity gives AI agents memory that persists across sessions. Single Go binary, zero dependencies.",
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(hookCmd)
	rootCmd.AddCommand(searchCmd)
	rootCmd.AddCommand(profileCmd)
	rootCmd.AddCommand(treeCmd)
	rootCmd.AddCommand(importCmd)
	rootCmd.AddCommand(dedupCmd)
}
