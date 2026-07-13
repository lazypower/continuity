package cli

import (
	"os"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/lazypower/continuity/internal/mcp"
	"github.com/spf13/cobra"
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Run the MCP server (stdio) exposing memory tools to an agent",
	Long: `Start a Model Context Protocol server on stdio. Register it with an MCP
client (e.g. Claude Code) so the agent can call continuity's memory tools —
remember, search, show, tree, profile, retract — directly, with structured
arguments instead of shell-escaped CLI flags.

The server is a thin client of the running daemon: start ` + "`continuity serve`" + `
first (or let your SessionStart hook launch it). Register it in .mcp.json:

  {
    "mcpServers": {
      "continuity": { "command": "continuity", "args": ["mcp"] }
    }
  }`,
	Args: cobra.NoArgs,
	// stdio speaks JSON-RPC on stdout; cobra must never print usage there and
	// corrupt the stream. Errors still surface on stderr, which the client ignores.
	SilenceUsage: true,
	RunE:         runMCP,
}

func runMCP(cmd *cobra.Command, args []string) error {
	srv := mcp.NewServer(hooks.NewClient(), VersionString())
	return srv.Serve(os.Stdin, os.Stdout)
}
