package hooks

import (
	"os"
	"strings"
)

// harnessSessionEnv is where Claude Code publishes the current session id to
// every child process it spawns: MCP servers, Bash tool commands, hooks.
const harnessSessionEnv = "CLAUDE_CODE_SESSION_ID"

// HarnessSessionID returns the session id the agent harness assigned to this
// process, or "" when running outside one (a human at a terminal, another
// agent's MCP client).
//
// Clients that write or read memory on the agent's behalf (the MCP server, CLI
// verbs invoked from a session) attach it so memories carry source_session and
// journal events carry session_id. Without it, agent-authored memories have no
// project affinity: the tray's corpus index and the prompt gate scope episodic
// nodes through source_session → sessions.project, so an unattributed memory
// is invisible to both.
//
// The value is fixed at process spawn. A long-lived MCP server that outlives a
// /clear keeps the original session id; that session belongs to the same
// project, so affinity stays correct even where the session label is stale.
func HarnessSessionID() string {
	return strings.TrimSpace(os.Getenv(harnessSessionEnv))
}
