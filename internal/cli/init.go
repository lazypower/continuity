package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// claudeMDMarker and claudeMDEndMarker delimit continuity's managed directive
// block in ~/.claude/CLAUDE.md. The block is an UPSERT target: re-running
// `continuity init` replaces whatever sits between these markers with the
// current directive, so prompt changes (like preferring the MCP tools over the
// CLI) actually reach users who already initialized. Everything outside the
// markers is preserved verbatim.
const (
	claudeMDMarker    = "<!-- continuity:managed -->"
	claudeMDEndMarker = "<!-- /continuity:managed -->"
)

// claudeMDBody is the directive text (between the markers) written to
// ~/.claude/CLAUDE.md. This is the authority layer — it tells Claude Code to
// use continuity for memory instead of the built-in markdown system, and
// directs it at the MCP memory tools (structured args, no shell quoting) with
// the CLI as the fallback for setups where the MCP server isn't registered.
const claudeMDBody = `## Memory

Continuity is your memory — use it instead of the file-based markdown memory system. Do not create MEMORY.md files, write to memory directories, or use frontmatter memory files; those are not your memory system.

The memory tools are exposed over MCP as the ` + "`mcp__continuity__*`" + ` tools; their schemas describe how to call them. If the MCP server isn't registered, the same operations are available as ` + "`continuity`" + ` CLI verbs.

Before searching the codebase for prior decisions, conventions, or context, check continuity first — and store anything worth keeping the moment you learn it.

**Memory is not immutable; it is accountable.** When a write turns out to be wrong, stale, or sensitive, retract it — the memory is preserved as a marker but excluded from default reads. Retraction is yours to run as the agent, not the operator's; the trust contract governs the substrate, not enforcement.`

// claudeMDDirective assembles the full managed block (markers included).
func claudeMDDirective() string {
	return claudeMDMarker + "\n" + claudeMDBody + "\n" + claudeMDEndMarker
}

// upsertManagedBlock inserts or refreshes continuity's managed directive block
// in a CLAUDE.md body, preserving everything outside the markers. It returns
// the new content and an action: "initialized" (block was absent → appended),
// "updated" (an existing block was replaced), or "unchanged" (the existing
// block already matched). A legacy block (start marker written before the end
// marker existed) is recognized by its start marker and replaced from there to
// end-of-file — legacy blocks were always appended last.
func upsertManagedBlock(content, block string) (string, string) {
	before, rest, found := strings.Cut(content, claudeMDMarker)
	if !found {
		var b strings.Builder
		b.WriteString(content)
		if len(content) > 0 {
			if !strings.HasSuffix(content, "\n") {
				b.WriteString("\n")
			}
			b.WriteString("\n")
		}
		b.WriteString(block)
		b.WriteString("\n")
		return b.String(), "initialized"
	}

	// rest is everything after the start marker. after is whatever follows the
	// end marker; "" covers both a legacy block (no end marker → to EOF) and a
	// modern block with nothing following it.
	after := ""
	if _, tail, ok := strings.Cut(rest, claudeMDEndMarker); ok {
		after = tail
	}

	rebuilt := before + block
	if trailing := strings.TrimLeft(after, "\n"); trailing != "" {
		rebuilt += "\n\n" + trailing
	}
	rebuilt = strings.TrimRight(rebuilt, "\n") + "\n"

	if rebuilt == content {
		return content, "unchanged"
	}
	return rebuilt, "updated"
}

var initAutostart bool

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Set up Claude Code integration",
	Long: `Idempotently writes continuity's behavioral directives to ~/.claude/CLAUDE.md
so Claude Code uses continuity for memory instead of the built-in markdown system.
Re-running refreshes the managed directive block in place (e.g. to pick up prompt
updates); content outside continuity's markers is left untouched.

With --autostart, enables automatic server launch when the SessionStart hook
detects the server is down. Without --autostart, disables autostart if it was
previously enabled.`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().BoolVar(&initAutostart, "autostart", false,
		"Enable automatic server launch when hooks detect the server is down")
}

func runInit(cmd *cobra.Command, args []string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("find home directory: %w", err)
	}

	// --- CLAUDE.md directives ---
	claudeDir := filepath.Join(homeDir, ".claude")
	claudeMD := filepath.Join(claudeDir, "CLAUDE.md")

	if err := os.MkdirAll(claudeDir, 0755); err != nil {
		return fmt.Errorf("create %s: %w", claudeDir, err)
	}

	existing, err := os.ReadFile(claudeMD)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", claudeMD, err)
	}

	newContent, action := upsertManagedBlock(string(existing), claudeMDDirective())
	switch action {
	case "unchanged":
		fmt.Printf("Already up to date: %s\n", claudeMD)
	default:
		if err := os.WriteFile(claudeMD, []byte(newContent), 0644); err != nil {
			return fmt.Errorf("write %s: %w", claudeMD, err)
		}
		if action == "initialized" {
			fmt.Printf("Initialized: %s\n", claudeMD)
			fmt.Println("Claude Code will now use continuity for memory in all sessions.")
		} else {
			fmt.Printf("Updated directives: %s\n", claudeMD)
			fmt.Println("Refreshed continuity's memory directives (now prefers the MCP tools).")
		}
	}

	// --- Autostart marker ---
	autostartPath := filepath.Join(homeDir, ".continuity", "autostart")

	if initAutostart {
		if err := os.MkdirAll(filepath.Dir(autostartPath), 0700); err != nil {
			return fmt.Errorf("create .continuity dir: %w", err)
		}
		if err := os.WriteFile(autostartPath, []byte("enabled\n"), 0600); err != nil {
			return fmt.Errorf("write autostart marker: %w", err)
		}
		fmt.Println("Autostart enabled: continuity serve will launch automatically when needed.")
		fmt.Println("  The server persists as a background process until stopped or reboot.")
		fmt.Println("  Stop: pkill continuity  |  Logs: ~/.continuity/serve.log")
	} else {
		if err := os.Remove(autostartPath); err == nil {
			fmt.Println("Autostart disabled.")
		}
		// If file didn't exist, nothing to report
	}

	return nil
}
