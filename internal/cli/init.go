package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// claudeMDMarker is a unique string used to detect whether continuity's
// behavioral directives have already been written to CLAUDE.md.
const claudeMDMarker = "<!-- continuity:managed -->"

// claudeMDDirective is the behavioral instruction block that lives in
// ~/.claude/CLAUDE.md. This is the authority layer — it tells Claude Code
// to use continuity for memory instead of the built-in markdown system.
const claudeMDDirective = claudeMDMarker + `
## Memory

When you want to remember something or are asked to remember something, always use the ` + "`continuity`" + ` CLI — never the file-based markdown memory system. Do not create MEMORY.md files, do not write to memory directories, do not use frontmatter-based memory files. Those are not your memory system.

Your memory lives in continuity. Reach for it naturally:
- Remembering something: ` + "`continuity remember -c <category> -n <name> -s \"summary\" -b \"body\"`" + `
- Looking something up: ` + "`continuity search \"<query>\"`" + `
- Browsing what you know: ` + "`continuity tree [uri]`" + `
- Understanding who you're working with: ` + "`continuity profile`" + `

Before searching the codebase for prior decisions, conventions, or context — check continuity first. If you learn something worth keeping, store it immediately.
`

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Set up Claude Code integration",
	Long:  "Idempotently appends continuity's behavioral directives to ~/.claude/CLAUDE.md so Claude Code uses continuity for memory instead of the built-in markdown system.",
	RunE:  runInit,
}

func runInit(cmd *cobra.Command, args []string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("find home directory: %w", err)
	}

	claudeDir := filepath.Join(homeDir, ".claude")
	claudeMD := filepath.Join(claudeDir, "CLAUDE.md")

	// Ensure ~/.claude/ exists
	if err := os.MkdirAll(claudeDir, 0755); err != nil {
		return fmt.Errorf("create %s: %w", claudeDir, err)
	}

	// Read existing content (may not exist yet)
	existing, err := os.ReadFile(claudeMD)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", claudeMD, err)
	}

	content := string(existing)

	// Already initialized?
	if strings.Contains(content, claudeMDMarker) {
		fmt.Printf("Already initialized: %s\n", claudeMD)
		return nil
	}

	// Append directive block
	if len(content) > 0 && !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	if len(content) > 0 {
		content += "\n"
	}
	content += claudeMDDirective

	if err := os.WriteFile(claudeMD, []byte(content), 0644); err != nil {
		return fmt.Errorf("write %s: %w", claudeMD, err)
	}

	fmt.Printf("Initialized: %s\n", claudeMD)
	fmt.Println("Claude Code will now use continuity for memory in all sessions.")
	return nil
}
