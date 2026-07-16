package cli

import (
	"strings"
	"testing"
)

// TestUpsertManagedBlock covers the insert/refresh/preserve behavior of the
// CLAUDE.md managed directive block — the mechanism that lets a re-run of
// `continuity init` deliver an updated directive (e.g. CLI → MCP) to users who
// already initialized.
func TestUpsertManagedBlock(t *testing.T) {
	block := claudeMDDirective()

	t.Run("empty file initializes", func(t *testing.T) {
		got, action := upsertManagedBlock("", block)
		if action != "initialized" {
			t.Fatalf("action = %q, want initialized", action)
		}
		if !strings.Contains(got, claudeMDMarker) || !strings.Contains(got, claudeMDEndMarker) {
			t.Fatalf("result missing markers:\n%s", got)
		}
		if !strings.HasSuffix(got, "\n") {
			t.Errorf("result should end with a newline")
		}
	})

	t.Run("appends to existing content, preserving it", func(t *testing.T) {
		prior := "# My CLAUDE.md\n\nSome personal notes.\n"
		got, action := upsertManagedBlock(prior, block)
		if action != "initialized" {
			t.Fatalf("action = %q, want initialized", action)
		}
		if !strings.HasPrefix(got, prior) {
			t.Errorf("prior content not preserved at head:\n%s", got)
		}
		if !strings.Contains(got, claudeMDMarker) {
			t.Errorf("managed block not appended")
		}
	})

	t.Run("re-run with identical block is unchanged", func(t *testing.T) {
		first, _ := upsertManagedBlock("existing\n", block)
		got, action := upsertManagedBlock(first, block)
		if action != "unchanged" {
			t.Fatalf("action = %q, want unchanged", action)
		}
		if got != first {
			t.Errorf("content mutated on no-op re-run")
		}
	})

	t.Run("legacy block (no end marker) is upgraded in place", func(t *testing.T) {
		// Simulate a block written before the end marker existed: start marker
		// + old CLI-oriented text, appended at EOF.
		legacy := "# header\n\n" + claudeMDMarker + "\n## Memory\n\nUse the `continuity` CLI: `continuity remember ...`.\n"
		got, action := upsertManagedBlock(legacy, block)
		if action != "updated" {
			t.Fatalf("action = %q, want updated", action)
		}
		if !strings.HasPrefix(got, "# header\n") {
			t.Errorf("content before the block not preserved:\n%s", got)
		}
		if strings.Contains(got, "Use the `continuity` CLI") {
			t.Errorf("legacy directive text not replaced:\n%s", got)
		}
		if !strings.Contains(got, claudeMDEndMarker) {
			t.Errorf("upgraded block missing end marker")
		}
		if strings.Count(got, claudeMDMarker) != 1 {
			t.Errorf("expected exactly one managed block, got %d start markers", strings.Count(got, claudeMDMarker))
		}
	})

	t.Run("content after a modern block is preserved on refresh", func(t *testing.T) {
		stale := "top\n\n" + claudeMDMarker + "\nOLD BODY\n" + claudeMDEndMarker + "\n\n## User section after\ntrailing text\n"
		got, action := upsertManagedBlock(stale, block)
		if action != "updated" {
			t.Fatalf("action = %q, want updated", action)
		}
		if strings.Contains(got, "OLD BODY") {
			t.Errorf("stale body not replaced")
		}
		if !strings.Contains(got, "## User section after") || !strings.HasSuffix(got, "trailing text\n") {
			t.Errorf("trailing user content not preserved:\n%s", got)
		}
		if strings.Count(got, claudeMDMarker) != 1 {
			t.Errorf("expected exactly one managed block, got %d", strings.Count(got, claudeMDMarker))
		}
	})

	t.Run("directive is MCP-first and doesn't re-map tool signatures", func(t *testing.T) {
		// Guard against reverting to CLI-first phrasing.
		if !strings.Contains(claudeMDBody, "mcp__continuity__") {
			t.Errorf("directive should name the mcp__continuity__* tools")
		}
		// The MCP layer is the authority for tool signatures; the directive must
		// not duplicate per-tool parameter lists (they rot when a tool changes).
		if strings.Contains(claudeMDBody, "category, name, summary, body") {
			t.Errorf("directive should not re-map tool parameters — the MCP schemas own that")
		}
	})
}
