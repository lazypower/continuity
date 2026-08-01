package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// The operators manual drifted from the code once already, badly enough that
// the README described the default embedder as opt-in, the Stop hook as
// extracting when extraction had been off for a release, and a whole
// configuration file that was never mentioned at all. Prose discipline is what
// failed; a gate is what replaces it.
//
// These tests enumerate the real surface from the code and fail the build when
// something ships undocumented. They deliberately check only that a name is
// PRESENT — whether the surrounding prose is any good is a human's job, and a
// test that tried to judge that would be both unreliable and easy to game.

const docsDir = "../../docs"

func readDoc(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(docsDir, rel))
	if err != nil {
		t.Fatalf("read %s: %v — the operators manual is part of the build, not an optional extra", rel, err)
	}
	return string(b)
}

// collectCommands walks the cobra tree and returns every invocable command path
// ("snapshot list", "embedder use", ...). Hidden commands and cobra's own
// built-ins are excluded: they are machinery, not operator surface.
func collectCommands(cmd *cobra.Command, prefix string) []string {
	var out []string
	for _, c := range cmd.Commands() {
		if c.Hidden || c.Name() == "help" || c.Name() == "completion" {
			continue
		}
		full := strings.TrimSpace(prefix + " " + c.Name())
		if c.Runnable() {
			out = append(out, full)
		}
		out = append(out, collectCommands(c, full)...)
	}
	return out
}

func TestDocsContract_EveryCommandIsDocumented(t *testing.T) {
	doc := readDoc(t, "reference/cli.md")

	var missing []string
	for _, name := range collectCommands(rootCmd, "") {
		// `hook <event>` handlers are wired into Claude Code once and never
		// typed by hand; they are documented as a group in advanced/.
		if strings.HasPrefix(name, "hook") {
			continue
		}
		if !strings.Contains(doc, "continuity "+name) {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("undocumented command(s) in docs/reference/cli.md:\n  %s\n\n"+
			"Every command a user can type must be in the CLI reference. Add it there, "+
			"and consider whether it also belongs in a guide.",
			strings.Join(missing, "\n  "))
	}
}

// envVarPattern finds CONTINUITY_* names in Go source. Matching the literal
// rather than a curated list is the point: a new variable is caught even when
// whoever added it never thought about documentation.
var envVarPattern = regexp.MustCompile(`CONTINUITY_[A-Z0-9_]+`)

func TestDocsContract_EveryEnvVarIsDocumented(t *testing.T) {
	doc := readDoc(t, "reference/configuration.md")

	found := map[string]bool{}
	err := filepath.Walk("../..", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // unreadable trees are not this test's business
		}
		if info.IsDir() {
			// Vendored code, build output, and agent worktrees are not our surface.
			switch info.Name() {
			case ".git", "node_modules", "ui", "vendor", ".claude":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// Test-only variables configure fixtures, not installations.
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		for _, m := range envVarPattern.FindAllString(string(b), -1) {
			found[m] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("found no CONTINUITY_* variables at all — the scan is broken, not the docs")
	}

	var missing []string
	for name := range found {
		if !strings.Contains(doc, name) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("undocumented environment variable(s) in docs/reference/configuration.md:\n  %s\n\n"+
			"An operator cannot discover a variable that is only named in the source.",
			strings.Join(missing, "\n  "))
	}
}

// TestDocsContract_NoBrokenInternalLinks catches the failure mode that makes a
// documentation tree feel abandoned: a link that 404s. Only relative links to
// files inside docs/ are checked; external URLs are not this test's business.
func TestDocsContract_NoBrokenInternalLinks(t *testing.T) {
	linkPattern := regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

	var broken []string
	err := filepath.Walk(docsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		for _, m := range linkPattern.FindAllStringSubmatch(string(b), -1) {
			target := m[1]
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") ||
				strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			// Strip any anchor; we verify the file exists, not the heading.
			if i := strings.Index(target, "#"); i >= 0 {
				target = target[:i]
			}
			if target == "" {
				continue
			}
			resolved := filepath.Join(filepath.Dir(path), target)
			if _, statErr := os.Stat(resolved); statErr != nil {
				broken = append(broken, path+" → "+m[1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk docs: %v", err)
	}

	if len(broken) > 0 {
		sort.Strings(broken)
		t.Errorf("broken internal link(s):\n  %s", strings.Join(broken, "\n  "))
	}
}
