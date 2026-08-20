package hooks

import (
	"os"
	"path/filepath"
	"testing"
)

// mkdirAll is a fatal-on-error helper for building git layouts in tests.
func mkdirAll(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	return path
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// gitRepo builds a primary checkout: <root>/.git as a directory.
func gitRepo(t *testing.T, root string) {
	t.Helper()
	mkdirAll(t, filepath.Join(root, ".git"))
}

// linkedWorktree builds the on-disk layout `git worktree add` produces:
// <main>/.git/worktrees/<name>/commondir → "../.." and <wt>/.git →
// "gitdir: <main>/.git/worktrees/<name>".
func linkedWorktree(t *testing.T, main, wt, name string) {
	t.Helper()
	gitdir := mkdirAll(t, filepath.Join(main, ".git", "worktrees", name))
	writeFile(t, filepath.Join(gitdir, "commondir"), "../..\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: "+gitdir+"\n")
}

func TestProjectIdentity(t *testing.T) {
	tmp := t.TempDir()

	main := mkdirAll(t, filepath.Join(tmp, "repo"))
	gitRepo(t, main)

	// In-repo worktree — the exact layout that fragments affinity (#79).
	inRepoWT := filepath.Join(main, ".claude", "worktrees", "agent-x")
	linkedWorktree(t, main, inRepoWT, "agent-x")

	// Worktree outside the repository root resolves to the same primary.
	siblingWT := filepath.Join(tmp, "repo-sibling-wt")
	linkedWorktree(t, main, siblingWT, "sibling")

	nonGit := mkdirAll(t, filepath.Join(tmp, "plain-dir"))

	// A .git file that isn't a gitdir pointer must fall back to the raw cwd,
	// never guess.
	malformed := mkdirAll(t, filepath.Join(tmp, "malformed"))
	writeFile(t, filepath.Join(malformed, ".git"), "not a gitdir pointer\n")

	tests := []struct {
		name string
		cwd  string
		want string
	}{
		{"empty cwd", "", ""},
		{"non-git dir stays as-is", nonGit, nonGit},
		{"primary checkout root", main, main},
		{"subdirectory of primary checkout", mkdirAll(t, filepath.Join(main, "internal", "store")), main},
		{"in-repo linked worktree", inRepoWT, main},
		{"subdirectory of in-repo worktree", mkdirAll(t, filepath.Join(inRepoWT, "internal")), main},
		{"out-of-repo linked worktree", siblingWT, main},
		{"malformed .git file falls back to cwd", malformed, malformed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := projectIdentity(tt.cwd); got != tt.want {
				t.Errorf("projectIdentity(%q) = %q, want %q", tt.cwd, got, tt.want)
			}
		})
	}
}

// TestProjectIdentity_RelativeGitdir covers a `.git` file with a relative
// gitdir pointer — legal per git-worktree(1) even if `git worktree add`
// writes absolute paths.
func TestProjectIdentity_RelativeGitdir(t *testing.T) {
	tmp := t.TempDir()
	main := mkdirAll(t, filepath.Join(tmp, "repo"))
	gitRepo(t, main)

	wt := filepath.Join(tmp, "rel-wt")
	gitdir := mkdirAll(t, filepath.Join(main, ".git", "worktrees", "rel"))
	writeFile(t, filepath.Join(gitdir, "commondir"), "../..\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: ../repo/.git/worktrees/rel\n")

	if got := projectIdentity(wt); got != main {
		t.Errorf("projectIdentity(%q) = %q, want %q", wt, got, main)
	}
}

// TestProjectIdentity_BareCommonDir: when the common dir doesn't sit inside a
// primary checkout (bare repo), there is no primary path to resolve to — the
// raw cwd is kept rather than a guess.
func TestProjectIdentity_BareCommonDir(t *testing.T) {
	tmp := t.TempDir()
	bare := mkdirAll(t, filepath.Join(tmp, "repo.git"))

	wt := filepath.Join(tmp, "bare-wt")
	gitdir := mkdirAll(t, filepath.Join(bare, "worktrees", "w"))
	writeFile(t, filepath.Join(gitdir, "commondir"), "../..\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: "+gitdir+"\n")

	if got := projectIdentity(wt); got != wt {
		t.Errorf("projectIdentity(%q) = %q, want raw cwd %q", wt, got, wt)
	}
}
