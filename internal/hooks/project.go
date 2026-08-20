package hooks

import (
	"os"
	"path/filepath"
	"strings"
)

// projectIdentity resolves a session cwd to its canonical project identity:
// the repository's primary checkout path (#79, ADR-001 §3).
//
// sessions.project is the join key for corpus affinity
// (mem_nodes.source_session → sessions.session_id → sessions.project). A raw
// cwd fragments that key: a linked git worktree (e.g. .claude/worktrees/<x>)
// or a repo subdirectory registers as a distinct project even though its
// memories belong to the repository.
//
// Normalization lives hook-side, not server-side, because the evidence is
// filesystem-local: resolving a worktree means reading .git metadata at cwd on
// the machine the session runs on, and only the hook process is guaranteed to
// be there. The server stores whatever identity the hook asserts and never
// depends on the client's filesystem. Normalizing at the write boundary also
// means the stored value is canonical going forward — repository provenance is
// walked from metadata at init time, never repaired by a later defrag pass.
//
// Resolution: walk up from cwd to the first .git entry. A .git directory marks
// the primary checkout — return the directory containing it. A .git file marks
// a linked worktree — follow gitdir → commondir to the shared .git directory
// and return its parent (the primary checkout). Non-git cwd, or a layout that
// cannot be resolved (bare repo, malformed metadata), returns cwd unchanged:
// less normalization, never guessed identity.
func projectIdentity(cwd string) string {
	if cwd == "" {
		return ""
	}
	dir := filepath.Clean(cwd)
	for {
		gitPath := filepath.Join(dir, ".git")
		if fi, err := os.Stat(gitPath); err == nil {
			if fi.IsDir() {
				return dir
			}
			if root := primaryCheckoutFromGitFile(dir, gitPath); root != "" {
				return root
			}
			return cwd
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return cwd
		}
		dir = parent
	}
}

// primaryCheckoutFromGitFile resolves a linked worktree's `.git` file
// ("gitdir: <path>") through its gitdir's `commondir` file to the shared .git
// directory, and returns the primary checkout path (the shared .git's parent).
// Returns "" when the layout doesn't resolve — the caller falls back to the
// raw cwd rather than guessing.
func primaryCheckoutFromGitFile(worktreeRoot, gitFile string) string {
	data, err := os.ReadFile(gitFile)
	if err != nil {
		return ""
	}
	firstLine, _, _ := strings.Cut(strings.TrimSpace(string(data)), "\n")
	gitdir, ok := strings.CutPrefix(firstLine, "gitdir:")
	if !ok {
		return ""
	}
	gitdir = strings.TrimSpace(gitdir)
	if gitdir == "" {
		return ""
	}
	if !filepath.IsAbs(gitdir) {
		gitdir = filepath.Join(worktreeRoot, gitdir)
	}

	common := gitdir
	if data, err := os.ReadFile(filepath.Join(gitdir, "commondir")); err == nil {
		c := strings.TrimSpace(string(data))
		if c != "" {
			if !filepath.IsAbs(c) {
				c = filepath.Join(gitdir, c)
			}
			common = c
		}
	}
	common = filepath.Clean(common)

	// The shared .git of a non-bare repo sits inside the primary checkout.
	// Anything else (bare repo, exotic layout) has no primary checkout path
	// to resolve to.
	if filepath.Base(common) != ".git" {
		return ""
	}
	return filepath.Dir(common)
}
