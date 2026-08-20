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
			// Only a regular file can be a worktree pointer. Anything else
			// (FIFO, device, socket) must not be opened — a blocking read
			// here would hang the SessionStart hook, and a hook must fail
			// back to the raw cwd, never hang (#79).
			if fi.Mode().IsRegular() {
				if root := primaryCheckoutFromGitFile(dir, gitPath); root != "" {
					return root
				}
			}
			return cwd
		} else if _, lerr := os.Lstat(gitPath); lerr == nil {
			// The entry exists but cannot be classified (broken symlink,
			// symlink cycle, permission failure). Ascending past it could
			// resolve to an ENCLOSING repository this directory does not
			// belong to — an unresolvable layout fails back to the raw cwd,
			// never to a guess.
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
// maxGitMetadataBytes bounds reads of git metadata files (.git pointer,
// commondir). Real ones are a single short line; anything larger is not git
// metadata and must not be slurped into hook memory.
const maxGitMetadataBytes = 1 << 20

// readGitMetadata reads a git metadata file, refusing non-regular files (a
// FIFO would block the hook forever) and oversized ones.
func readGitMetadata(path string) ([]byte, bool) {
	fi, err := os.Stat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Size() > maxGitMetadataBytes {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	return data, true
}

// samePath reports whether two cleaned paths name the same file, tolerating
// symlinked path components (macOS /tmp → /private/tmp): git may record one
// spelling in the back-pointer while the hook's cwd carries the other.
// Unresolvable paths are simply unequal — the caller fails back to raw cwd.
func samePath(a, b string) bool {
	if a == b {
		return true
	}
	ra, err := filepath.EvalSymlinks(a)
	if err != nil {
		return false
	}
	rb, err := filepath.EvalSymlinks(b)
	if err != nil {
		return false
	}
	return ra == rb
}

func primaryCheckoutFromGitFile(worktreeRoot, gitFile string) string {
	data, ok := readGitMetadata(gitFile)
	if !ok {
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
	gitdir = filepath.Clean(gitdir)

	// Verify git's own worktree linkage before trusting the layout: a real
	// linked worktree's gitdir carries a `gitdir` back-pointer file naming the
	// worktree's .git file (written by `git worktree add`, maintained by
	// `git worktree repair`). Without this check, a crafted .git file could
	// point at ANY directory ending in .git and claim that repository's
	// identity — the walk must never escape to a repo the metadata merely
	// names. No back-pointer, or a back-pointer naming somewhere else → not
	// this worktree's gitdir → fall back to the raw cwd.
	back, ok := readGitMetadata(filepath.Join(gitdir, "gitdir"))
	if !ok {
		return ""
	}
	backPath := strings.TrimSpace(string(back))
	if backPath == "" {
		return ""
	}
	if !filepath.IsAbs(backPath) {
		backPath = filepath.Join(gitdir, backPath)
	}
	if !samePath(filepath.Clean(backPath), gitFile) {
		return ""
	}

	common := gitdir
	if data, ok := readGitMetadata(filepath.Join(gitdir, "commondir")); ok {
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
