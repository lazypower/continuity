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
		// Lstat, not Stat: git only ever writes .git as a real directory or a
		// real regular file. A .git that is a SYMLINK is not a git-authored
		// entry — following it (os.Stat does) would let a cwd-local symlink
		// point at a foreign linked worktree's real .git and inherit that
		// repository's identity. Any non-plain entry (symlink, FIFO, device)
		// fails back to the raw cwd: never guessed, never hung (#79).
		if fi, err := os.Lstat(gitPath); err == nil {
			if fi.IsDir() {
				return canonicalIdentity(dir)
			}
			if fi.Mode().IsRegular() {
				if root := primaryCheckoutFromGitFile(dir, gitPath); root != "" {
					return canonicalIdentity(root)
				}
			}
			// The entry exists but is not a plain dir/file (symlink, cycle,
			// FIFO) or did not resolve. Ascending past it could bind to an
			// ENCLOSING repository this directory does not belong to, so it
			// fails back to the raw cwd rather than to a guess.
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
//
// This resolves LINKED WORKTREES only — the #79 fragmentation case. Other
// gitfile layouts (submodules, `--separate-git-dir`) have no worktree
// back-pointer / no commondir, so they return "" and the session keeps its
// raw cwd: shape-only, never a wrong identity. Normalizing those is outside
// #79's scope, and their raw-cwd affinity is an accepted limit, not a
// regression — the same layouts fell back before this hardening.
// maxGitMetadataBytes bounds reads of git metadata files (.git pointer,
// commondir). Real ones are a single short line; anything larger is not git
// metadata and must not be slurped into hook memory.
const maxGitMetadataBytes = 1 << 20

// readGitMetadata reads a git metadata file, refusing non-regular files (a
// FIFO would block the hook forever) and oversized ones. The check-then-read
// is not atomic; an actor racing file swaps in the cwd mid-hook is the local
// FS owner and out of the threat model — the guard exists for static hostile
// layouts (an extracted tarball, a crafted checkout).
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

// canonicalIdentity resolves symlinked path components so one repository
// yields ONE identity spelling however the session entered it (macOS /tmp vs
// /private/tmp; a symlinked ~/Code): sessions at the primary checkout carry
// the cwd's spelling while worktree resolution carries git metadata's, and
// the affinity join key must not fragment on the difference. Applied only to
// a RESOLVED identity — a raw-cwd fallback stays exactly what the hook
// received. Lexical path on resolution failure.
func canonicalIdentity(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return p
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
			c = filepath.Clean(c)
			// A linked worktree's gitdir lives at <common>/worktrees/<name>
			// — that geometry is what makes commondir trustworthy. A
			// commondir naming anything else is crafted metadata trying to
			// select a foreign repository's identity (the gitdir back-pointer
			// alone can't prevent this: a cwd-controlled fake gitdir can
			// point back at its own .git while commondir names a victim).
			if !samePath(filepath.Dir(filepath.Dir(gitdir)), c) {
				return ""
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
