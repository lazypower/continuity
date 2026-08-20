package hooks

import (
	"os"
	"path/filepath"
	"testing"
)

// canonicalTempDir returns a symlink-resolved t.TempDir(): projectIdentity
// canonicalizes resolved identities (macOS /var → /private/var), so tests
// build layouts on physical paths to compare spellings directly.
func canonicalTempDir(t *testing.T) string {
	t.Helper()
	p, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks(TempDir): %v", err)
	}
	return p
}

// mustEvalSymlinks resolves a path's symlinked components, fatally on error.
func mustEvalSymlinks(t *testing.T, p string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatalf("EvalSymlinks(%s): %v", p, err)
	}
	return r
}

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
// <main>/.git/worktrees/<name>/commondir → "../..", the `gitdir` back-pointer
// naming the worktree's .git file, and <wt>/.git →
// "gitdir: <main>/.git/worktrees/<name>".
func linkedWorktree(t *testing.T, main, wt, name string) {
	t.Helper()
	gitdir := mkdirAll(t, filepath.Join(main, ".git", "worktrees", name))
	writeFile(t, filepath.Join(gitdir, "commondir"), "../..\n")
	writeFile(t, filepath.Join(gitdir, "gitdir"), filepath.Join(wt, ".git")+"\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: "+gitdir+"\n")
}

func TestProjectIdentity(t *testing.T) {
	tmp := canonicalTempDir(t)

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
	tmp := canonicalTempDir(t)
	main := mkdirAll(t, filepath.Join(tmp, "repo"))
	gitRepo(t, main)

	wt := filepath.Join(tmp, "rel-wt")
	gitdir := mkdirAll(t, filepath.Join(main, ".git", "worktrees", "rel"))
	writeFile(t, filepath.Join(gitdir, "commondir"), "../..\n")
	// Relative back-pointer — legal alongside a relative gitdir.
	writeFile(t, filepath.Join(gitdir, "gitdir"), "../../../../rel-wt/.git\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: ../repo/.git/worktrees/rel\n")

	if got := projectIdentity(wt); got != main {
		t.Errorf("projectIdentity(%q) = %q, want %q", wt, got, main)
	}
}

// TestProjectIdentity_ForeignGitdirRejected: a crafted .git file naming an
// arbitrary directory that happens to end in .git must NOT claim that
// repository's identity. Git's worktree linkage is bidirectional — the gitdir
// carries a back-pointer to the worktree's .git file — and resolution requires
// it; a pointer without the back-link falls back to the raw cwd.
func TestProjectIdentity_ForeignGitdirRejected(t *testing.T) {
	tmp := canonicalTempDir(t)
	victim := mkdirAll(t, filepath.Join(tmp, "victim"))
	gitRepo(t, victim)

	attacker := mkdirAll(t, filepath.Join(tmp, "attacker"))
	writeFile(t, filepath.Join(attacker, ".git"), "gitdir: "+filepath.Join(victim, ".git")+"\n")

	if got := projectIdentity(attacker); got != attacker {
		t.Errorf("projectIdentity(%q) = %q — crafted gitdir claimed %q; want raw cwd", attacker, got, victim)
	}
}

// TestProjectIdentity_MismatchedBackPointerRejected: a gitdir whose
// back-pointer names some OTHER worktree is not this worktree's gitdir.
func TestProjectIdentity_MismatchedBackPointerRejected(t *testing.T) {
	tmp := canonicalTempDir(t)
	main := mkdirAll(t, filepath.Join(tmp, "repo"))
	gitRepo(t, main)

	real := filepath.Join(tmp, "real-wt")
	linkedWorktree(t, main, real, "real")

	// Imposter points at the real worktree's gitdir; its back-pointer names
	// real-wt/.git, not imposter/.git.
	imposter := mkdirAll(t, filepath.Join(tmp, "imposter"))
	writeFile(t, filepath.Join(imposter, ".git"),
		"gitdir: "+filepath.Join(main, ".git", "worktrees", "real")+"\n")

	if got := projectIdentity(imposter); got != imposter {
		t.Errorf("projectIdentity(%q) = %q, want raw cwd", imposter, got)
	}
}

// TestProjectIdentity_ForgedCommondirRejected: a cwd-controlled fake gitdir
// can satisfy the back-pointer check (it points back at its own .git) while
// its commondir names a victim repository's .git. The commondir is only
// trustworthy inside real worktree geometry — gitdir at <common>/worktrees/<n>
// — so the forgery must fall back to the raw cwd, not the victim's identity.
func TestProjectIdentity_ForgedCommondirRejected(t *testing.T) {
	tmp := canonicalTempDir(t)
	victim := mkdirAll(t, filepath.Join(tmp, "victim"))
	gitRepo(t, victim)

	attacker := mkdirAll(t, filepath.Join(tmp, "attacker"))
	fakeGitdir := mkdirAll(t, filepath.Join(attacker, "fakegit"))
	writeFile(t, filepath.Join(attacker, ".git"), "gitdir: "+fakeGitdir+"\n")
	writeFile(t, filepath.Join(fakeGitdir, "gitdir"), filepath.Join(attacker, ".git")+"\n")
	writeFile(t, filepath.Join(fakeGitdir, "commondir"), filepath.Join(victim, ".git")+"\n")

	if got := projectIdentity(attacker); got != attacker {
		t.Errorf("projectIdentity(%q) = %q — forged commondir claimed %q; want raw cwd", attacker, got, victim)
	}
}

// TestProjectIdentity_BrokenGitSymlinkFailsBack: an entry named .git that
// exists but cannot be classified (broken symlink) must fail back to the raw
// cwd — ascending past it would resolve to an enclosing repository the
// directory does not belong to.
func TestProjectIdentity_BrokenGitSymlinkFailsBack(t *testing.T) {
	tmp := canonicalTempDir(t)
	parent := mkdirAll(t, filepath.Join(tmp, "parent"))
	gitRepo(t, parent)

	child := mkdirAll(t, filepath.Join(parent, "child"))
	if err := os.Symlink(filepath.Join(tmp, "does-not-exist"), filepath.Join(child, ".git")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	if got := projectIdentity(child); got != child {
		t.Errorf("projectIdentity(%q) = %q — broken .git symlink ascended to %q; want raw cwd", child, got, parent)
	}
}

// TestProjectIdentity_BareCommonDir: when the common dir doesn't sit inside a
// primary checkout (bare repo), there is no primary path to resolve to — the
// raw cwd is kept rather than a guess.
func TestProjectIdentity_BareCommonDir(t *testing.T) {
	tmp := canonicalTempDir(t)
	bare := mkdirAll(t, filepath.Join(tmp, "repo.git"))

	wt := filepath.Join(tmp, "bare-wt")
	gitdir := mkdirAll(t, filepath.Join(bare, "worktrees", "w"))
	writeFile(t, filepath.Join(gitdir, "commondir"), "../..\n")
	writeFile(t, filepath.Join(gitdir, "gitdir"), filepath.Join(wt, ".git")+"\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: "+gitdir+"\n")

	if got := projectIdentity(wt); got != wt {
		t.Errorf("projectIdentity(%q) = %q, want raw cwd %q", wt, got, wt)
	}
}
