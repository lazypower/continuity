//go:build unix

package hooks

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// projectIdentityWithin runs projectIdentity and fails the test if it does
// not return within the deadline — the hang itself is the defect under test:
// a SessionStart hook must fail back to the raw cwd, never block.
func projectIdentityWithin(t *testing.T, cwd string, deadline time.Duration) string {
	t.Helper()
	done := make(chan string, 1)
	go func() { done <- projectIdentity(cwd) }()
	select {
	case got := <-done:
		return got
	case <-time.After(deadline):
		t.Fatalf("projectIdentity(%q) hung past %s", cwd, deadline)
		return ""
	}
}

// One repository, one identity spelling: entering the primary checkout
// through a symlinked path (macOS /tmp → /private/tmp) must yield the same
// identity as entering it directly, or the affinity join key fragments.
func TestProjectIdentity_SymlinkedEntryCanonicalizes(t *testing.T) {
	tmp := canonicalTempDir(t)
	main := mkdirAll(t, filepath.Join(tmp, "repo"))
	gitRepo(t, main)

	alias := filepath.Join(tmp, "alias")
	if err := os.Symlink(main, alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	if got := projectIdentity(alias); got != main {
		t.Errorf("projectIdentity(%q) = %q, want canonical %q", alias, got, main)
	}
	if direct := projectIdentity(main); direct != projectIdentity(alias) {
		t.Errorf("identity fragments on spelling: %q vs %q", direct, projectIdentity(alias))
	}
}

// A FIFO named .git must not be opened: a blocking read with no writer would
// hang the hook forever. The walk falls back to the raw cwd.
func TestProjectIdentity_FifoGitFileDoesNotHang(t *testing.T) {
	tmp := canonicalTempDir(t)
	dir := filepath.Join(tmp, "fifo-git")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, ".git"), 0o644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}

	if got := projectIdentityWithin(t, dir, 5*time.Second); got != dir {
		t.Errorf("projectIdentity(%q) = %q, want raw cwd", dir, got)
	}
}

// A FIFO commondir inside an otherwise valid worktree layout must likewise be
// refused; resolution fails and the raw cwd is kept.
func TestProjectIdentity_FifoCommondirDoesNotHang(t *testing.T) {
	tmp := canonicalTempDir(t)
	main := filepath.Join(tmp, "repo")
	gitRepo(t, main)

	wt := filepath.Join(tmp, "fifo-wt")
	gitdir := mkdirAll(t, filepath.Join(main, ".git", "worktrees", "fifo"))
	if err := syscall.Mkfifo(filepath.Join(gitdir, "commondir"), 0o644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	writeFile(t, filepath.Join(gitdir, "gitdir"), filepath.Join(wt, ".git")+"\n")
	mkdirAll(t, wt)
	writeFile(t, filepath.Join(wt, ".git"), "gitdir: "+gitdir+"\n")

	if got := projectIdentityWithin(t, wt, 5*time.Second); got != wt {
		t.Errorf("projectIdentity(%q) = %q, want raw cwd", wt, got)
	}
}
