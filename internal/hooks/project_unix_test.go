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

// A FIFO named .git must not be opened: a blocking read with no writer would
// hang the hook forever. The walk falls back to the raw cwd.
func TestProjectIdentity_FifoGitFileDoesNotHang(t *testing.T) {
	tmp := t.TempDir()
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
	tmp := t.TempDir()
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
