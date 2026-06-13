// Package testharness provides shared scaffolding for subprocess end-to-end
// tests. It is NOT a _test.go-only package — exposing it as a regular package
// lets every test package import it (Go's test-package rules forbid importing
// _test files across package boundaries).
//
// Build a binary, start `continuity serve`, wait for /api/health, invoke the
// CLI as a real child process, capture stdout/stderr/exit. The same harness
// powers internal/cli/retract_e2e_test.go and internal/hooks/hooks_e2e_test.go;
// extending it for migration / install / concurrency tests should be cheap.
package testharness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// BuildContinuityBinary compiles cmd/continuity with the noembed build tag so
// the binary builds without the UI assets `make ui` bakes in. Returns the
// absolute path to the built binary.
func BuildContinuityBinary(t *testing.T) string {
	t.Helper()
	binDir := t.TempDir()
	binName := "continuity"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(binDir, binName)

	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("findRepoRoot: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-tags", "noembed", "-o", binPath, "./cmd/continuity")
	cmd.Dir = repoRoot
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build: %v\n%s", err, stderr.String())
	}
	return binPath
}

// findRepoRoot walks up from the test cwd until it finds a go.mod file.
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found in any parent of test cwd")
		}
		dir = parent
	}
}

// FreeTCPPort asks the kernel for a free TCP port, releases the listener, and
// returns the port. There is a small race window between Close() and the
// subprocess Listen — acceptable for tests.
func FreeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("FreeTCPPort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("FreeTCPPort close: %v", err)
	}
	return port
}

// ServerProcess wraps a running `continuity serve` child process. Stderr is
// captured to a buffer so tests can inspect server-side log lines (the server
// uses Go's log package which writes to stderr).
type ServerProcess struct {
	cmd       *exec.Cmd
	stderrBuf *bytes.Buffer
	stopped   bool
}

// StartServeProcess spawns `continuity serve` with the given env. Callers
// MUST call WaitForReady before issuing CLI commands and SHOULD register
// Stop with t.Cleanup.
func StartServeProcess(t *testing.T, bin string, env []string) *ServerProcess {
	t.Helper()
	cmd := exec.Command(bin, "serve")
	cmd.Env = env

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	cmd.Stdout = io.Discard

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start serve: %v", err)
	}
	return &ServerProcess{cmd: cmd, stderrBuf: stderr}
}

// Stderr returns the server's accumulated stderr. Safe to call repeatedly;
// reflects whatever the server has logged up to the moment of the call.
func (s *ServerProcess) Stderr() string { return s.stderrBuf.String() }

// Stop sends SIGTERM to the server's process group and waits up to 5 seconds
// for a graceful exit. SIGKILL if it overshoots.
func (s *ServerProcess) Stop() {
	if s.stopped {
		return
	}
	s.stopped = true

	if s.cmd.Process != nil {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGTERM)
	}

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		if s.cmd.Process != nil {
			_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		}
		<-done
	}
}

// WaitForReady polls /api/health up to 10 seconds; t.Fatal if the server
// never responds 200.
func WaitForReady(t *testing.T, healthURL string) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server did not become ready at %s within 10s", healthURL)
}

// WaitForCondition polls check() until it returns true or the deadline
// elapses. msg is printed via t.Fatalf on timeout. Used to wait for async
// server-side side effects (extraction goroutines, DB writes).
func WaitForCondition(t *testing.T, timeout time.Duration, msg string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("waitForCondition timeout: %s", msg)
}

// CLIResult is the outcome of one CLI invocation: captured channels and exit
// code. Methods chain so call sites read top-down.
type CLIResult struct {
	Args     []string
	Stdout   string
	Stderr   string
	ExitCode int
}

// RunCLI executes the binary with the given args + env, capturing both
// channels and the exit code. Caller asserts via ExpectExit; non-zero exits
// do not Fatal here.
func RunCLI(t *testing.T, bin string, env []string, args ...string) *CLIResult {
	t.Helper()
	return RunCLIWithStdin(t, bin, env, "", args...)
}

// RunCLIWithStdin is RunCLI with a stdin payload. The hook subprocess tests
// use this to feed the JSON Claude Code would normally pipe to a hook.
func RunCLIWithStdin(t *testing.T, bin string, env []string, stdin string, args ...string) *CLIResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := &CLIResult{
		Args:   args,
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		res.ExitCode = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("RunCLI: launch failed for args %v: %v\nstderr:\n%s", args, err, stderr.String())
	}
	return res
}

// ExpectExit asserts the captured exit code matches want.
func (r *CLIResult) ExpectExit(t *testing.T, want int) *CLIResult {
	t.Helper()
	if r.ExitCode != want {
		t.Errorf("args %v: exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			r.Args, r.ExitCode, want, r.Stdout, r.Stderr)
	}
	return r
}

// ExpectStdoutContains asserts stdout contains every fragment passed.
func (r *CLIResult) ExpectStdoutContains(t *testing.T, frags ...string) *CLIResult {
	t.Helper()
	for _, f := range frags {
		if !strings.Contains(r.Stdout, f) {
			t.Errorf("args %v: stdout missing %q\nstdout:\n%s", r.Args, f, r.Stdout)
		}
	}
	return r
}

// ExpectStderrContains asserts stderr contains every fragment passed.
func (r *CLIResult) ExpectStderrContains(t *testing.T, frags ...string) *CLIResult {
	t.Helper()
	for _, f := range frags {
		if !strings.Contains(r.Stderr, f) {
			t.Errorf("args %v: stderr missing %q\nstderr:\n%s", r.Args, f, r.Stderr)
		}
	}
	return r
}

// ExpectStdoutAbsent asserts stdout contains NONE of the given fragments.
func (r *CLIResult) ExpectStdoutAbsent(t *testing.T, frags ...string) *CLIResult {
	t.Helper()
	for _, f := range frags {
		if strings.Contains(r.Stdout, f) {
			t.Errorf("args %v: stdout must NOT contain %q\nstdout:\n%s", r.Args, f, r.Stdout)
		}
	}
	return r
}

// ExpectStderrAbsent asserts stderr contains NONE of the given fragments.
func (r *CLIResult) ExpectStderrAbsent(t *testing.T, frags ...string) *CLIResult {
	t.Helper()
	for _, f := range frags {
		if strings.Contains(r.Stderr, f) {
			t.Errorf("args %v: stderr must NOT contain %q\nstderr:\n%s", r.Args, f, r.Stderr)
		}
	}
	return r
}

// HermeticEnv is the canonical env-var set used by every subprocess test:
// hermetic DB, hermetic HOME, kernel-allocated port, TFIDF forced (no Ollama
// probe), bound to loopback, CONTINUITY_URL pointed at the chosen port.
//
// Returns the resolved server URL and the env slice ready to pass to
// StartServeProcess / RunCLI. The DB path is filled in via dbPath; the rest
// of workDir is left to the caller for any per-test fixtures.
func HermeticEnv(t *testing.T, workDir string, dbPath string, port int) (string, []string) {
	t.Helper()
	if port == 0 {
		port = FreeTCPPort(t)
	}
	homeDir := filepath.Join(workDir, "home")
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		t.Fatalf("mkdir HOME: %v", err)
	}
	serverURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	env := append(os.Environ(),
		"HOME="+homeDir,
		"CONTINUITY_DB="+dbPath,
		"CONTINUITY_PORT="+strconv.Itoa(port),
		"CONTINUITY_BIND=127.0.0.1",
		"CONTINUITY_EMBEDDER=tfidf",
		"CONTINUITY_URL="+serverURL,
	)
	return serverURL, env
}
