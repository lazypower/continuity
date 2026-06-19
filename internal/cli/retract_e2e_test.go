//go:build !windows

package cli

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/lazypower/continuity/internal/store"
)

// TestRetract_SubprocessE2E_TFIDF exercises the retract / dedup-against-retracted
// agent-experience surface through a real subprocess — the level the in-process
// httptest tests in retract_integration_test.go cannot cover.
//
// What this test pins (per issue #21):
//   - Exit code semantics: os.Exit(2) on the dedup gate, 0 on success.
//   - stderr/stdout channel separation: humans/scripts read different fds for
//     different signals (success vs gate vs error).
//   - The exact text the CLI prints — agents parse it, so drift is breakage.
//   - Absence-of-leakage: the dedup gate must surface URIs but NEVER the
//     tombstone reason. PII captured in the reason field is the threat model.
//   - The `show <uri> --include-retracted` reveal path.
//   - The `remember --acknowledge-retracted` bypass path.
//
// Why TFIDF: the test runs in a clean-room CI environment that has no Ollama.
// Forcing CONTINUITY_EMBEDDER=tfidf removes the probe's environment dependency,
// and exercising the TFIDF code path in CI is itself the point — we ship it as
// a fallback and have not been testing it end-to-end.
//
// Why subprocess: in-process httptest tests share state and goroutine context
// with the test harness, which hides exit-code semantics, stderr-vs-stdout
// signaling, and any drift between code-path beliefs and the actual binary.
// Each invocation here is an independent process.
func TestRetract_SubprocessE2E_TFIDF(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess e2e: skipped under -short (builds a binary, spawns a server)")
	}

	bin := buildContinuityBinary(t)

	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "test.db")
	homeDir := filepath.Join(workDir, "home")
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Seed the DB with enough varied text that TFIDF builds a real vocabulary
	// covering the tokens our test queries use (operator, home, address,
	// discussion, captured, accident). Without this, NewTFIDFEmbedder produces
	// a 1-dim vector and every cosine similarity collapses to 0, so the
	// dedup-against-retracted gate cannot fire and the test would silently
	// fail to exercise its target invariant.
	seedTFIDFCorpus(t, dbPath)

	port := freeTCPPort(t)
	serverURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	procEnv := append(os.Environ(),
		"HOME="+homeDir,
		"CONTINUITY_DB="+dbPath,
		"CONTINUITY_PORT="+strconv.Itoa(port),
		"CONTINUITY_BIND=127.0.0.1",
		"CONTINUITY_EMBEDDER=tfidf",
		"CONTINUITY_URL="+serverURL,
	)

	srv := startServeProcess(t, bin, procEnv)
	t.Cleanup(srv.stop)
	waitForReady(t, serverURL+"/api/health")

	// Step 1 — remember original.
	step1 := runCLI(t, bin, procEnv, "remember",
		"-c", "events",
		"-n", "operator-home-address-discussion",
		"-s", "operator's full home address discussion",
		"-b", "Body content captured during conversation that has enough length to pass validation thresholds easily.",
	)
	step1.expectExit(t, 0).
		expectStdoutContains(t, "created:").
		expectStdoutContains(t, "mem://user/events/operator-home-address-discussion")

	// Step 2 — retract the original. Reason carries the PII-shaped marker; we
	// will assert this string never leaks via the gate's stderr or via the
	// default show output.
	const piiReason = "captured operator home address by accident; remove on sight"
	step2 := runCLI(t, bin, procEnv, "retract",
		"mem://user/events/operator-home-address-discussion",
		"--reason", piiReason,
	)
	step2.expectExit(t, 0).
		expectStdoutContains(t, "retracted:").
		expectStdoutContains(t, "mem://user/events/operator-home-address-discussion")

	// Step 3 — write a semantically similar memory under the same category.
	// The dedup-against-retracted gate must fire: exit 2, stderr names the
	// matched URI, stderr DOES NOT leak the reason. This is the agent-facing
	// contract from PR #20's design.
	step3 := runCLI(t, bin, procEnv, "remember",
		"-c", "events",
		"-n", "second-attempt-similar",
		"-s", "operator home address mentioned again in same discussion thread",
		"-b", "Different body content carrying enough length to pass validation easily as well.",
	)
	step3.expectExit(t, 2).
		expectStderrContains(t, "matches_retracted").
		expectStderrContains(t, "mem://user/events/operator-home-address-discussion").
		expectStdoutAbsent(t, "created:", "updated:") // stdout MUST stay clean on the gate path

	// Absence-of-leakage: the reason field is sequestered by contract. Verify
	// no fragment of the reason text appears in stderr.
	for _, leak := range []string{
		"captured operator",
		"home address by accident",
		"remove on sight",
		piiReason,
	} {
		if strings.Contains(step3.stderr, leak) {
			t.Errorf("step 3 stderr leaks tombstone reason via %q:\n%s", leak, step3.stderr)
		}
	}

	// Step 4 — show with --include-retracted reveals the reason deliberately.
	step4 := runCLI(t, bin, procEnv, "show",
		"mem://user/events/operator-home-address-discussion",
		"--include-retracted",
	)
	step4.expectExit(t, 0).
		expectStdoutContains(t, piiReason)

	// Step 5 — show WITHOUT the flag suppresses the reason and the body.
	step5 := runCLI(t, bin, procEnv, "show",
		"mem://user/events/operator-home-address-discussion",
	)
	step5.expectExit(t, 0)
	for _, leak := range []string{
		"captured operator",
		"home address by accident",
		"remove on sight",
		piiReason,
	} {
		if strings.Contains(step5.stdout, leak) {
			t.Errorf("step 5 (show without flag) leaks reason via %q:\n%s", leak, step5.stdout)
		}
	}
	if !strings.Contains(step5.stdout, "[retracted]") {
		t.Errorf("step 5 must mark the node as retracted in output:\n%s", step5.stdout)
	}

	// Step 6 — show --json without the flag omits reason/summary/body fields.
	step6 := runCLI(t, bin, procEnv, "show",
		"mem://user/events/operator-home-address-discussion",
		"--json",
	)
	step6.expectExit(t, 0)
	var jsonOut map[string]any
	if err := json.Unmarshal([]byte(step6.stdout), &jsonOut); err != nil {
		t.Fatalf("step 6 stdout is not valid JSON: %v\n%s", err, step6.stdout)
	}
	for _, key := range []string{"tombstone_reason", "summary", "body"} {
		if _, ok := jsonOut[key]; ok {
			t.Errorf("step 6 JSON must OMIT %q without --include-retracted; got: %v", key, jsonOut[key])
		}
	}
	if r, ok := jsonOut["retracted"]; !ok || r != true {
		t.Errorf("step 6 JSON must mark retracted=true; got: %v", jsonOut["retracted"])
	}

	// Step 7 — retry the same similar write with --acknowledge-retracted.
	// Gate bypassed, write succeeds, exit 0, stdout reports created.
	step7 := runCLI(t, bin, procEnv, "remember",
		"-c", "events",
		"-n", "second-attempt-similar",
		"-s", "operator home address mentioned again in same discussion thread",
		"-b", "Different body content carrying enough length to pass validation easily as well.",
		"--acknowledge-retracted",
	)
	step7.expectExit(t, 0).
		expectStdoutContains(t, "created:").
		expectStdoutContains(t, "mem://user/events/second-attempt-similar").
		expectStderrAbsent(t, "matches_retracted")

	// Step 8 — show the override write proves the override landed at the
	// expected URI (i.e. the gate-bypass path doesn't sneak the write onto a
	// timestamp-suffixed slug).
	step8 := runCLI(t, bin, procEnv, "show", "mem://user/events/second-attempt-similar")
	step8.expectExit(t, 0).
		expectStdoutContains(t, "mem://user/events/second-attempt-similar")

	// Step 9 — server shutdown is clean (cleanup will SIGTERM; assert in the
	// stop() helper that exit is 0 and stderr shows the expected drain line).
	// (Handled in srv.stop via t.Cleanup; nothing to do here.)
}

// ----- helpers below -----

// buildContinuityBinary compiles cmd/continuity with the noembed build tag so
// the binary builds without the UI assets the production Makefile bakes in.
// Returns the absolute path to the binary.
func buildContinuityBinary(t *testing.T) string {
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

// findRepoRoot walks up from the test file's directory until it finds a go.mod
// file, returning the directory containing it.
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

// seedTFIDFCorpus opens the SQLite DB directly and writes enough varied L0
// abstracts that NewTFIDFEmbedder has a real vocabulary covering the tokens
// the test queries use. Seeding here (not via the CLI) is intentional: it
// represents the production state where the user already has a populated DB
// when they invoke retract.
func seedTFIDFCorpus(t *testing.T, dbPath string) {
	t.Helper()
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("seed: open db: %v", err)
	}
	defer db.Close()

	seeds := []struct{ name, l0, l1 string }{
		{"operator-profile-setup", "operator profile setup steps completed", "Body about operator profile configuration with enough length to pass validation thresholds."},
		{"memory-audit-notes", "memory subsystem audit complete this morning", "Body about memory subsystem audit findings with enough length to pass validation thresholds."},
		{"address-book-sync", "address book sync operational notes morning", "Body about address book synchronization with enough length to pass validation thresholds."},
		{"discussion-thread-ref", "previous discussion thread reference for later", "Body about a previous discussion thread that has enough length to pass validation thresholds."},
		{"home-directory-layout", "home directory layout reviewed today", "Body about home directory layout that has enough length to pass validation thresholds."},
		{"accident-incident-log", "accident incident log captured for review", "Body about an accident incident log capture that has enough length to pass validation thresholds."},
		{"morning-standup-recap", "morning standup recap mentioned several topics", "Body about a morning standup recap that has enough length to pass validation thresholds."},
	}
	for _, s := range seeds {
		if err := db.CreateNode(&store.MemNode{
			URI:        "mem://user/events/" + s.name,
			NodeType:   "leaf",
			Category:   "events",
			L0Abstract: s.l0,
			L1Overview: s.l1,
		}); err != nil {
			t.Fatalf("seed %s: %v", s.name, err)
		}
	}
}

// freeTCPPort asks the kernel for a free TCP port, releases the listener, and
// returns the port number. There is a small race window between Close() and
// the subprocess Listen, acceptable for tests.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPPort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("freeTCPPort close: %v", err)
	}
	return port
}

// serverProcess wraps a running `continuity serve` child process with safe
// shutdown helpers.
type serverProcess struct {
	cmd       *exec.Cmd
	stderrBuf *bytes.Buffer
	stopped   bool
}

// startServeProcess spawns `continuity serve` with the given env and pipes its
// stderr to a buffer for post-mortem inspection. Returns once the process is
// running; callers must wait for /api/health before issuing CLI commands.
func startServeProcess(t *testing.T, bin string, env []string) *serverProcess {
	t.Helper()
	cmd := exec.Command(bin, "serve")
	cmd.Env = env

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	cmd.Stdout = io.Discard

	// Put the server in its own process group so we can deliver SIGTERM
	// without racing the test harness's signal handling.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start serve: %v", err)
	}
	return &serverProcess{cmd: cmd, stderrBuf: stderr}
}

// stop sends SIGTERM to the server's process group and waits up to 5 seconds
// for it to exit. Errors are reported via t.Errorf so cleanup never panics in
// flight.
func (s *serverProcess) stop() {
	if s.stopped {
		return
	}
	s.stopped = true

	// SIGTERM the process group. The leading minus targets the pgid.
	if s.cmd.Process != nil {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGTERM)
	}

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
		// graceful exit (or non-zero — we tolerate either; the test asserts
		// the meaningful surfaces elsewhere)
	case <-time.After(5 * time.Second):
		if s.cmd.Process != nil {
			_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		}
		<-done
	}
}

// waitForReady polls /api/health up to 10 seconds, failing the test if the
// server never responds 200. The poll interval is short so the test isn't
// dominated by startup latency.
func waitForReady(t *testing.T, healthURL string) {
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

// cliResult is the outcome of one CLI invocation: captured channels and exit
// code. Methods chain so call sites read top-down.
type cliResult struct {
	args     []string
	stdout   string
	stderr   string
	exitCode int
}

// runCLI executes the binary with the given args + env, capturing both
// channels and the exit code. It does NOT fail the test on non-zero exit —
// the caller does, via expectExit.
func runCLI(t *testing.T, bin string, env []string, args ...string) *cliResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := &cliResult{
		args:   args,
		stdout: stdout.String(),
		stderr: stderr.String(),
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		res.exitCode = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("runCLI: launch failed for args %v: %v\nstderr:\n%s", args, err, stderr.String())
	}
	return res
}

func (r *cliResult) expectExit(t *testing.T, want int) *cliResult {
	t.Helper()
	if r.exitCode != want {
		t.Errorf("args %v: exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			r.args, r.exitCode, want, r.stdout, r.stderr)
	}
	return r
}

func (r *cliResult) expectStdoutContains(t *testing.T, frag string) *cliResult {
	t.Helper()
	if !strings.Contains(r.stdout, frag) {
		t.Errorf("args %v: stdout missing %q\nstdout:\n%s", r.args, frag, r.stdout)
	}
	return r
}

func (r *cliResult) expectStderrContains(t *testing.T, frag string) *cliResult {
	t.Helper()
	if !strings.Contains(r.stderr, frag) {
		t.Errorf("args %v: stderr missing %q\nstderr:\n%s", r.args, frag, r.stderr)
	}
	return r
}

func (r *cliResult) expectStdoutAbsent(t *testing.T, frags ...string) *cliResult {
	t.Helper()
	for _, f := range frags {
		if strings.Contains(r.stdout, f) {
			t.Errorf("args %v: stdout must NOT contain %q\nstdout:\n%s", r.args, f, r.stdout)
		}
	}
	return r
}

func (r *cliResult) expectStderrAbsent(t *testing.T, frags ...string) *cliResult {
	t.Helper()
	for _, f := range frags {
		if strings.Contains(r.stderr, f) {
			t.Errorf("args %v: stderr must NOT contain %q\nstderr:\n%s", r.args, f, r.stderr)
		}
	}
	return r
}
