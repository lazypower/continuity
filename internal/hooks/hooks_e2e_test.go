//go:build !windows

package hooks

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lazypower/continuity/internal/store"
	"github.com/lazypower/continuity/internal/testharness"
)

// hookE2E is the shared per-test scaffold: a built binary, a running serve
// process with TFIDF forced, env vars wired, a DB handle for state
// assertions, and the server URL. Sub-tests build their own stdin payloads
// and call the binary via the testharness CLI helpers.
type hookE2E struct {
	bin       string
	env       []string
	workDir   string
	dbPath    string
	serverURL string
	srv       *testharness.ServerProcess
	db        *store.DB
}

// setupHookE2E builds the binary, starts the server with a hermetic env, and
// opens a second DB handle for the test to inspect server-mutated state.
// SQLite WAL mode lets the test's connection coexist with the server's.
func setupHookE2E(t *testing.T) *hookE2E {
	t.Helper()
	if testing.Short() {
		t.Skip("hooks subprocess e2e: skipped under -short")
	}

	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "test.db")

	serverURL, env := testharness.HermeticEnv(t, workDir, dbPath, 0)
	srv := testharness.StartServeProcess(t, bin, env)
	t.Cleanup(srv.Stop)
	testharness.WaitForReady(t, serverURL+"/api/health")

	// Open the DB AFTER serve has started so migrations have run.
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open db for assertions: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return &hookE2E{
		bin:       bin,
		env:       env,
		workDir:   workDir,
		dbPath:    dbPath,
		serverURL: serverURL,
		srv:       srv,
		db:        db,
	}
}

// runHook is the hook-subprocess analogue of testharness.RunCLI: it pipes a
// JSON-encoded HookInput to `bin hook <event>` and captures the result.
func (h *hookE2E) runHook(t *testing.T, event string, input HookInput) *testharness.CLIResult {
	t.Helper()
	stdin, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal hook input: %v", err)
	}
	return testharness.RunCLIWithStdin(t, h.bin, h.env, string(stdin), "hook", event)
}

// writeTranscript writes a synthetic Claude Code JSONL transcript with the
// given number of user / assistant message pairs and returns the file path.
// userText is repeated per message so the condensed length can be tuned by
// the caller (low-message tests vs past-threshold tests).
func writeTranscript(t *testing.T, dir string, name string, userMessages int, userText, assistantText string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	var b strings.Builder
	for i := 0; i < userMessages; i++ {
		userMsg := map[string]any{
			"type": "user",
			"message": map[string]any{
				"role":    "user",
				"content": fmt.Sprintf("%s (turn %d)", userText, i+1),
			},
		}
		line, _ := json.Marshal(userMsg)
		b.Write(line)
		b.WriteString("\n")

		asstMsg := map[string]any{
			"type": "assistant",
			"message": map[string]any{
				"role":    "assistant",
				"content": fmt.Sprintf("%s (turn %d)", assistantText, i+1),
			},
		}
		line, _ = json.Marshal(asstMsg)
		b.Write(line)
		b.WriteString("\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	return path
}

// =========================================================================
// SessionStart contract
// =========================================================================

// TestHookStart_SubprocessE2E_WritesContextJSON pins the agent contract for
// SessionStart: stdout MUST be a valid SessionStartOutput JSON document with
// the hookSpecificOutput.hookEventName field set to "SessionStart", because
// Claude Code parses this to discover the context-injection payload. Exit
// code MUST be 0 (hooks must never crash Claude — see ExitError in output.go).
func TestHookStart_SubprocessE2E_WritesContextJSON(t *testing.T) {
	h := setupHookE2E(t)

	res := h.runHook(t, "start", HookInput{
		SessionID:     "test-session-start-1",
		HookEventName: "SessionStart",
		Source:        "startup",
	})
	res.ExpectExit(t, 0)

	var out SessionStartOutput
	if err := json.Unmarshal([]byte(res.Stdout), &out); err != nil {
		t.Fatalf("stdout is not valid SessionStartOutput JSON: %v\nstdout:\n%s", err, res.Stdout)
	}
	if out.HookSpecificOutput.HookEventName != "SessionStart" {
		t.Errorf("hookEventName = %q, want SessionStart", out.HookSpecificOutput.HookEventName)
	}
}

// TestHookStart_SubprocessE2E_ServerDownDegradesGracefully pins the
// graceful-degradation contract from handler.go:25-30. With the server
// stopped, SessionStart must still produce a valid empty-context JSON
// document and exit 0. Crashing or returning malformed JSON here would
// break every Claude Code session that fires while the server is restarting.
func TestHookStart_SubprocessE2E_ServerDownDegradesGracefully(t *testing.T) {
	h := setupHookE2E(t)
	h.srv.Stop() // intentional: simulate server-down before the hook fires

	// Disable autostart by pointing HOME away from any persisted plist/unit.
	// HermeticEnv already does this — the tempdir HOME has no
	// LaunchAgents / systemd-unit files, so TryAutostart returns false.

	res := h.runHook(t, "start", HookInput{
		SessionID:     "test-server-down",
		HookEventName: "SessionStart",
	})
	res.ExpectExit(t, 0)

	var out SessionStartOutput
	if err := json.Unmarshal([]byte(res.Stdout), &out); err != nil {
		t.Fatalf("server-down stdout is not valid JSON: %v\nstdout:\n%s", err, res.Stdout)
	}
	if out.HookSpecificOutput.AdditionalContext != "" {
		t.Errorf("server-down context should be empty; got %q", out.HookSpecificOutput.AdditionalContext)
	}
	if out.HookSpecificOutput.HookEventName != "SessionStart" {
		t.Errorf("server-down hookEventName drift: %q", out.HookSpecificOutput.HookEventName)
	}
}

// =========================================================================
// UserPromptSubmit contract
// =========================================================================

// TestHookSubmit_SubprocessE2E_CreatesSession pins that a fresh
// UserPromptSubmit POSTs /api/sessions/init and the server creates the
// sessions row. Exit 0, no stdout (hooks except SessionStart must stay
// silent — anything they print would be injected into Claude's context).
func TestHookSubmit_SubprocessE2E_CreatesSession(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-submit-init-" + time.Now().Format("150405.000000")
	res := h.runHook(t, "submit", HookInput{
		SessionID: sessionID,
		CWD:       "/tmp/test-project",
		Prompt:    "ordinary user message",
	})
	res.ExpectExit(t, 0)
	if strings.TrimSpace(res.Stdout) != "" {
		t.Errorf("non-start hook leaked to stdout (would contaminate Claude context):\n%s", res.Stdout)
	}

	testharness.WaitForCondition(t, 2*time.Second,
		fmt.Sprintf("session %q should be created after submit", sessionID),
		func() bool {
			sess, _ := h.db.GetSession(sessionID)
			return sess != nil
		})
}

// TestHookSubmit_SubprocessE2E_InternalSentinelSkipsInit pins the
// anti-recursion guard from submit.go:31. When the server calls `claude -p`
// for extraction, that spawns a new Claude Code session whose hooks fire
// back into us; the sentinel prefix tells us to bail before init. Without
// this guard, extraction would recursively trigger more extractions.
func TestHookSubmit_SubprocessE2E_InternalSentinelSkipsInit(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-internal-sentinel-" + time.Now().Format("150405.000000")
	res := h.runHook(t, "submit", HookInput{
		SessionID: sessionID,
		CWD:       "/tmp/test-project",
		Prompt:    internalSentinel + " extraction call",
	})
	res.ExpectExit(t, 0)
	if strings.TrimSpace(res.Stdout) != "" {
		t.Errorf("internal-sentinel submit leaked to stdout:\n%s", res.Stdout)
	}

	// Wait long enough that an init would have landed if it were going to,
	// then assert it didn't.
	time.Sleep(200 * time.Millisecond)
	sess, err := h.db.GetSession(sessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess != nil {
		t.Errorf("internal-sentinel prompt must NOT create session %q (recursion guard); got %+v", sessionID, sess)
	}
}

// TestHookSubmit_SubprocessE2E_SignalTriggerReachesServer pins the signal
// keyword path: prompts containing "remember this" (etc.) trigger a
// fire-and-forget POST to /api/sessions/<id>/signal. In CI there is no LLM
// configured, so the server-side ExtractSignal call fails — the failure log
// line is our proof the route was reached. (Successful extraction depends on
// the LLM and is covered by engine-layer tests.)
func TestHookSubmit_SubprocessE2E_SignalTriggerReachesServer(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-signal-trigger-" + time.Now().Format("150405.000000")
	res := h.runHook(t, "submit", HookInput{
		SessionID: sessionID,
		CWD:       "/tmp/test-project",
		Prompt:    "remember this: the cache eviction policy is LRU not LFU",
	})
	res.ExpectExit(t, 0)

	testharness.WaitForCondition(t, 3*time.Second,
		"server should attempt signal extraction (and fail without LLM)",
		func() bool {
			return strings.Contains(h.srv.Stderr(),
				fmt.Sprintf("signal extraction failed for %s", sessionID))
		})
}

// =========================================================================
// PostToolUse contract
// =========================================================================

// TestHookTool_SubprocessE2E_RecordsObservation pins the happy path: a tool
// invocation produces exactly one observations row attributed to the session.
func TestHookTool_SubprocessE2E_RecordsObservation(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-tool-record-" + time.Now().Format("150405.000000")
	// Pre-init the session so foreign-key-ish coupling is realistic.
	_ = h.runHook(t, "submit", HookInput{
		SessionID: sessionID, CWD: "/tmp", Prompt: "init",
	}).ExpectExit(t, 0)
	testharness.WaitForCondition(t, 2*time.Second,
		"session created via submit",
		func() bool { s, _ := h.db.GetSession(sessionID); return s != nil })

	res := h.runHook(t, "tool", HookInput{
		SessionID:    sessionID,
		ToolName:     "Write",
		ToolInput:    json.RawMessage(`{"file_path":"/tmp/x.txt","content":"hi"}`),
		ToolResponse: json.RawMessage(`{"success":true}`),
	})
	res.ExpectExit(t, 0)
	if strings.TrimSpace(res.Stdout) != "" {
		t.Errorf("tool hook leaked to stdout:\n%s", res.Stdout)
	}

	testharness.WaitForCondition(t, 2*time.Second,
		"observation should be persisted",
		func() bool {
			c, _ := h.db.GetSessionObservationCount(sessionID)
			return c >= 1
		})
}

// TestHookTool_SubprocessE2E_SkipsMetaTools pins the skipTools allowlist
// from input.go: TodoRead, Thinking, TaskList, etc. must NOT produce
// observations rows, because they are meta-noise that would crowd out real
// tool signals during extraction.
func TestHookTool_SubprocessE2E_SkipsMetaTools(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-tool-skip-" + time.Now().Format("150405.000000")
	_ = h.runHook(t, "submit", HookInput{
		SessionID: sessionID, CWD: "/tmp", Prompt: "init",
	}).ExpectExit(t, 0)
	testharness.WaitForCondition(t, 2*time.Second,
		"session created",
		func() bool { s, _ := h.db.GetSession(sessionID); return s != nil })

	for _, skip := range []string{"TodoRead", "TodoWrite", "Thinking", "TaskList", "TaskCreate", "TaskGet", "TaskUpdate"} {
		res := h.runHook(t, "tool", HookInput{
			SessionID:    sessionID,
			ToolName:     skip,
			ToolInput:    json.RawMessage(`{}`),
			ToolResponse: json.RawMessage(`{}`),
		})
		res.ExpectExit(t, 0)
	}

	// Wait long enough that a write would have landed; then assert count is 0.
	time.Sleep(300 * time.Millisecond)
	c, err := h.db.GetSessionObservationCount(sessionID)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if c != 0 {
		t.Errorf("meta-tools should not produce observations; got count=%d", c)
	}
}

// =========================================================================
// Stop contract — client-side gate
// =========================================================================

// TestHookStop_SubprocessE2E_LowMessageSkipsExtractCall pins the client-side
// gate from stop.go: if the transcript has fewer than 3 user messages, Stop
// MUST NOT call /api/sessions/<id>/extract at all (the server gate would
// also skip, but the cheap parse here avoids per-turn HTTP). Asserted by
// checking the server's stderr does NOT show any extraction-attempt log
// line attributable to this session.
func TestHookStop_SubprocessE2E_LowMessageSkipsExtractCall(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-stop-lowmsg-" + time.Now().Format("150405.000000")
	_ = h.runHook(t, "submit", HookInput{
		SessionID: sessionID, CWD: "/tmp", Prompt: "init",
	}).ExpectExit(t, 0)
	testharness.WaitForCondition(t, 2*time.Second, "session ready",
		func() bool { s, _ := h.db.GetSession(sessionID); return s != nil })

	transcriptPath := writeTranscript(t, h.workDir, "low-msg.jsonl", 1,
		"hi", "hello back")

	res := h.runHook(t, "stop", HookInput{
		SessionID:      sessionID,
		TranscriptPath: transcriptPath,
	})
	res.ExpectExit(t, 0)

	// Give async paths time to land — but assert what should be ABSENT.
	time.Sleep(400 * time.Millisecond)
	for _, banned := range []string{
		fmt.Sprintf("extraction: skipping %s", sessionID),
		fmt.Sprintf("extraction failed for %s", sessionID),
		fmt.Sprintf("extraction: failed to mark %s", sessionID),
	} {
		if strings.Contains(h.srv.Stderr(), banned) {
			t.Errorf("Stop client-side gate failed: server log shows %q for low-message transcript", banned)
		}
	}
	sess, _ := h.db.GetSession(sessionID)
	if sess == nil {
		t.Fatal("session disappeared")
	}
	if sess.ExtractedAt != nil {
		t.Errorf("extracted_at must remain NULL on low-message Stop; got %v", *sess.ExtractedAt)
	}
}

// =========================================================================
// SessionEnd contract — PR #4 regression invariant
// =========================================================================

// TestHookEnd_SubprocessE2E_PR4Invariant_LowContentDoesNotMark is the direct
// regression test for the PR #4 bug class. SessionEnd always POSTs /extract
// (belt-and-suspenders, end.go:13-22). With a low-content transcript, the
// server-side gate in engine.extractSession MUST short-circuit BEFORE
// MarkExtracted. If the gate accidentally ran AFTER the mark (the original
// bug), extracted_at would get set despite no extraction happening, and a
// later End with real content would idempotency-skip silently.
//
// Pinned signals:
//   - Server stderr contains "extraction: skipping <id> ... (not marking)".
//   - DB sessions.extracted_at remains NULL.
//   - No "extraction failed for" log (we never reached the LLM path).
func TestHookEnd_SubprocessE2E_PR4Invariant_LowContentDoesNotMark(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-end-pr4-" + time.Now().Format("150405.000000")
	_ = h.runHook(t, "submit", HookInput{
		SessionID: sessionID, CWD: "/tmp", Prompt: "init",
	}).ExpectExit(t, 0)
	testharness.WaitForCondition(t, 2*time.Second, "session ready",
		func() bool { s, _ := h.db.GetSession(sessionID); return s != nil })

	// 1 user message → fewer than 3 → gate skips, must not mark.
	transcriptPath := writeTranscript(t, h.workDir, "low-content.jsonl", 1,
		"hi", "hello back")

	res := h.runHook(t, "end", HookInput{
		SessionID:      sessionID,
		TranscriptPath: transcriptPath,
		Reason:         "test",
	})
	res.ExpectExit(t, 0)

	// Wait for the async server goroutine to finish its skip-and-log path.
	testharness.WaitForCondition(t, 3*time.Second,
		"server should log gate-skip without marking",
		func() bool {
			return strings.Contains(h.srv.Stderr(),
				fmt.Sprintf("extraction: skipping %s", sessionID))
		})

	if !strings.Contains(h.srv.Stderr(), "(not marking)") {
		t.Errorf("server stderr missing '(not marking)' suffix:\n%s", h.srv.Stderr())
	}
	if strings.Contains(h.srv.Stderr(), fmt.Sprintf("extraction failed for %s", sessionID)) {
		t.Errorf("gate did not skip — LLM extraction was attempted")
	}

	// The load-bearing assertion: extracted_at MUST be NULL.
	sess, err := h.db.GetSession(sessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess == nil {
		t.Fatal("session disappeared")
	}
	if sess.ExtractedAt != nil {
		t.Errorf("PR #4 regression: gate-skipped extraction must NOT mark extracted_at; got %v", *sess.ExtractedAt)
	}
}

// TestHookEnd_SubprocessE2E_PastThresholdReachesExtractor pins the positive
// side of the gate: a transcript with >=3 user messages AND >=100 chars
// condensed MUST let extraction proceed to the LLM call. In CI there is no
// LLM, so we see "extraction failed for <id>" — which is the proof we want.
// Without this assertion, a regression that flipped the gate to always-skip
// would pass the PR-4 invariant test silently.
func TestHookEnd_SubprocessE2E_PastThresholdReachesExtractor(t *testing.T) {
	h := setupHookE2E(t)

	sessionID := "test-end-past-" + time.Now().Format("150405.000000")
	_ = h.runHook(t, "submit", HookInput{
		SessionID: sessionID, CWD: "/tmp", Prompt: "init",
	}).ExpectExit(t, 0)
	testharness.WaitForCondition(t, 2*time.Second, "session ready",
		func() bool { s, _ := h.db.GetSession(sessionID); return s != nil })

	// 5 user messages × long content → well past both gate thresholds.
	longUser := strings.Repeat("question about the architecture and design choices ", 3)
	longAsst := strings.Repeat("here is the substantive answer with reasoning and context ", 3)
	transcriptPath := writeTranscript(t, h.workDir, "past.jsonl", 5,
		longUser, longAsst)

	res := h.runHook(t, "end", HookInput{
		SessionID:      sessionID,
		TranscriptPath: transcriptPath,
		Reason:         "test",
	})
	res.ExpectExit(t, 0)

	testharness.WaitForCondition(t, 10*time.Second,
		"past-threshold extraction should reach the LLM call and fail without one",
		func() bool {
			return strings.Contains(h.srv.Stderr(),
				fmt.Sprintf("extraction failed for %s", sessionID))
		})
	// Negative check: must NOT have hit the gate-skip path.
	if strings.Contains(h.srv.Stderr(),
		fmt.Sprintf("extraction: skipping %s", sessionID)) {
		t.Errorf("past-threshold transcript hit the gate-skip path:\n%s", h.srv.Stderr())
	}
}

// =========================================================================
// Full lifecycle — end-to-end pin
// =========================================================================

// TestHookLifecycle_SubprocessE2E_FullSession walks the realistic hook order
// against a single hermetic DB and asserts the cumulative state: session
// row exists, observations are recorded, extraction was attempted (and
// failed gracefully without an LLM). This is the integration check the
// per-handler tests cannot give on their own — it pins that the handlers
// compose correctly across a session boundary.
func TestHookLifecycle_SubprocessE2E_FullSession(t *testing.T) {
	h := setupHookE2E(t)
	sessionID := "test-lifecycle-" + time.Now().Format("150405.000000")

	// SessionStart — agent receives context JSON.
	startRes := h.runHook(t, "start", HookInput{
		SessionID:     sessionID,
		HookEventName: "SessionStart",
	})
	startRes.ExpectExit(t, 0)
	var startOut SessionStartOutput
	if err := json.Unmarshal([]byte(startRes.Stdout), &startOut); err != nil {
		t.Fatalf("SessionStart JSON: %v\n%s", err, startRes.Stdout)
	}

	// 3 user prompts (init + 2 follow-ups).
	for i := 0; i < 3; i++ {
		h.runHook(t, "submit", HookInput{
			SessionID: sessionID,
			CWD:       "/tmp/test-project",
			Prompt:    fmt.Sprintf("question %d about the architecture", i),
		}).ExpectExit(t, 0)
	}

	// 2 tool invocations.
	h.runHook(t, "tool", HookInput{
		SessionID:    sessionID,
		ToolName:     "Write",
		ToolInput:    json.RawMessage(`{"file":"x"}`),
		ToolResponse: json.RawMessage(`{"ok":true}`),
	}).ExpectExit(t, 0)
	h.runHook(t, "tool", HookInput{
		SessionID:    sessionID,
		ToolName:     "Read",
		ToolInput:    json.RawMessage(`{"file":"y"}`),
		ToolResponse: json.RawMessage(`{"ok":true}`),
	}).ExpectExit(t, 0)

	// Past-threshold transcript so Stop's gate passes.
	longUser := strings.Repeat("substantive user content about the system design ", 3)
	longAsst := strings.Repeat("thorough assistant response with code references ", 3)
	transcriptPath := writeTranscript(t, h.workDir, "lifecycle.jsonl", 5,
		longUser, longAsst)

	h.runHook(t, "stop", HookInput{
		SessionID:      sessionID,
		TranscriptPath: transcriptPath,
	}).ExpectExit(t, 0)
	h.runHook(t, "end", HookInput{
		SessionID:      sessionID,
		TranscriptPath: transcriptPath,
		Reason:         "test_complete",
	}).ExpectExit(t, 0)

	// State assertions.
	testharness.WaitForCondition(t, 2*time.Second, "session row present",
		func() bool { s, _ := h.db.GetSession(sessionID); return s != nil })

	testharness.WaitForCondition(t, 2*time.Second, "observations recorded",
		func() bool {
			c, _ := h.db.GetSessionObservationCount(sessionID)
			return c >= 2 // Write + Read; meta-tools would have been skipped if any were here
		})

	// Extraction was attempted at least once (Stop or End both POSTed /extract
	// past the gate; either log line is fine).
	testharness.WaitForCondition(t, 10*time.Second,
		"extraction was attempted",
		func() bool {
			return strings.Contains(h.srv.Stderr(),
				fmt.Sprintf("extraction failed for %s", sessionID))
		})
}
