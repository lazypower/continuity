//go:build !windows

package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/store"
	"github.com/lazypower/continuity/internal/testharness"
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

	bin := testharness.BuildContinuityBinary(t)

	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "test.db")

	// Seed the DB with enough varied text that TFIDF builds a real vocabulary
	// covering the tokens our test queries use (operator, home, address,
	// discussion, captured, accident). Without this, NewTFIDFEmbedder produces
	// a 1-dim vector and every cosine similarity collapses to 0, so the
	// dedup-against-retracted gate cannot fire and the test would silently
	// fail to exercise its target invariant.
	seedTFIDFCorpus(t, dbPath)

	serverURL, procEnv := testharness.HermeticEnv(t, workDir, dbPath, 0)

	srv := testharness.StartServeProcess(t, bin, procEnv)
	t.Cleanup(srv.Stop)
	testharness.WaitForReady(t, serverURL+"/api/health")

	// Step 1 — remember original.
	step1 := testharness.RunCLI(t, bin, procEnv, "remember",
		"-c", "events",
		"-n", "operator-home-address-discussion",
		"-s", "operator's full home address discussion",
		"-b", "Body content captured during conversation that has enough length to pass validation thresholds easily.",
	)
	step1.ExpectExit(t, 0).
		ExpectStdoutContains(t, "created:", "mem://user/events/operator-home-address-discussion")

	// Step 2 — retract the original. Reason carries the PII-shaped marker; we
	// will assert this string never leaks via the gate's stderr or via the
	// default show output.
	const piiReason = "captured operator home address by accident; remove on sight"
	step2 := testharness.RunCLI(t, bin, procEnv, "retract",
		"mem://user/events/operator-home-address-discussion",
		"--reason", piiReason,
	)
	step2.ExpectExit(t, 0).
		ExpectStdoutContains(t, "retracted:", "mem://user/events/operator-home-address-discussion")

	// Step 3 — write a semantically similar memory under the same category.
	// The dedup-against-retracted gate must fire: exit 2, stderr names the
	// matched URI, stderr DOES NOT leak the reason. This is the agent-facing
	// contract from PR #20's design.
	step3 := testharness.RunCLI(t, bin, procEnv, "remember",
		"-c", "events",
		"-n", "second-attempt-similar",
		"-s", "operator home address mentioned again in same discussion thread",
		"-b", "Different body content carrying enough length to pass validation easily as well.",
	)
	step3.ExpectExit(t, 2).
		ExpectStderrContains(t, "matches_retracted", "mem://user/events/operator-home-address-discussion").
		ExpectStdoutAbsent(t, "created:", "updated:") // stdout MUST stay clean on the gate path

	// Absence-of-leakage: the reason field is sequestered by contract. Verify
	// no fragment of the reason text appears in stderr.
	for _, leak := range []string{
		"captured operator",
		"home address by accident",
		"remove on sight",
		piiReason,
	} {
		if strings.Contains(step3.Stderr, leak) {
			t.Errorf("step 3 stderr leaks tombstone reason via %q:\n%s", leak, step3.Stderr)
		}
	}

	// Step 4 — show with --include-retracted reveals the reason deliberately.
	step4 := testharness.RunCLI(t, bin, procEnv, "show",
		"mem://user/events/operator-home-address-discussion",
		"--include-retracted",
	)
	step4.ExpectExit(t, 0).
		ExpectStdoutContains(t, piiReason)

	// Step 5 — show WITHOUT the flag suppresses the reason and the body.
	step5 := testharness.RunCLI(t, bin, procEnv, "show",
		"mem://user/events/operator-home-address-discussion",
	)
	step5.ExpectExit(t, 0)
	for _, leak := range []string{
		"captured operator",
		"home address by accident",
		"remove on sight",
		piiReason,
	} {
		if strings.Contains(step5.Stdout, leak) {
			t.Errorf("step 5 (show without flag) leaks reason via %q:\n%s", leak, step5.Stdout)
		}
	}
	if !strings.Contains(step5.Stdout, "[retracted]") {
		t.Errorf("step 5 must mark the node as retracted in output:\n%s", step5.Stdout)
	}

	// Step 6 — show --json without the flag omits reason/summary/body fields.
	step6 := testharness.RunCLI(t, bin, procEnv, "show",
		"mem://user/events/operator-home-address-discussion",
		"--json",
	)
	step6.ExpectExit(t, 0)
	var jsonOut map[string]any
	if err := json.Unmarshal([]byte(step6.Stdout), &jsonOut); err != nil {
		t.Fatalf("step 6 stdout is not valid JSON: %v\n%s", err, step6.Stdout)
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
	step7 := testharness.RunCLI(t, bin, procEnv, "remember",
		"-c", "events",
		"-n", "second-attempt-similar",
		"-s", "operator home address mentioned again in same discussion thread",
		"-b", "Different body content carrying enough length to pass validation easily as well.",
		"--acknowledge-retracted",
	)
	step7.ExpectExit(t, 0).
		ExpectStdoutContains(t, "created:", "mem://user/events/second-attempt-similar").
		ExpectStderrAbsent(t, "matches_retracted")

	// Step 8 — show the override write proves the override landed at the
	// expected URI (i.e. the gate-bypass path doesn't sneak the write onto a
	// timestamp-suffixed slug).
	step8 := testharness.RunCLI(t, bin, procEnv, "show", "mem://user/events/second-attempt-similar")
	step8.ExpectExit(t, 0).
		ExpectStdoutContains(t, "mem://user/events/second-attempt-similar")
}

// seedTFIDFCorpus opens the SQLite DB directly and writes enough varied L0
// abstracts that NewTFIDFEmbedder has a real vocabulary covering the tokens
// the retract test queries use. Seeding here (not via the CLI) is intentional:
// it represents the production state where the user already has a populated DB
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
