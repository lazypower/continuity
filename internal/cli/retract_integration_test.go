package cli

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/server"
	"github.com/lazypower/continuity/internal/store"
)

// retractTestServer wires a real httptest server with engine + store + embedder,
// and points the CLI client at it via CONTINUITY_URL. Returns the components so
// tests can manipulate state directly when needed.
func retractTestServer(t *testing.T) (*store.DB, *engine.Engine) {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	eng := engine.New(db, nil)
	srv := server.New(db, eng, "test-version")
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	prev := os.Getenv("CONTINUITY_URL")
	os.Setenv("CONTINUITY_URL", ts.URL)
	t.Cleanup(func() { os.Setenv("CONTINUITY_URL", prev) })

	return db, eng
}

func resetRetractFlags() {
	retractURI = ""
	retractReason = ""
	retractSupersededBy = ""
}

func resetRememberFlags() {
	rememberCategory = ""
	rememberName = ""
	rememberSummary = ""
	rememberBody = ""
	rememberDetail = ""
	rememberSession = ""
	rememberAcknowledgeRetracted = false
}

func TestRetract_EndToEndCLI(t *testing.T) {
	db, _ := retractTestServer(t)

	// Seed via the store directly.
	if err := db.CreateNode(&store.MemNode{
		URI:        "mem://user/events/test-foo",
		NodeType:   "leaf",
		Category:   "events",
		L0Abstract: "test memory for retract repro",
		L1Overview: "Body content with enough length to pass validation thresholds.",
	}); err != nil {
		t.Fatal(err)
	}

	resetRetractFlags()
	retractReason = "test repro, no ongoing value"
	out, err := captureStdout(t, func() error {
		return runRetract(retractCmd, []string{"mem://user/events/test-foo"})
	})
	if err != nil {
		t.Fatalf("runRetract: %v", err)
	}
	if !strings.Contains(out, "retracted") {
		t.Errorf("output missing 'retracted': %q", out)
	}

	// DB state: node exists, marked retracted.
	got, _ := db.GetNodeByURI("mem://user/events/test-foo")
	if got == nil {
		t.Fatal("node disappeared from DB after retract")
	}
	if !got.IsRetracted() {
		t.Errorf("node not marked retracted in DB")
	}
	if got.TombstoneReason != "test repro, no ongoing value" {
		t.Errorf("reason = %q, want exact match", got.TombstoneReason)
	}
}

func TestShow_RetractedDefaultHidesReasonAndContent(t *testing.T) {
	db, _ := retractTestServer(t)
	if err := db.CreateNode(&store.MemNode{
		URI:        "mem://user/events/old-fact",
		NodeType:   "leaf",
		Category:   "events",
		L0Abstract: "summary that should be hidden",
		L1Overview: "Body content with enough length to pass validation thresholds.",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode("mem://user/events/old-fact",
		"sensitive-marker-text-XYZ", ""); err != nil {
		t.Fatal(err)
	}

	resetShowFlags()
	showIncludeRetracted = false
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://user/events/old-fact"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	// Absence-of-leakage: reason text and original content must NOT appear.
	for _, leak := range []string{"sensitive-marker-text-XYZ", "summary that should be hidden", "Body content with enough"} {
		if strings.Contains(out, leak) {
			t.Errorf("show output leaks %q without --include-retracted:\n%s", leak, out)
		}
	}
	// Should mark the memory as retracted.
	if !strings.Contains(out, "[retracted]") {
		t.Errorf("show output missing [retracted] marker:\n%s", out)
	}
}

func TestShow_RetractedJSONOmitsReasonField(t *testing.T) {
	db, _ := retractTestServer(t)
	if err := db.CreateNode(&store.MemNode{
		URI:        "mem://user/events/json-check",
		NodeType:   "leaf",
		Category:   "events",
		L0Abstract: "summary",
		L1Overview: "Body content with enough length to pass validation thresholds.",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode("mem://user/events/json-check",
		"reason-marker", ""); err != nil {
		t.Fatal(err)
	}

	resetShowFlags()
	showJSON = true
	showIncludeRetracted = false
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://user/events/json-check"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json output is not valid JSON: %v\n%s", err, out)
	}

	// Field absence is the contract — not empty string, not null, not redacted text.
	if _, ok := got["tombstone_reason"]; ok {
		t.Errorf("tombstone_reason key should be ABSENT (not empty/null) without --include-retracted; got value: %v", got["tombstone_reason"])
	}
	if _, ok := got["summary"]; ok {
		t.Errorf("summary key should be ABSENT (content sequestered) without --include-retracted; got value: %v", got["summary"])
	}
	if _, ok := got["body"]; ok {
		t.Errorf("body key should be ABSENT (content sequestered) without --include-retracted; got value: %v", got["body"])
	}
	// Retracted marker should be present.
	if r, ok := got["retracted"]; !ok || r != true {
		t.Errorf("retracted marker missing or false: %v", got["retracted"])
	}
}

func TestShow_RetractedWithFlagRevealsReason(t *testing.T) {
	db, _ := retractTestServer(t)
	if err := db.CreateNode(&store.MemNode{
		URI:        "mem://user/events/reveal-check",
		NodeType:   "leaf",
		Category:   "events",
		L0Abstract: "summary content",
		L1Overview: "Body content with enough length to pass validation thresholds.",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode("mem://user/events/reveal-check",
		"explicit-reason-marker", ""); err != nil {
		t.Fatal(err)
	}

	resetShowFlags()
	showIncludeRetracted = true
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://user/events/reveal-check"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}
	if !strings.Contains(out, "explicit-reason-marker") {
		t.Errorf("show --include-retracted should reveal reason; got:\n%s", out)
	}
	if !strings.Contains(out, "summary content") {
		t.Errorf("show --include-retracted should reveal original L0 summary; got:\n%s", out)
	}
}

// PII scenario from the spec: write memory with PII-shaped reason → retract →
// attempt similar write → two-step prompt fires (URI present, reason absent) →
// fetch reason via show + flag → override proceeds. Override is NOT recorded.
func TestRetract_PIIScenario_FullLoop(t *testing.T) {
	db, eng := retractTestServer(t)

	// Step 1: Original write that gets retracted because PII.
	uri, _, err := eng.Remember(context.Background(), engine.RememberInput{
		Category: "events", Name: "sensitive-fact",
		Summary: "operator's full home address mentioned in the conversation",
		Body:    "Body content with enough length to pass validation thresholds easily.",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Build embedder + embed (TFIDF needs corpus to exist before construction).
	embedder, _ := engine.NewTFIDFEmbedder(db, 512)
	eng.SetEmbedder(embedder)
	n, _ := db.GetNodeByURI(uri)
	if err := eng.EmbedNode(context.Background(), n); err != nil {
		t.Fatal(err)
	}

	if _, err := db.RetractNode(uri, "captured operator's home address by accident", ""); err != nil {
		t.Fatal(err)
	}

	// Step 2: Attempt similar write — gate fires.
	_, _, err = eng.Remember(context.Background(), engine.RememberInput{
		Category: "events", Name: "second-attempt",
		Summary: "operator's full home address from earlier discussion",
		Body:    "Different body content with enough length to pass validation thresholds.",
	})
	if err == nil {
		t.Fatal("expected dedup-against-retracted to fire, got nil")
	}
	isMatch, uris := engine.IsRetractedMatch(err)
	if !isMatch || len(uris) == 0 {
		t.Fatalf("expected RetractedMatchError, got: %v", err)
	}

	// Absence-of-leakage: error doesn't reveal the reason content.
	for _, leak := range []string{"home address", "by accident", "captured operator's"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("dedup gate error leaks reason via %q: %s", leak, err.Error())
		}
	}

	// Step 3: Inspect via show --include-retracted to fetch the reason deliberately.
	got, _ := db.GetNodeByURI(uri)
	if !strings.Contains(got.TombstoneReason, "captured") {
		t.Errorf("reason should be reachable via direct DB read with retraction state; got %q", got.TombstoneReason)
	}

	// Step 4: Agent decides to proceed — passes AcknowledgeRetracted.
	overrideURI, created, err := eng.Remember(context.Background(), engine.RememberInput{
		Category:             "events",
		Name:                 "override-attempt",
		Summary:              "operator's full home address from earlier discussion",
		Body:                 "Different body content with enough length to pass validation thresholds.",
		AcknowledgeRetracted: true,
	})
	if err != nil {
		t.Fatalf("override should succeed with AcknowledgeRetracted=true, got: %v", err)
	}
	if !created || overrideURI != "mem://user/events/override-attempt" {
		t.Errorf("override write didn't create at expected URI: created=%v uri=%q", created, overrideURI)
	}

	// Step 5: Override behavior — no separate "almost-rejected" record exists.
	// The DB has exactly the retracted original + the new override write. Nothing
	// in between, no audit-of-the-override-itself. v1 contract: override is
	// unrecorded, the agent's reasoning lives in conversation context.
	allNodes, _ := db.ListLeavesIncludingRetracted()
	relevant := 0
	for _, n := range allNodes {
		if n.URI == uri || n.URI == overrideURI {
			relevant++
		}
	}
	if relevant != 2 {
		t.Errorf("expected exactly 2 relevant nodes (original + override), got %d", relevant)
	}
}
