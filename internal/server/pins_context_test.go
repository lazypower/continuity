package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

// TestPinEndpoint_WorksWithoutEngine is the regression for Codex finding #1: a
// server started without an LLM (nil engine — the supported Ollama-free config)
// must still allow pin/unpin, because pinning is store-native. testServer builds
// New(db, nil, ...), so this exercises exactly that path.
func TestPinEndpoint_WorksWithoutEngine(t *testing.T) {
	srv := testServer(t) // engine is nil
	if srv.engine != nil {
		t.Fatal("precondition: testServer should have a nil engine")
	}

	if err := srv.db.UpsertNode(&store.MemNode{
		URI:        "mem://user/feedback/no-llm-pin",
		NodeType:   "leaf",
		Category:   "feedback",
		L0Abstract: "pin without an LLM",
		L1Overview: "body",
		Relevance:  0.9,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	req := newTestRequest("POST", "/api/memories/pin",
		strings.NewReader(`{"uri":"mem://user/feedback/no-llm-pin"}`))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("pin without engine: status %d (body %s), want 200", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "pinned") {
		t.Errorf("pin response = %s, want status pinned", w.Body.String())
	}

	// And it actually landed.
	got, _ := srv.db.GetNodeByURI("mem://user/feedback/no-llm-pin")
	if got == nil || !got.IsPinned() {
		t.Errorf("node not pinned after 200 OK")
	}
}

// TestBuildContext_PinnedSection verifies a pinned memory rides the cold-boot
// window in its own ### Pinned section and is NOT duplicated in Recent Memories.
func TestBuildContext_PinnedSection(t *testing.T) {
	srv := testServer(t)

	if err := srv.db.UpsertNode(&store.MemNode{
		URI:        "mem://user/feedback/codex-before-pr",
		NodeType:   "leaf",
		Category:   "feedback",
		L0Abstract: "Codex review before every PR",
		L1Overview: "run the build/break/assess loop before opening any PR",
		Relevance:  0.9,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := srv.db.PinNode("mem://user/feedback/codex-before-pr"); err != nil {
		t.Fatalf("PinNode: %v", err)
	}

	ctx := srv.buildContext("")

	if !strings.Contains(ctx, "### Pinned") {
		t.Fatalf("context missing Pinned section:\n%s", ctx)
	}
	pinnedIdx := strings.Index(ctx, "### Pinned")
	if !strings.Contains(ctx[pinnedIdx:], "Codex review before every PR") {
		t.Errorf("pinned memory not rendered in Pinned section")
	}

	// Must not appear twice (once in Pinned, once in Recent Memories / Your Profile).
	if n := strings.Count(ctx, "Codex review before every PR"); n != 1 {
		t.Errorf("pinned memory rendered %d times, want exactly 1 (no duplication across sections)", n)
	}
}

// TestBuildContext_RetractedPinSilent is the injection-side half of the safety
// property: a pinned-then-retracted memory must never reach the context window.
func TestBuildContext_RetractedPinSilent(t *testing.T) {
	srv := testServer(t)

	if err := srv.db.UpsertNode(&store.MemNode{
		URI:        "mem://user/feedback/was-wrong",
		NodeType:   "leaf",
		Category:   "feedback",
		L0Abstract: "PINNED-BUT-RETRACTED-SENTINEL",
		L1Overview: "this guidance turned out to be wrong",
		Relevance:  0.9,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := srv.db.PinNode("mem://user/feedback/was-wrong"); err != nil {
		t.Fatalf("PinNode: %v", err)
	}
	if _, err := srv.db.RetractNode("mem://user/feedback/was-wrong", "guidance was wrong", ""); err != nil {
		t.Fatalf("RetractNode: %v", err)
	}

	ctx := srv.buildContext("")
	if strings.Contains(ctx, "PINNED-BUT-RETRACTED-SENTINEL") {
		t.Errorf("retracted pin leaked into context window:\n%s", ctx)
	}
}
