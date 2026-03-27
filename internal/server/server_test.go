package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return New(db, nil, "test-version")
}

func TestBuildContextBudget(t *testing.T) {
	srv := testServer(t)

	// Seed a relational profile that exceeds the output budget
	longProfile := strings.Repeat("The user communicates tersely. ", 100) // ~3000 chars
	err := srv.db.UpsertNode(&store.MemNode{
		URI:        "mem://user/profile/communication",
		NodeType:   "leaf",
		Category:   "profile",
		L0Abstract: "Relational profile",
		L1Overview: longProfile,
		L2Content:  longProfile,
	})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}

	ctx := srv.buildContext("")
	if len(ctx) > maxContextTotal+500 { // allow some slack for footer/tags
		t.Errorf("context too large: %d chars, budget is %d", len(ctx), maxContextTotal)
	}
	if !strings.Contains(ctx, "Working With You") {
		t.Error("context missing relational profile section")
	}
}

func TestBuildContextItemBudget(t *testing.T) {
	srv := testServer(t)

	// Seed 20 memory nodes with L0s at exactly 150 chars each
	for i := 0; i < 20; i++ {
		l0 := fmt.Sprintf("Memory item %02d: %s", i, strings.Repeat("x", 130))
		if len(l0) > 200 {
			l0 = l0[:150]
		}
		err := srv.db.UpsertNode(&store.MemNode{
			URI:        fmt.Sprintf("mem://agent/patterns/item-%02d", i),
			NodeType:   "leaf",
			Category:   "patterns",
			L0Abstract: l0,
			L1Overview: "overview content that is long enough",
			L2Content:  "full content",
			Relevance:  0.9,
		})
		if err != nil {
			t.Fatalf("upsert node %d: %v", i, err)
		}
	}

	ctx := srv.buildContext("")
	if len(ctx) > maxContextTotal+500 {
		t.Errorf("context too large with 20 items: %d chars, budget is %d", len(ctx), maxContextTotal)
	}
}

func TestBuildContextOversizedL0Truncated(t *testing.T) {
	srv := testServer(t)

	// Seed a node with an L0 that exceeds per-item budget (200 chars)
	bigL0 := strings.Repeat("This memory is way too long for an L0 abstract. ", 10) // ~480 chars
	err := srv.db.UpsertNode(&store.MemNode{
		URI:        "mem://agent/patterns/bloated",
		NodeType:   "leaf",
		Category:   "patterns",
		L0Abstract: bigL0,
		L1Overview: "overview content that is long enough",
		L2Content:  "full content",
		Relevance:  0.9,
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	ctx := srv.buildContext("")
	// The item should appear but truncated
	if !strings.Contains(ctx, "Recent Memories") {
		t.Error("context missing memories section")
	}
	// The full bloated L0 should NOT appear verbatim
	if strings.Contains(ctx, bigL0) {
		t.Error("oversized L0 was not truncated in context output")
	}
}

func TestTruncateAtSentence(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		maxLen int
		want   string
	}{
		{"short enough", "Hello world.", 50, "Hello world."},
		{"sentence boundary", "First sentence. Second sentence. Third sentence.", 35, "First sentence. Second sentence."},
		{"word boundary", "One two three four five six", 15, "One two three"},
		{"no good boundary", strings.Repeat("x", 300), 200, strings.Repeat("x", 200)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateAtSentence(tt.input, tt.maxLen)
			if len(got) > tt.maxLen {
				t.Errorf("result too long: %d > %d", len(got), tt.maxLen)
			}
			if tt.want != "" && got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv := testServer(t)

	req := httptest.NewRequest("GET", "/api/health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if body["status"] != "ok" {
		t.Errorf("status = %v, want ok", body["status"])
	}
	if body["version"] != "test-version" {
		t.Errorf("version = %v, want test-version", body["version"])
	}
	if body["db"] != true {
		t.Errorf("db = %v, want true", body["db"])
	}
}

func TestStubRoutes(t *testing.T) {
	srv := testServer(t)

	// These routes are still stubs (501)
	stubs := []struct {
		method string
		path   string
	}{
		{"GET", "/api/sessions"},
		{"GET", "/api/sessions/abc123"},
	}

	for _, s := range stubs {
		req := httptest.NewRequest(s.method, s.path, nil)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)

		if w.Code != http.StatusNotImplemented {
			t.Errorf("%s %s: status = %d, want %d", s.method, s.path, w.Code, http.StatusNotImplemented)
		}

		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Errorf("%s %s: decode body: %v", s.method, s.path, err)
			continue
		}
		if body["error"] == "" {
			t.Errorf("%s %s: expected error message in body", s.method, s.path)
		}
	}
}

func TestSearchRoute(t *testing.T) {
	srv := testServer(t)

	// Search without embedder returns 503
	req := httptest.NewRequest("GET", "/api/search?q=test", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("search without embedder: status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	// Search without q param returns 400
	req = httptest.NewRequest("GET", "/api/search", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("search without q: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestProfileRoute(t *testing.T) {
	srv := testServer(t)

	req := httptest.NewRequest("GET", "/api/profile", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("profile: status = %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, ok := body["relational_profile"]; !ok {
		t.Error("expected relational_profile in response")
	}
}

func TestTreeRoute(t *testing.T) {
	srv := testServer(t)

	req := httptest.NewRequest("GET", "/api/tree", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("tree: status = %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, ok := body["nodes"]; !ok {
		t.Error("expected nodes in response")
	}
}
