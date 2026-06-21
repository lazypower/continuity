package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

// TestMetricsEndpoint verifies the /api/metrics route is wired through the
// router and returns the Memory Health payload as JSON.
func TestMetricsEndpoint(t *testing.T) {
	srv := testServer(t)

	for i, uri := range []string{
		"mem://user/patterns/a",
		"mem://user/cases/b",
		"mem://user/cases/c",
	} {
		if err := srv.db.CreateNode(&store.MemNode{
			URI: uri, NodeType: "leaf", Category: []string{"patterns", "cases", "cases"}[i],
			L0Abstract: "x",
		}); err != nil {
			t.Fatalf("create %s: %v", uri, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	req.Host = "127.0.0.1"
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}

	var m store.Metrics
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body.String())
	}
	if m.Summary.ActiveTotal != 3 {
		t.Errorf("active_total = %d, want 3", m.Summary.ActiveTotal)
	}
	if len(m.Categories) != 2 {
		t.Errorf("categories = %d, want 2 (patterns, cases)", len(m.Categories))
	}
	// cases (2) should sort before patterns (1).
	if m.Categories[0].Category != "cases" {
		t.Errorf("categories[0] = %q, want cases (highest count first)", m.Categories[0].Category)
	}
}
