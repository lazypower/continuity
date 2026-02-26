package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		{"POST", "/api/memories"},
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
