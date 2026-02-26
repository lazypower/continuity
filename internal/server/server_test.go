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
	return New(db, "test-version")
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

	stubs := []struct {
		method string
		path   string
	}{
		{"GET", "/api/search?q=test"},
		{"GET", "/api/profile"},
		{"GET", "/api/tree"},
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
