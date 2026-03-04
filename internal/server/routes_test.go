package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

func TestSessionInit(t *testing.T) {
	srv := testServer(t)

	body := `{"session_id":"test-001","project":"/tmp/myproject"}`
	req := httptest.NewRequest("POST", "/api/sessions/init", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["session_id"] != "test-001" {
		t.Errorf("session_id = %v, want test-001", resp["session_id"])
	}
	if resp["status"] != "active" {
		t.Errorf("status = %v, want active", resp["status"])
	}
}

func TestSessionInitMissingID(t *testing.T) {
	srv := testServer(t)

	body := `{"project":"/tmp/myproject"}`
	req := httptest.NewRequest("POST", "/api/sessions/init", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestAddObservation(t *testing.T) {
	srv := testServer(t)

	// Init session first
	initBody := `{"session_id":"test-001","project":"/tmp/myproject"}`
	req := httptest.NewRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Add observation
	obsBody := `{"tool_name":"Bash","tool_input":"{\"command\":\"ls\"}","tool_response":"file1 file2"}`
	req = httptest.NewRequest("POST", "/api/sessions/test-001/observations", strings.NewReader(obsBody))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusCreated, w.Body.String())
	}
}

func TestCompleteSession(t *testing.T) {
	srv := testServer(t)

	// Init session
	initBody := `{"session_id":"test-001","project":"/tmp/myproject"}`
	req := httptest.NewRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Complete session
	req = httptest.NewRequest("POST", "/api/sessions/test-001/complete", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "completed" {
		t.Errorf("status = %v, want completed", resp["status"])
	}
}

func TestEndSession(t *testing.T) {
	srv := testServer(t)

	// Init session
	initBody := `{"session_id":"test-001","project":"/tmp/myproject"}`
	req := httptest.NewRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// End session
	req = httptest.NewRequest("POST", "/api/sessions/test-001/end", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ended" {
		t.Errorf("status = %v, want ended", resp["status"])
	}
}

func TestSignalRouteNoEngine(t *testing.T) {
	srv := testServer(t) // engine is nil

	body := `{"prompt":"remember this: always use WAL mode"}`
	req := httptest.NewRequest("POST", "/api/sessions/test-001/signal", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("signal without engine: status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestSignalRouteMissingPrompt(t *testing.T) {
	srv := testServer(t)

	body := `{}`
	req := httptest.NewRequest("POST", "/api/sessions/test-001/signal", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("signal without prompt: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestSignalRouteInvalidJSON(t *testing.T) {
	srv := testServer(t)

	req := httptest.NewRequest("POST", "/api/sessions/test-001/signal", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("signal with bad json: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestGetContext(t *testing.T) {
	srv := testServer(t)

	// Empty context
	req := httptest.NewRequest("GET", "/api/context", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["context"] == "" {
		t.Error("expected non-empty context")
	}
	if !strings.Contains(resp["context"], "Continuity") {
		t.Errorf("context missing 'Continuity' header: %s", resp["context"])
	}
}

func testServerWithEngine(t *testing.T) *Server {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	eng := engine.New(db, nil)
	return New(db, eng, "test-version")
}

func TestRememberRoute(t *testing.T) {
	srv := testServerWithEngine(t)

	body := `{"category":"preferences","name":"devbox","summary":"Always use devbox","body":"The project uses devbox shell to provide Go and SQLite tools."}`
	req := httptest.NewRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "created" {
		t.Errorf("status = %q, want created", resp["status"])
	}
	if resp["uri"] != "mem://user/preferences/devbox" {
		t.Errorf("uri = %q, want mem://user/preferences/devbox", resp["uri"])
	}
}

func TestRememberRouteUpdate(t *testing.T) {
	srv := testServerWithEngine(t)

	body := `{"category":"preferences","name":"devbox","summary":"Always use devbox","body":"The project uses devbox shell to provide Go and SQLite tools."}`

	// First call → created
	req := httptest.NewRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("first: status = %d, want %d", w.Code, http.StatusCreated)
	}

	// Second call with different content → updated
	body2 := `{"category":"preferences","name":"devbox","summary":"Updated devbox preference","body":"Updated: devbox shell provides Go, SQLite, and additional tooling."}`
	req = httptest.NewRequest("POST", "/api/memories", strings.NewReader(body2))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("second: status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "updated" {
		t.Errorf("status = %q, want updated", resp["status"])
	}
}

func TestRememberRouteNoEngine(t *testing.T) {
	srv := testServer(t) // engine is nil

	body := `{"category":"preferences","name":"test","summary":"test","body":"test body with enough content for validation."}`
	req := httptest.NewRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestRememberRouteMissingFields(t *testing.T) {
	srv := testServerWithEngine(t)

	tests := []struct {
		name string
		body string
	}{
		{"missing category", `{"name":"test","summary":"test","body":"test body"}`},
		{"missing name", `{"category":"preferences","summary":"test","body":"test body"}`},
		{"missing summary", `{"category":"preferences","name":"test","body":"test body"}`},
		{"missing body", `{"category":"preferences","name":"test","summary":"test"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/memories", strings.NewReader(tt.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestRememberRouteInvalidJSON(t *testing.T) {
	srv := testServerWithEngine(t)

	req := httptest.NewRequest("POST", "/api/memories", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestGetContextWithSessions(t *testing.T) {
	srv := testServer(t)

	// Create a completed session with observations
	initBody := `{"session_id":"old-001","project":"/tmp/myproject"}`
	req := httptest.NewRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	obsBody := `{"tool_name":"Bash","tool_input":"{}","tool_response":"ok"}`
	req = httptest.NewRequest("POST", "/api/sessions/old-001/observations", strings.NewReader(obsBody))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	req = httptest.NewRequest("POST", "/api/sessions/old-001/complete", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Get context for a new session
	req = httptest.NewRequest("GET", "/api/context?session_id=new-001", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)

	if !strings.Contains(resp["context"], "Recent Sessions") {
		t.Errorf("context missing 'Recent Sessions': %s", resp["context"])
	}
	if !strings.Contains(resp["context"], "myproject") {
		t.Errorf("context missing project name: %s", resp["context"])
	}
}
