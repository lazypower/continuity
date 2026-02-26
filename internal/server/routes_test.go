package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
