package server

import (
	"context"
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
	req := newTestRequest("POST", "/api/sessions/init", strings.NewReader(body))
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
	req := newTestRequest("POST", "/api/sessions/init", strings.NewReader(body))
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
	req := newTestRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Add observation
	obsBody := `{"tool_name":"Bash","tool_input":"{\"command\":\"ls\"}","tool_response":"file1 file2"}`
	req = newTestRequest("POST", "/api/sessions/test-001/observations", strings.NewReader(obsBody))
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
	req := newTestRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Complete session
	req = newTestRequest("POST", "/api/sessions/test-001/complete", nil)
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
	req := newTestRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// End session
	req = newTestRequest("POST", "/api/sessions/test-001/end", nil)
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
	req := newTestRequest("POST", "/api/sessions/test-001/signal", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("signal without engine: status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestSignalRouteMissingPrompt(t *testing.T) {
	srv := testServer(t)

	body := `{}`
	req := newTestRequest("POST", "/api/sessions/test-001/signal", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("signal without prompt: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestSignalRouteInvalidJSON(t *testing.T) {
	srv := testServer(t)

	req := newTestRequest("POST", "/api/sessions/test-001/signal", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("signal with bad json: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestGetContext(t *testing.T) {
	srv := testServer(t)

	// Empty context
	req := newTestRequest("GET", "/api/context", nil)
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

// TestRememberRouteInvalidCategorySurfacesReason is the regression for issue
// #35: the remember handler used to collapse genuine validation errors into
// the generic "failed to store memory" string, hiding the actionable reason
// (e.g. an unknown category) server-side only. The reason must now reach the
// client verbatim with a 400.
func TestRememberRouteInvalidCategorySurfacesReason(t *testing.T) {
	srv := testServerWithEngine(t)

	body := `{"category":"feeback","name":"x","summary":"typo'd category","body":"body content long enough to clear the minimum length validation bar."}`
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusBadRequest, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	got := resp["error"]
	if got == "failed to store memory" {
		t.Fatalf("client got the generic message, want the real validation reason; body: %s", w.Body.String())
	}
	if !strings.Contains(got, "invalid category") || !strings.Contains(got, "feeback") {
		t.Errorf("error = %q, want it to name the bad category", got)
	}
}

// TestRememberRouteRetractedMatchStillSequestered verifies the issue #35 change
// did not disturb the retracted-match 409 path: a write that semantically
// collides with a retracted memory must still get a 409 with matched URIs and
// NO retraction reasons, not a 400 validation message.
func TestRememberRouteRetractedMatchStillSequestered(t *testing.T) {
	srv := testServerWithEngine(t)

	// Mirror the engine-level regression ordering (retract_test.go): seed, then
	// build the TFIDF embedder over the corpus, embed the node so the vector
	// space has signal, and only then retract. This makes the
	// dedup-against-retracted gate fire deterministically.
	ctx := context.Background()
	const l0 = "operator's mother's maiden name discussed in context"
	uri, _, err := srv.engine.Remember(ctx, engine.RememberInput{
		Category: "events", Name: "old-pii-event",
		Summary: l0,
		Body:    "Memory body content with more than enough length to pass validation thresholds.",
		AcknowledgeRetracted: true,
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	embedder, err := engine.NewTFIDFEmbedder(srv.db, 512)
	if err != nil {
		t.Fatalf("embedder: %v", err)
	}
	srv.engine.SetEmbedder(embedder)
	n, _ := srv.db.GetNodeByURI(uri)
	if err := srv.engine.EmbedNode(ctx, n); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if _, err := srv.db.RetractNode(uri, "PII captured accidentally", ""); err != nil {
		t.Fatalf("retract: %v", err)
	}

	body := `{"category":"events","name":"new-event","summary":"operator's mother's maiden name from earlier discussion","body":"Different body content with more than enough length to pass validation thresholds."}`
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d (409 retracted-match); body: %s", w.Code, http.StatusConflict, w.Body.String())
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "matches_retracted" {
		t.Errorf("status = %v, want matches_retracted", resp["status"])
	}
	if _, ok := resp["matched_uris"]; !ok {
		t.Errorf("response missing matched_uris: %s", w.Body.String())
	}
	// The retraction reason must stay sequestered — it must not appear anywhere.
	if strings.Contains(w.Body.String(), "PII") {
		t.Errorf("retraction reason leaked into 409 response: %s", w.Body.String())
	}
}

// TestRetractRouteInvalidURISurfacesReason confirms the same classification was
// applied to the other write handler: a malformed URI is user input and its
// reason must reach the client, not be collapsed to "failed to retract memory".
func TestRetractRouteInvalidURISurfacesReason(t *testing.T) {
	srv := testServerWithEngine(t)

	body := `{"uri":"not-a-mem-uri","reason":"cleanup"}`
	req := newTestRequest("POST", "/api/memories/retract", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusBadRequest, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	got := resp["error"]
	if got == "failed to retract memory" {
		t.Fatalf("client got the generic message, want the real validation reason; body: %s", w.Body.String())
	}
	if !strings.Contains(got, "must start with mem://") {
		t.Errorf("error = %q, want it to explain the mem:// requirement", got)
	}
}

// TestRememberRouteInternalErrorStaysGeneric documents that a non-validation
// failure (here: a direct URI collision with a retracted tombstone is a
// validation error, but a true internal error path) is NOT surfaced verbatim.
// We exercise the closest practical internal-ish path: a retracted-slug
// collision is classified as validation (safe to surface), while everything
// the engine does not tag as ValidationError keeps the generic message. This
// asserts the negative: the generic message is reachable and unchanged.
func TestRememberRouteValidationVsGenericClassification(t *testing.T) {
	// Sanity-check the classifier contract the handler relies on: a plain
	// (non-ValidationError) error must NOT be reported as a validation error,
	// so the handler falls through to the generic message + server log.
	if isVal, _ := engine.IsValidationError(context.DeadlineExceeded); isVal {
		t.Errorf("IsValidationError mis-classified a generic error as validation")
	}
	verr := &engine.ValidationError{Message: "invalid category \"feeback\""}
	if isVal, msg := engine.IsValidationError(verr); !isVal || msg != verr.Message {
		t.Errorf("IsValidationError(%v) = (%v, %q), want (true, %q)", verr, isVal, msg, verr.Message)
	}
}

// TestRememberRouteInternalErrorStaysGenericOverHTTP drives the actual handler
// with a genuine internal (non-validation) failure: closing the DB before an
// otherwise-valid POST forces a "check existing" lookup error deep in
// Engine.Remember, which is NOT a ValidationError. The client must see exactly
// the generic "failed to store memory" and none of the underlying error text —
// no DB/SQL/path leak. This is the positive complement to the classifier unit
// test, exercising the real HTTP path the generic branch guards.
func TestRememberRouteInternalErrorStaysGenericOverHTTP(t *testing.T) {
	srv := testServerWithEngine(t)

	// Force every subsequent store call to fail with an internal error.
	srv.db.Close()

	body := `{"category":"preferences","name":"devbox","summary":"Always use devbox","body":"The project uses devbox shell to provide Go and SQLite tools."}`
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusBadRequest, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["error"] != "failed to store memory" {
		t.Fatalf("error = %q, want exactly %q (generic, no internal detail)", resp["error"], "failed to store memory")
	}
	// Belt-and-suspenders: no internal error vocabulary may leak into the body.
	for _, leak := range []string{"check existing", "sql", "SQL", "database is closed", "upsert"} {
		if strings.Contains(w.Body.String(), leak) {
			t.Errorf("internal detail %q leaked into client response: %s", leak, w.Body.String())
		}
	}
}

// TestRetractRouteStoreDomainRejectionSurfacesReason is the issue #35 follow-up:
// store-level domain rejections (here, retracting a URI that does not exist) used
// to fall through Engine.Retract unclassified and collapse into the generic
// "failed to retract memory". They must now be surfaced as 400 with their real,
// client-safe reason.
func TestRetractRouteStoreDomainRejectionSurfacesReason(t *testing.T) {
	srv := testServerWithEngine(t)

	// Well-formed mem:// URI (passes the engine-level prefix check) that points at
	// a memory that does not exist — a store-level domain rejection.
	body := `{"uri":"mem://user/events/does-not-exist","reason":"cleanup"}`
	req := newTestRequest("POST", "/api/memories/retract", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusBadRequest, w.Body.String())
	}

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	got := resp["error"]
	if got == "failed to retract memory" {
		t.Fatalf("client got the generic message, want the real store-level reason; body: %s", w.Body.String())
	}
	if !strings.Contains(got, "not found") {
		t.Errorf("error = %q, want it to explain the memory was not found", got)
	}
}

func TestRememberRoute(t *testing.T) {
	srv := testServerWithEngine(t)

	body := `{"category":"preferences","name":"devbox","summary":"Always use devbox","body":"The project uses devbox shell to provide Go and SQLite tools."}`
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
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

// TestRememberRouteFeedbackAndReference confirms the POST /api/memories
// endpoint accepts the categories added in issue #24. Without an end-to-end
// API test, a regression that breaks one of these categories in extraction
// or validation could land silently — the CLI would surface it but only
// after a release.
func TestRememberRouteFeedbackAndReference(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantURI string
	}{
		{
			name:    "feedback",
			body:    `{"category":"feedback","name":"terse-summaries","summary":"User wants terse responses with no trailing summaries.","body":"Rule: terse responses, no trailing summaries. Why: diff carries the info. How to apply: never recap."}`,
			wantURI: "mem://user/feedback/terse-summaries",
		},
		{
			name:    "reference",
			body:    `{"category":"reference","name":"linear-ingest","summary":"Pipeline bugs tracked in Linear project INGEST.","body":"Linear project INGEST is the canonical home for pipeline bug reports — file there, do not use GitHub Issues."}`,
			wantURI: "mem://user/reference/linear-ingest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := testServerWithEngine(t)

			req := newTestRequest("POST", "/api/memories", strings.NewReader(tt.body))
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
			if resp["uri"] != tt.wantURI {
				t.Errorf("uri = %q, want %q", resp["uri"], tt.wantURI)
			}
		})
	}
}

func TestRememberRouteUpdate(t *testing.T) {
	srv := testServerWithEngine(t)

	body := `{"category":"preferences","name":"devbox","summary":"Always use devbox","body":"The project uses devbox shell to provide Go and SQLite tools."}`

	// First call → created
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("first: status = %d, want %d", w.Code, http.StatusCreated)
	}

	// Second call with different content → updated
	body2 := `{"category":"preferences","name":"devbox","summary":"Updated devbox preference","body":"Updated: devbox shell provides Go, SQLite, and additional tooling."}`
	req = newTestRequest("POST", "/api/memories", strings.NewReader(body2))
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

func TestGetMemoryRoute(t *testing.T) {
	srv := testServerWithEngine(t)

	// Seed a memory via POST
	body := `{"category":"patterns","name":"test-journal","summary":"tiny test","body":"section A\n- entry 1\n\nsection B\n- entry 2\n"}`
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("seed: status = %d, want %d", w.Code, http.StatusCreated)
	}

	// Read it back
	req = newTestRequest("GET", "/api/memories?uri=mem://agent/patterns/test-journal", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not valid JSON: %v\n%s", err, w.Body.String())
	}
	if resp["uri"] != "mem://agent/patterns/test-journal" {
		t.Errorf("uri = %v, want mem://agent/patterns/test-journal", resp["uri"])
	}
	if resp["summary"] != "tiny test" {
		t.Errorf("summary = %v, want tiny test", resp["summary"])
	}
	gotBody, _ := resp["body"].(string)
	if !strings.Contains(gotBody, "section A") || !strings.Contains(gotBody, "section B") {
		t.Errorf("body did not preserve both sections: %q", gotBody)
	}
	if resp["category"] != "patterns" {
		t.Errorf("category = %v, want patterns", resp["category"])
	}
}

func TestGetMemoryRouteNotFound(t *testing.T) {
	srv := testServerWithEngine(t)

	req := newTestRequest("GET", "/api/memories?uri=mem://agent/patterns/does-not-exist", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestGetMemoryRouteMissingURI(t *testing.T) {
	srv := testServerWithEngine(t)

	req := newTestRequest("GET", "/api/memories", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestRememberRouteNoEngine(t *testing.T) {
	srv := testServer(t) // engine is nil

	body := `{"category":"preferences","name":"test","summary":"test","body":"test body with enough content for validation."}`
	req := newTestRequest("POST", "/api/memories", strings.NewReader(body))
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
			req := newTestRequest("POST", "/api/memories", strings.NewReader(tt.body))
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

	req := newTestRequest("POST", "/api/memories", strings.NewReader("not json"))
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
	req := newTestRequest("POST", "/api/sessions/init", strings.NewReader(initBody))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	obsBody := `{"tool_name":"Bash","tool_input":"{}","tool_response":"ok"}`
	req = newTestRequest("POST", "/api/sessions/old-001/observations", strings.NewReader(obsBody))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	req = newTestRequest("POST", "/api/sessions/old-001/complete", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Get context for a new session
	req = newTestRequest("GET", "/api/context?session_id=new-001", nil)
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

func TestUnmarkEmptyExtractionsRoute(t *testing.T) {
	srv := testServerWithEngine(t)

	// Seed: one session marked extracted with no memories (should unmark)
	// and one marked-with-memory (should stay)
	srv.db.InitSession("empty-sess", "proj")
	srv.db.MarkExtracted("empty-sess")
	srv.db.InitSession("real-sess", "proj")
	srv.db.MarkExtracted("real-sess")
	if err := srv.db.UpsertNode(&store.MemNode{
		URI:           "mem://user/preferences/x",
		NodeType:      "leaf",
		Category:      "preferences",
		SourceSession: "real-sess",
	}); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}

	req := newTestRequest("POST", "/api/sessions/unmark-empty-extractions", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Status   string `json:"status"`
		Unmarked int64  `json:"unmarked"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Unmarked != 1 {
		t.Errorf("unmarked = %d, want 1", resp.Unmarked)
	}

	empty, _ := srv.db.GetSession("empty-sess")
	if empty.ExtractedAt != nil {
		t.Error("empty-sess should have been unmarked")
	}
	kept, _ := srv.db.GetSession("real-sess")
	if kept.ExtractedAt == nil {
		t.Error("real-sess should have stayed marked")
	}
}

// TestExtractSessionRouteAcceptsForce verifies the extract endpoint accepts
// and honors the force flag. Real extraction runs async so we only assert
// the request is accepted cleanly.
func TestExtractSessionRouteAcceptsForce(t *testing.T) {
	srv := testServerWithEngine(t)
	srv.db.InitSession("extract-001", "proj")

	body := `{"transcript_path":"/nonexistent/transcript.jsonl","force":true}`
	req := newTestRequest("POST", "/api/sessions/extract-001/extract", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", w.Code, w.Body.String())
	}
}
