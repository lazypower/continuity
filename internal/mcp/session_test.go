package mcp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/lazypower/continuity/internal/hooks"
)

// sessionRecorder is a daemon stub that captures the session attribution each
// MCP tool sends: session_id in the remember body, or as a query parameter on
// show and search.
type sessionRecorder struct {
	mu       sync.Mutex
	remember string
	show     string
	search   string
}

func recordingDaemon(t *testing.T) (*hooks.Client, *sessionRecorder) {
	t.Helper()
	rec := &sessionRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/memories", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if r.Method == http.MethodPost {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			rec.remember, _ = body["session_id"].(string)
			json.NewEncoder(w).Encode(map[string]any{"status": "created", "uri": "mem://user/events/test-fact"})
			return
		}
		rec.show = r.URL.Query().Get("session_id")
		json.NewEncoder(w).Encode(map[string]any{
			"uri": "mem://user/events/test-fact", "category": "events", "summary": "s", "body": "b",
		})
	})
	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.search = r.URL.Query().Get("session_id")
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"query": "q", "count": 0, "results": []any{}})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	t.Setenv("CONTINUITY_URL", ts.URL)
	return hooks.NewClient(), rec
}

func callTools(t *testing.T, srv *Server, rememberArgs map[string]any) {
	t.Helper()
	drive(t, srv,
		req(1, "tools/call", map[string]any{"name": "remember", "arguments": rememberArgs}),
		req(2, "tools/call", map[string]any{"name": "show", "arguments": map[string]any{"uri": "mem://user/events/test-fact"}}),
		req(3, "tools/call", map[string]any{"name": "search", "arguments": map[string]any{"query": "anything"}}),
	)
}

var baseRemember = map[string]any{"category": "events", "name": "test-fact", "summary": "s", "body": "b"}

// Every tool that writes a memory or a journal event attributes it to the
// harness session, so agent-authored memories gain project affinity and
// deepened/shown events join to the session that produced them.
func TestHarnessSessionAttributesEveryCall(t *testing.T) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-harness")
	client, rec := recordingDaemon(t)
	callTools(t, NewServer(client, "test"), baseRemember)

	if rec.remember != "sess-harness" || rec.show != "sess-harness" || rec.search != "sess-harness" {
		t.Fatalf("want harness session on every call, got remember=%q show=%q search=%q",
			rec.remember, rec.show, rec.search)
	}
}

// Inside a harness, its session id wins over an agent-supplied session_id: the
// agent's value may be stale or invented, and a wrong one strips affinity.
func TestHarnessSessionOverridesArgument(t *testing.T) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-harness")
	client, rec := recordingDaemon(t)
	args := map[string]any{"session_id": "sess-explicit"}
	for k, v := range baseRemember {
		args[k] = v
	}
	callTools(t, NewServer(client, "test"), args)

	if rec.remember != "sess-harness" {
		t.Fatalf("remember session = %q, want sess-harness", rec.remember)
	}
}

// Outside a harness the argument is the only source, and it is used.
func TestArgumentUsedOutsideHarness(t *testing.T) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	client, rec := recordingDaemon(t)
	args := map[string]any{"session_id": "sess-explicit"}
	for k, v := range baseRemember {
		args[k] = v
	}
	callTools(t, NewServer(client, "test"), args)

	if rec.remember != "sess-explicit" {
		t.Fatalf("remember session = %q, want sess-explicit", rec.remember)
	}
}

// Outside a harness nothing is invented: the calls carry no session, exactly
// as before.
func TestNoHarnessSendsNoSession(t *testing.T) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	client, rec := recordingDaemon(t)
	callTools(t, NewServer(client, "test"), baseRemember)

	if rec.remember != "" || rec.show != "" || rec.search != "" {
		t.Fatalf("want no session outside a harness, got remember=%q show=%q search=%q",
			rec.remember, rec.show, rec.search)
	}
}
