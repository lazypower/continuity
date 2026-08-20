package hooks

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The gate fails closed as a contract (ADR-001 §4, #80): every degraded state
// resolves to silence — no stdout, no error, no delay beyond the gate budget.
// These tests pin each failure mode at the hook boundary.

func gateClientFor(url string) *Client {
	return &Client{
		http:      &http.Client{Timeout: gateTimeout},
		serverURL: url,
	}
}

func gateInput(prompt string) *HookInput {
	return &HookInput{SessionID: "sess-1", CWD: "/tmp/nowhere", Prompt: prompt}
}

// Server stopped: connection refused resolves to silence.
func TestPromptGate_ServerDownIsSilent(t *testing.T) {
	// A closed port: bind a listener, then close it.
	ts := httptest.NewServer(http.NotFoundHandler())
	url := ts.URL
	ts.Close()

	out := captureStdout(t, func() {
		promptGate(gateClientFor(url), gateInput("migration snapshots"))
	})
	if out != "" {
		t.Errorf("server-down gate wrote stdout: %q", out)
	}
}

// Server past its budget: the gate waits at most its own timeout, then
// resolves to silence — the prompt is never delayed by the full 5s hook budget.
func TestPromptGate_SlowServerIsSilentWithinBudget(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hold the response past the gate budget
	}))
	defer func() { close(release); ts.Close() }()

	start := time.Now()
	out := captureStdout(t, func() {
		promptGate(gateClientFor(ts.URL), gateInput("migration snapshots"))
	})
	elapsed := time.Since(start)

	if out != "" {
		t.Errorf("slow-server gate wrote stdout: %q", out)
	}
	if elapsed > gateTimeout+500*time.Millisecond {
		t.Errorf("gate held the prompt %v, budget is %v", elapsed, gateTimeout)
	}
}

// Non-200 and malformed bodies resolve to silence.
func TestPromptGate_BadResponsesAreSilent(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"http 500": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		},
		"http 503": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "locked", http.StatusServiceUnavailable)
		},
		"malformed json": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("{not json"))
		},
		"wrong shape": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"inject": "not-a-list"}`))
		},
	}
	for name, handler := range cases {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(handler)
			defer ts.Close()
			out := captureStdout(t, func() {
				promptGate(gateClientFor(ts.URL), gateInput("migration snapshots"))
			})
			if out != "" {
				t.Errorf("%s: gate wrote stdout: %q", name, out)
			}
		})
	}
}

// Empty inject list (shadow mode, or nothing above τ): silence, and the
// request carried the prompt + session for calibration.
func TestPromptGate_EmptyInjectIsSilent(t *testing.T) {
	var gotBody map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/gate" {
			t.Errorf("gate called %s, want /api/gate", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&gotBody)
		json.NewEncoder(w).Encode(map[string]any{
			"mode": "shadow", "max_similarity": 0.61, "tau": 0.5, "inject": []any{},
		})
	}))
	defer ts.Close()

	out := captureStdout(t, func() {
		promptGate(gateClientFor(ts.URL), gateInput("a scarred topic"))
	})
	if out != "" {
		t.Errorf("shadow gate wrote stdout: %q", out)
	}
	if gotBody["prompt"] != "a scarred topic" || gotBody["session_id"] != "sess-1" {
		t.Errorf("gate request body = %v", gotBody)
	}
}

// The one success path: injectable hits render as L0 + mem:// URI lines in a
// UserPromptSubmit additionalContext payload.
func TestPromptGate_InjectsL0AndURI(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"mode": "on", "max_similarity": 0.73, "tau": 0.5,
			"inject": []map[string]any{
				{"uri": "mem://agent/cases/scar", "l0_abstract": "migration snapshots caused data loss", "similarity": 0.73},
			},
		})
	}))
	defer ts.Close()

	out := captureStdout(t, func() {
		promptGate(gateClientFor(ts.URL), gateInput("migration snapshots"))
	})
	var parsed UserPromptSubmitOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("stdout is not hook JSON: %q (%v)", out, err)
	}
	if parsed.HookSpecificOutput.HookEventName != "UserPromptSubmit" {
		t.Errorf("hookEventName = %q", parsed.HookSpecificOutput.HookEventName)
	}
	ctx := parsed.HookSpecificOutput.AdditionalContext
	if !strings.Contains(ctx, "mem://agent/cases/scar") ||
		!strings.Contains(ctx, "migration snapshots caused data loss") {
		t.Errorf("additionalContext missing L0/URI: %q", ctx)
	}
	if strings.Contains(ctx, "L1") {
		t.Errorf("additionalContext must carry pointers, not payloads: %q", ctx)
	}
}

// Empty prompts never round-trip.
func TestPromptGate_EmptyPromptSkips(t *testing.T) {
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer ts.Close()
	out := captureStdout(t, func() {
		promptGate(gateClientFor(ts.URL), gateInput(""))
	})
	if called || out != "" {
		t.Errorf("empty prompt reached the gate (called=%v, out=%q)", called, out)
	}
}
