package cli

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/server"
	"github.com/lazypower/continuity/internal/store"
)

// showTestServer spins up an in-memory server and points the CLI client at it
// via CONTINUITY_URL. Returns the store so tests can seed nodes directly.
func showTestServer(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	eng := engine.New(db, nil)
	srv := server.New(db, eng, "test-version")
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	prev := os.Getenv("CONTINUITY_URL")
	os.Setenv("CONTINUITY_URL", ts.URL)
	t.Cleanup(func() { os.Setenv("CONTINUITY_URL", prev) })

	return db
}

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what was
// written. Used because runShow prints with fmt.Println, matching the existing
// CLI style (other commands also write directly to os.Stdout).
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	orig := os.Stdout
	os.Stdout = w

	runErr := fn()

	w.Close()
	os.Stdout = orig

	out, _ := io.ReadAll(r)
	return string(out), runErr
}

// resetShowFlags resets package-level flag vars between test cases.
func resetShowFlags() {
	showLayer = "all"
	showJSON = false
}

func seedShowNode(t *testing.T, db *store.DB, uri, l0, l1, l2 string) {
	t.Helper()
	n := &store.MemNode{
		URI:        uri,
		NodeType:   "leaf",
		Category:   "patterns",
		L0Abstract: l0,
		L1Overview: l1,
		L2Content:  l2,
	}
	if err := db.CreateNode(n); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestShowAllLayers(t *testing.T) {
	db := showTestServer(t)
	seedShowNode(t, db,
		"mem://agent/patterns/test-journal",
		"tiny test",
		"section A\n- entry 1\n\nsection B\n- entry 2\n",
		"",
	)

	resetShowFlags()
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/test-journal"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	// Should include delimiters + both sections of the body
	for _, want := range []string{"## Summary", "tiny test", "## Body", "section A", "- entry 1", "section B", "- entry 2"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// Detail header should be absent when detail is empty
	if strings.Contains(out, "## Detail") {
		t.Errorf("## Detail should be hidden when empty, got:\n%s", out)
	}
}

func TestShowLayerBodyOnly(t *testing.T) {
	db := showTestServer(t)
	seedShowNode(t, db,
		"mem://agent/patterns/body-only",
		"the summary",
		"the body content",
		"",
	)

	resetShowFlags()
	showLayer = "body"
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/body-only"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	if strings.TrimSpace(out) != "the body content" {
		t.Errorf("expected body only, got: %q", out)
	}
}

func TestShowLayerSummaryOnly(t *testing.T) {
	db := showTestServer(t)
	seedShowNode(t, db,
		"mem://agent/patterns/summary-only",
		"the summary",
		"the body content",
		"",
	)

	resetShowFlags()
	showLayer = "summary"
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/summary-only"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	if strings.TrimSpace(out) != "the summary" {
		t.Errorf("expected summary only, got: %q", out)
	}
}

func TestShowJSON(t *testing.T) {
	db := showTestServer(t)
	seedShowNode(t, db,
		"mem://agent/patterns/json-test",
		"s",
		"b",
		"d",
	)

	resetShowFlags()
	showJSON = true
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/json-test"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if parsed["uri"] != "mem://agent/patterns/json-test" {
		t.Errorf("uri = %v", parsed["uri"])
	}
	if parsed["summary"] != "s" || parsed["body"] != "b" || parsed["detail"] != "d" {
		t.Errorf("tier values wrong: %+v", parsed)
	}
}

func TestShowJSONLayerFiltering(t *testing.T) {
	db := showTestServer(t)
	seedShowNode(t, db,
		"mem://agent/patterns/json-filter",
		"s",
		"b",
		"d",
	)

	resetShowFlags()
	showJSON = true
	showLayer = "body"
	out, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/json-filter"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if _, hasSummary := parsed["summary"]; hasSummary {
		t.Errorf("summary should be absent when --layer=body, got: %+v", parsed)
	}
	if _, hasDetail := parsed["detail"]; hasDetail {
		t.Errorf("detail should be absent when --layer=body, got: %+v", parsed)
	}
	if parsed["body"] != "b" {
		t.Errorf("body = %v, want b", parsed["body"])
	}
}

func TestShowAutoPrependsPrefix(t *testing.T) {
	db := showTestServer(t)
	seedShowNode(t, db,
		"mem://agent/patterns/no-prefix",
		"the summary",
		"the body",
		"",
	)

	resetShowFlags()
	out, err := captureStdout(t, func() error {
		// Omit the mem:// prefix
		return runShow(showCmd, []string{"agent/patterns/no-prefix"})
	})
	if err != nil {
		t.Fatalf("runShow: %v", err)
	}
	if !strings.Contains(out, "the summary") || !strings.Contains(out, "the body") {
		t.Errorf("auto-prepend failed, got: %s", out)
	}
}

func TestShowNotFound(t *testing.T) {
	showTestServer(t)

	resetShowFlags()
	_, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/never-existed"})
	})
	if err == nil {
		t.Fatal("expected error for missing memory, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestShowInvalidLayer(t *testing.T) {
	showTestServer(t)

	resetShowFlags()
	showLayer = "bogus"
	_, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/anything"})
	})
	if err == nil {
		t.Fatal("expected error for invalid layer, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --layer") {
		t.Errorf("error should mention invalid --layer, got: %v", err)
	}
}

func TestShowServerDown(t *testing.T) {
	// Point at a URL that won't respond.
	prev := os.Getenv("CONTINUITY_URL")
	os.Setenv("CONTINUITY_URL", "http://127.0.0.1:1") // port 1 is reserved, should refuse
	t.Cleanup(func() { os.Setenv("CONTINUITY_URL", prev) })

	resetShowFlags()
	_, err := captureStdout(t, func() error {
		return runShow(showCmd, []string{"mem://agent/patterns/anything"})
	})
	if err == nil {
		t.Fatal("expected error when server is unreachable, got nil")
	}
	if !strings.Contains(err.Error(), "server is not running") {
		t.Errorf("error should mention server not running, got: %v", err)
	}
}
