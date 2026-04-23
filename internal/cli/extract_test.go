package cli

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/server"
	"github.com/lazypower/continuity/internal/store"
)

// extractTestServer stands up an in-memory server and points the CLI client
// at it via CONTINUITY_URL. Mirrors showTestServer.
func extractTestServer(t *testing.T) *store.DB {
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

func resetExtractFlags() {
	extractForce = false
	extractTranscript = ""
	extractBackfillEmpty = false
}

func writeDummyTranscript(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "transcript.jsonl")
	entries := []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Hello please help me out with this task"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Sure, I can help."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Another message here"}},
	}
	f, _ := os.Create(path)
	defer f.Close()
	for _, e := range entries {
		data, _ := json.Marshal(e)
		f.Write(data)
		f.Write([]byte("\n"))
	}
	return path
}

func TestExtractCLIWithExplicitTranscript(t *testing.T) {
	db := extractTestServer(t)
	db.InitSession("cli-sess", "proj")
	path := writeDummyTranscript(t)

	resetExtractFlags()
	extractTranscript = path

	out, err := captureStdout(t, func() error {
		return runExtract(extractCmd, []string{"cli-sess"})
	})
	if err != nil {
		t.Fatalf("runExtract: %v", err)
	}
	if !strings.Contains(out, "extraction queued") {
		t.Errorf("expected queued message, got: %s", out)
	}
}

func TestExtractCLIBackfillEmpty(t *testing.T) {
	db := extractTestServer(t)
	db.InitSession("damaged", "proj")
	db.MarkExtracted("damaged")

	resetExtractFlags()
	extractBackfillEmpty = true

	out, err := captureStdout(t, func() error {
		return runExtract(extractCmd, nil)
	})
	if err != nil {
		t.Fatalf("runExtract --backfill-empty: %v", err)
	}
	if !strings.Contains(out, "unmarked 1 session") {
		t.Errorf("expected unmark count, got: %s", out)
	}

	// Verify the damaged session was actually unmarked.
	s, _ := db.GetSession("damaged")
	if s.ExtractedAt != nil {
		t.Error("damaged session should have been unmarked via CLI path")
	}
}

func TestExtractCLIBackfillExclusiveFlags(t *testing.T) {
	extractTestServer(t)

	resetExtractFlags()
	extractBackfillEmpty = true
	extractForce = true

	err := runExtract(extractCmd, nil)
	if err == nil {
		t.Fatal("expected error when --backfill-empty combined with --force")
	}
	if !strings.Contains(err.Error(), "cannot be combined") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractCLIRequiresSessionID(t *testing.T) {
	extractTestServer(t)
	resetExtractFlags()

	err := runExtract(extractCmd, nil)
	if err == nil {
		t.Fatal("expected error when no session-id and no --backfill-empty")
	}
}

func TestExtractCLITranscriptMissing(t *testing.T) {
	extractTestServer(t)
	resetExtractFlags()
	extractTranscript = "/no/such/path.jsonl"

	err := runExtract(extractCmd, []string{"abc"})
	if err == nil {
		t.Fatal("expected error for missing transcript")
	}
	if !strings.Contains(err.Error(), "not readable") {
		t.Errorf("unexpected error: %v", err)
	}
}
