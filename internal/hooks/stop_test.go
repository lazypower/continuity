package hooks

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// writeStopTranscript writes a JSONL transcript for stop-hook gate testing.
func writeStopTranscript(t *testing.T, entries []map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "transcript.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	for _, e := range entries {
		data, _ := json.Marshal(e)
		f.Write(data)
		f.Write([]byte("\n"))
	}
	return path
}

func TestShouldExtractBelowUserMessageThreshold(t *testing.T) {
	path := writeStopTranscript(t, []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Just saying hello please respond"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Hello! How can I help?"}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Actually never mind, bye"}},
	})

	if shouldExtract(path) {
		t.Error("expected false — only 2 user messages, below 3-message gate")
	}
}

func TestShouldExtractPassesThreshold(t *testing.T) {
	// Reuse the multi-message transcript shape from engine tests.
	path := writeStopTranscript(t, []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Help me build a Go CLI tool with cobra for task management please"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "I'll help you build a Go CLI tool using cobra. Let me start by setting up the project structure."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "I prefer minimal dependencies. Always use the standard library where possible."}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Good point. I'll stick to the standard library for HTTP, JSON, and file operations."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Can you add SQLite for storage? Use modernc.org/sqlite since it's pure Go."}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "I'll add modernc.org/sqlite. Pure Go, cross-compiles cleanly without CGO."}},
	})

	if !shouldExtract(path) {
		t.Error("expected true — 3 user messages with plenty of content should pass")
	}
}

func TestShouldExtractMissingFile(t *testing.T) {
	if shouldExtract("/definitely/does/not/exist.jsonl") {
		t.Error("expected false when transcript can't be read")
	}
}
