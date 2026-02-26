package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

func testDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// writeTranscript writes a JSONL transcript file for testing.
func writeTranscript(t *testing.T, entries []map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "transcript.jsonl")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create transcript: %v", err)
	}
	defer f.Close()

	for _, entry := range entries {
		data, _ := json.Marshal(entry)
		f.Write(data)
		f.Write([]byte("\n"))
	}
	return path
}

// makeTranscript creates a realistic transcript with user and assistant messages.
func makeTranscript(t *testing.T) string {
	return writeTranscript(t, []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Help me build a Go CLI tool with cobra for task management"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "I'll help you build a Go CLI tool using cobra. Let me start by setting up the project structure with go mod init and creating the main command file."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "I prefer using minimal dependencies. Always use the standard library where possible."}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Good point. I'll stick to the standard library for HTTP, JSON, and file operations. Cobra is the only external dependency we need for the CLI framework."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Can you add SQLite for storage? Use modernc.org/sqlite since it's pure Go."}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "I'll add modernc.org/sqlite as the database driver. It's a pure Go implementation that cross-compiles cleanly without CGO."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Looks good. Remember to always use WAL mode for SQLite in production."}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Got it, I'll set PRAGMA journal_mode=WAL in the database initialization."}},
	})
}

func TestExtractSession(t *testing.T) {
	db := testDB(t)

	extractionResponse := `[
		{
			"category": "preferences",
			"uri_hint": "minimal-dependencies",
			"l0": "Prefers minimal dependencies, standard library where possible",
			"l1": "The user strongly prefers minimal external dependencies. Uses standard library for HTTP, JSON, and file operations. Only adds external packages when truly necessary (e.g., cobra for CLI).",
			"l2": "Full details about dependency preferences..."
		},
		{
			"category": "patterns",
			"uri_hint": "sqlite-wal-mode",
			"l0": "Always use WAL mode for SQLite databases",
			"l1": "SQLite should be configured with WAL (Write-Ahead Logging) mode for concurrent read access and better performance.",
			"l2": "Full WAL mode details..."
		}
	]`

	mock := &llm.MockClient{
		Response: &llm.Response{Content: extractionResponse, Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)
	engine := New(db, mock)

	// Only test extraction, not relational (mock returns same response for both)
	err := extractMemories(db, mock, nil, "test-session", transcriptPath)
	if err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	// Verify memories were created
	prefs, err := db.FindByCategory("preferences")
	if err != nil {
		t.Fatalf("FindByCategory: %v", err)
	}
	if len(prefs) != 1 {
		t.Errorf("expected 1 preference, got %d", len(prefs))
	}
	if len(prefs) > 0 && !strings.Contains(prefs[0].L0Abstract, "minimal dependencies") {
		t.Errorf("unexpected l0: %q", prefs[0].L0Abstract)
	}

	patterns, err := db.FindByCategory("patterns")
	if err != nil {
		t.Fatalf("FindByCategory: %v", err)
	}
	if len(patterns) != 1 {
		t.Errorf("expected 1 pattern, got %d", len(patterns))
	}

	_ = engine // used
}

func TestExtractSessionSkipsFewMessages(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{
		Response: &llm.Response{Content: "[]", Provider: "mock"},
	}

	// Only 2 user messages — should skip
	path := writeTranscript(t, []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Hello this is a test message"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Hi there, how can I help you today?"}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Goodbye this is another test message"}},
	})

	err := extractMemories(db, mock, nil, "test-session", path)
	if err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	// Should not have called the LLM
	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 LLM calls for few messages, got %d", len(mock.Calls))
	}
}

func TestExtractRelational(t *testing.T) {
	db := testDB(t)

	relationalResponse := `## 1. FEEDBACK CALIBRATION
User gives direct, specific feedback. Uses "good" sparingly — when they say it, they mean it.

## 2. WORKING DYNAMIC
Prefers to give high-level direction and let the agent execute autonomously. Reviews results rather than each step.

## 3. CORRECTIONS RECEIVED
- "Always use WAL mode for SQLite"
- "Prefer standard library where possible"

## 4. EARNED SIGNALS
User trusts agent with code generation and architectural decisions.`

	mock := &llm.MockClient{
		Response: &llm.Response{Content: relationalResponse, Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)

	err := extractRelational(db, mock, "test-session", transcriptPath)
	if err != nil {
		t.Fatalf("extractRelational: %v", err)
	}

	// Verify relational profile was created
	node, err := db.GetNodeByURI(relationalURI)
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if node == nil {
		t.Fatal("expected relational profile node")
	}
	if !strings.Contains(node.L1Overview, "FEEDBACK CALIBRATION") {
		t.Errorf("expected profile content, got: %q", node.L1Overview[:50])
	}
	if node.SourceSession != "test-session" {
		t.Errorf("source_session = %q, want test-session", node.SourceSession)
	}
}

func TestExtractRelationalDedup(t *testing.T) {
	db := testDB(t)

	// Pre-create the relational profile from same session
	db.UpsertNode(&store.MemNode{
		URI:           relationalURI,
		NodeType:      "leaf",
		Category:      "profile",
		L1Overview:    "Existing profile content",
		SourceSession: "test-session",
	})

	mock := &llm.MockClient{
		Response: &llm.Response{Content: "Should not be called", Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)

	err := extractRelational(db, mock, "test-session", transcriptPath)
	if err != nil {
		t.Fatalf("extractRelational: %v", err)
	}

	// Should not have called LLM (dedup)
	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 LLM calls (dedup), got %d", len(mock.Calls))
	}
}

func TestExtractRelationalNoUpdate(t *testing.T) {
	db := testDB(t)

	mock := &llm.MockClient{
		Response: &llm.Response{Content: "NO_UPDATE", Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)

	err := extractRelational(db, mock, "test-session", transcriptPath)
	if err != nil {
		t.Fatalf("extractRelational: %v", err)
	}

	// Should NOT have created a node
	node, _ := db.GetNodeByURI(relationalURI)
	if node != nil {
		t.Error("expected no node for NO_UPDATE response")
	}
}

func TestParseExtractionResponse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{
			name:  "plain json",
			input: `[{"category":"profile","uri_hint":"test","l0":"test","l1":"test","l2":"test"}]`,
			want:  1,
		},
		{
			name:  "with code fences",
			input: "```json\n[{\"category\":\"profile\",\"uri_hint\":\"test\",\"l0\":\"test\",\"l1\":\"test\",\"l2\":\"test\"}]\n```",
			want:  1,
		},
		{
			name:  "empty array",
			input: "[]",
			want:  0,
		},
		{
			name:  "with surrounding text",
			input: "Here are the memories:\n[{\"category\":\"events\",\"uri_hint\":\"deploy\",\"l0\":\"deployed\",\"l1\":\"deployed v2\",\"l2\":\"full\"}]\nThat's all.",
			want:  1,
		},
		{
			name:    "no array",
			input:   "No memories to extract.",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseExtractionResponse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseExtractionResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(got) != tt.want {
				t.Errorf("parseExtractionResponse() got %d candidates, want %d", len(got), tt.want)
			}
		})
	}
}

func TestFullPipeline(t *testing.T) {
	db := testDB(t)

	extractionResp := `[
		{"category":"preferences","uri_hint":"go-style","l0":"Uses Go with minimal deps","l1":"Prefers Go","l2":"Full"}
	]`
	relationalResp := `## 1. FEEDBACK CALIBRATION
Direct feedback style.

## 2. WORKING DYNAMIC
Autonomous execution preferred.

## 3. CORRECTIONS RECEIVED
- Use WAL mode

## 4. EARNED SIGNALS
Trusts agent with code generation.`

	callCount := 0
	mock := &llm.MockClient{}
	// Override Complete to return different responses for extraction vs relational
	originalComplete := mock.Complete
	_ = originalComplete
	mock.Response = &llm.Response{Content: extractionResp, Provider: "mock"}

	// Create a multi-response mock
	multiMock := &multiResponseMock{
		responses: []*llm.Response{
			{Content: extractionResp, Provider: "mock"},
			{Content: relationalResp, Provider: "mock"},
		},
	}
	_ = callCount

	transcriptPath := makeTranscript(t)
	engine := New(db, multiMock)

	err := engine.ExtractSession("full-test", transcriptPath)
	if err != nil {
		t.Fatalf("ExtractSession: %v", err)
	}

	// Verify memories exist
	leaves, _ := db.ListLeaves()
	foundPref := false
	foundProfile := false
	for _, l := range leaves {
		if l.Category == "preferences" {
			foundPref = true
		}
		if l.URI == relationalURI {
			foundProfile = true
		}
	}
	if !foundPref {
		t.Error("expected preferences memory")
	}
	if !foundProfile {
		t.Error("expected relational profile")
	}
}

func TestExtractSignal(t *testing.T) {
	db := testDB(t)

	signalResponse := `[{
		"category": "preferences",
		"uri_hint": "wal-mode",
		"l0": "Always use WAL mode for SQLite databases",
		"l1": "SQLite should be configured with WAL (Write-Ahead Logging) mode for concurrent read access and better write performance in production.",
		"l2": "Full WAL mode details: PRAGMA journal_mode=WAL should be set during database initialization."
	}]`

	mock := &llm.MockClient{
		Response: &llm.Response{Content: signalResponse, Provider: "mock"},
	}

	eng := New(db, mock)

	err := eng.ExtractSignal(context.Background(), "test-session", "remember this: always use WAL mode")
	if err != nil {
		t.Fatalf("ExtractSignal: %v", err)
	}

	// Verify the memory was created
	node, err := db.GetNodeByURI("mem://user/preferences/wal-mode")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if node == nil {
		t.Fatal("expected node to be created")
	}
	if !strings.Contains(node.L0Abstract, "WAL mode") {
		t.Errorf("unexpected L0: %q", node.L0Abstract)
	}
	if node.SourceSession != "test-session" {
		t.Errorf("source_session = %q, want test-session", node.SourceSession)
	}

	// Verify LLM was called with signal prompt
	if len(mock.Calls) != 1 {
		t.Errorf("expected 1 LLM call, got %d", len(mock.Calls))
	}
	if len(mock.Calls) > 0 && !strings.Contains(mock.Calls[0], "explicitly flagged") {
		t.Error("expected signal extraction prompt")
	}
}

func TestExtractSignalNoLLM(t *testing.T) {
	db := testDB(t)
	eng := New(db, nil)

	err := eng.ExtractSignal(context.Background(), "test-session", "remember this: test")
	if err == nil {
		t.Error("expected error when LLM is nil")
	}
}

// multiResponseMock returns different responses for successive calls.
type multiResponseMock struct {
	responses []*llm.Response
	callIdx   int
}

func (m *multiResponseMock) Complete(ctx context.Context, prompt string) (*llm.Response, error) {
	if m.callIdx >= len(m.responses) {
		return m.responses[len(m.responses)-1], nil
	}
	resp := m.responses[m.callIdx]
	m.callIdx++
	return resp, nil
}
