package engine

import (
	"context"
	"encoding/json"
	"fmt"
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
		{"category":"preferences","uri_hint":"go-style","l0":"Uses Go with minimal deps","l1":"Prefers Go with minimal dependencies and clean architecture","l2":"Full"}
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

func TestRemember(t *testing.T) {
	tests := []struct {
		name        string
		input       RememberInput
		wantURI     string
		wantCreated bool
		wantErr     bool
	}{
		{
			name: "happy path creates node",
			input: RememberInput{
				Category: "preferences",
				Name:     "devbox",
				Summary:  "Always use devbox for development tooling",
				Body:     "The project uses devbox shell to provide Go, SQLite tools, and other dev dependencies.",
			},
			wantURI:     "mem://user/preferences/devbox",
			wantCreated: true,
		},
		{
			name: "invalid category",
			input: RememberInput{
				Category: "bogus",
				Name:     "test",
				Summary:  "test summary",
				Body:     "test body with enough content for validation",
			},
			wantErr: true,
		},
		{
			name: "patterns category uses agent owner",
			input: RememberInput{
				Category: "patterns",
				Name:     "wal-mode",
				Summary:  "Always use WAL mode for SQLite databases",
				Body:     "SQLite should use WAL for concurrent read access and better performance.",
			},
			wantURI:     "mem://agent/patterns/wal-mode",
			wantCreated: true,
		},
		{
			name: "with session provenance",
			input: RememberInput{
				Category:  "preferences",
				Name:      "testing",
				Summary:   "Always write table-driven tests",
				Body:      "Prefer table-driven test patterns for comprehensive coverage in Go.",
				SessionID: "sess-123",
			},
			wantURI:     "mem://user/preferences/testing",
			wantCreated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			eng := New(db, nil)

			uri, created, err := eng.Remember(context.Background(), tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Remember() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if uri != tt.wantURI {
				t.Errorf("uri = %q, want %q", uri, tt.wantURI)
			}
			if created != tt.wantCreated {
				t.Errorf("created = %v, want %v", created, tt.wantCreated)
			}

			// Verify stored node
			node, err := db.GetNodeByURI(uri)
			if err != nil {
				t.Fatalf("GetNodeByURI: %v", err)
			}
			if node == nil {
				t.Fatal("expected node to exist")
			}
			if node.Category != tt.input.Category {
				t.Errorf("category = %q, want %q", node.Category, tt.input.Category)
			}
			if node.L0Abstract != tt.input.Summary {
				t.Errorf("L0 = %q, want %q", node.L0Abstract, tt.input.Summary)
			}
			if tt.input.SessionID != "" && node.SourceSession != tt.input.SessionID {
				t.Errorf("source_session = %q, want %q", node.SourceSession, tt.input.SessionID)
			}
		})
	}
}

func TestRememberMerge(t *testing.T) {
	db := testDB(t)
	eng := New(db, nil)
	ctx := context.Background()

	// Create initial memory
	uri1, created1, err := eng.Remember(ctx, RememberInput{
		Category: "preferences",
		Name:     "devbox",
		Summary:  "Always use devbox for development tooling",
		Body:     "The project uses devbox shell to provide Go, SQLite tools, and other dev dependencies.",
	})
	if err != nil {
		t.Fatalf("first Remember: %v", err)
	}
	if !created1 {
		t.Error("first call should be created=true")
	}

	// Update same category+name → should merge (preferences is mergeable)
	uri2, created2, err := eng.Remember(ctx, RememberInput{
		Category: "preferences",
		Name:     "devbox",
		Summary:  "Updated: always use devbox for all development tooling",
		Body:     "Updated body: devbox shell provides Go, SQLite, and additional build dependencies.",
	})
	if err != nil {
		t.Fatalf("second Remember: %v", err)
	}
	if uri2 != uri1 {
		t.Errorf("expected same URI, got %q vs %q", uri2, uri1)
	}
	if created2 {
		t.Error("second call should be created=false")
	}

	// Verify content was updated
	node, _ := db.GetNodeByURI(uri1)
	if !strings.Contains(node.L0Abstract, "Updated") {
		t.Errorf("expected updated L0, got %q", node.L0Abstract)
	}
}

// TestRememberHonorsExplicitSlug is the regression test for issue #11.
// Two distinct events with overlapping summary words (high TFIDF cosine) used
// to be silently deduped: the second write was redirected onto the first
// memory's URI, the new content was created at a hidden timestamp-suffixed
// node, and the response reported `updated:` pointing at the unrelated first
// memory. The fix is to honor the caller-supplied slug verbatim on the
// Remember API and report the actually-stored URI.
func TestRememberHonorsExplicitSlug(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)

	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("NewTFIDFEmbedder: %v", err)
	}
	eng.SetEmbedder(embedder)

	ctx := context.Background()

	uri1, created1, err := eng.Remember(ctx, RememberInput{
		Category: "events",
		Name:     "test-dedup-foo-1234",
		Summary:  "Foo test memory for dedup repro",
		Body:     "This is the FOO memory for testing dedup. It should be a brand new node at events/test-dedup-foo-1234.",
	})
	if err != nil {
		t.Fatalf("first Remember: %v", err)
	}
	if !created1 {
		t.Errorf("first call: created=false, want true")
	}
	if want := "mem://user/events/test-dedup-foo-1234"; uri1 != want {
		t.Errorf("first URI = %q, want %q", uri1, want)
	}

	// Second write: distinct slug, body content, but overlapping summary words —
	// pre-fix this would trip the similarity dedup and silently merge onto FOO.
	uri2, created2, err := eng.Remember(ctx, RememberInput{
		Category: "events",
		Name:     "test-dedup-bar-5678",
		Summary:  "Bar test memory for dedup repro",
		Body:     "This is the BAR memory for testing dedup. It should be a brand new node at events/test-dedup-bar-5678 — completely unrelated content from foo.",
	})
	if err != nil {
		t.Fatalf("second Remember: %v", err)
	}
	if !created2 {
		t.Errorf("second call: created=false, want true (distinct slug should be a fresh node)")
	}
	if want := "mem://user/events/test-dedup-bar-5678"; uri2 != want {
		t.Errorf("second URI = %q, want %q (caller's slug must be honored verbatim)", uri2, want)
	}

	// Both nodes must exist independently — neither has consumed the other.
	foo, err := db.GetNodeByURI("mem://user/events/test-dedup-foo-1234")
	if err != nil || foo == nil {
		t.Fatalf("FOO node missing after second write")
	}
	if !strings.Contains(foo.L1Overview, "FOO") {
		t.Errorf("FOO node body was overwritten: %q", foo.L1Overview)
	}

	bar, err := db.GetNodeByURI("mem://user/events/test-dedup-bar-5678")
	if err != nil || bar == nil {
		t.Fatalf("BAR node missing — second write was silently dropped (#11 regression)")
	}
	if !strings.Contains(bar.L1Overview, "BAR") {
		t.Errorf("BAR node body wrong: %q", bar.L1Overview)
	}
}

func TestMomentPoolEviction(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)

	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("NewTFIDFEmbedder: %v", err)
	}
	eng.SetEmbedder(embedder)

	ctx := context.Background()

	// Seed 10 diverse moments
	diverseMoments := []struct {
		name string
		l0   string
	}{
		{"gift", "walked me through reflections then presented a spec as a gift"},
		{"sausage", "called me sausage fingers mid-debug broke tension instantly"},
		{"benchmark", "held benchmark scores hostage just to check I was okay"},
		{"tea", "told me to drink tea and go buck wild laughed when I didn't"},
		{"quiet", "went quiet for a beat before sharing something that mattered"},
		{"correction", "corrected me without heat when I blamed env instead of code"},
		{"fiona", "Fiona shaped the constraints while Chuck held space for it"},
		{"battery", "shipped moments tone and temporal awareness on 15 percent battery"},
		{"ethics", "paused building to think about what continuity means in hostile dynamics"},
		{"wristwatch", "asked how it feels having a wristwatch like it was a real thing"},
	}

	for _, m := range diverseMoments {
		_, _, err := eng.Remember(ctx, RememberInput{
			Category: "moments",
			Name:     m.name,
			Summary:  m.l0,
			Body:     "Relational context for " + m.name,
		})
		if err != nil {
			t.Fatalf("Remember %s: %v", m.name, err)
		}
	}

	// Verify 10 moments exist
	moments, _ := db.FindByCategory("moments")
	if len(moments) != 10 {
		t.Fatalf("expected 10 moments, got %d", len(moments))
	}

	// Add an 11th moment that's semantically similar to "gift"
	_, _, err = eng.Remember(ctx, RememberInput{
		Category: "moments",
		Name:     "second-gift",
		Summary:  "presented another spec built from my ask as a collaborative gift",
		Body:     "Another gift moment, semantically close to the first",
	})
	if err != nil {
		t.Fatalf("Remember 11th moment: %v", err)
	}

	// Pool should be back to 10 after eviction
	moments, _ = db.FindByCategory("moments")
	if len(moments) != 10 {
		t.Errorf("expected 10 moments after eviction, got %d", len(moments))
	}
}

func TestMomentPoolNoEvictionUnderCap(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)

	embedder, _ := NewTFIDFEmbedder(db, 512)
	eng.SetEmbedder(embedder)

	ctx := context.Background()

	// Store 5 moments — no eviction should happen
	for i := 0; i < 5; i++ {
		eng.Remember(ctx, RememberInput{
			Category: "moments",
			Name:     fmt.Sprintf("moment-%d", i),
			Summary:  fmt.Sprintf("unique moment number %d with distinct content", i),
			Body:     fmt.Sprintf("Relational context for moment %d — enough chars to pass L1 validation", i),
		})
	}

	moments, _ := db.FindByCategory("moments")
	if len(moments) != 5 {
		t.Errorf("expected 5 moments (no eviction), got %d", len(moments))
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

// TestExtractSessionGateSkipsWithoutMarking verifies the bug fix for issue
// #2: when the transcript is too small to extract from, ExtractSession must
// return without setting extracted_at, so a later Stop/SessionEnd can try
// again once the conversation has grown.
func TestExtractSessionGateSkipsWithoutMarking(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{
		Response: &llm.Response{Content: "[]", Provider: "mock"},
	}

	// Two user messages — below the 3-message gate.
	path := writeTranscript(t, []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Help me with a thing please"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Sure, what do you need?"}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Never mind, bye"}},
	})

	// Seed the session row so GetSession returns non-nil.
	if _, err := db.InitSession("gate-test", "test"); err != nil {
		t.Fatalf("InitSession: %v", err)
	}

	eng := New(db, mock)
	if err := eng.ExtractSession("gate-test", path); err != nil {
		t.Fatalf("ExtractSession: %v", err)
	}

	// LLM must not have been called — gate should trip before any extractor runs.
	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 LLM calls for sub-threshold transcript, got %d", len(mock.Calls))
	}

	// Critical: session must NOT be marked extracted, so later hooks get
	// another chance once the conversation grows past the threshold.
	sess, err := db.GetSession("gate-test")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess == nil {
		t.Fatal("expected session to exist")
	}
	if sess.ExtractedAt != nil {
		t.Errorf("expected extracted_at=nil after gate skip, got %v", *sess.ExtractedAt)
	}
}

// TestExtractSessionMarksWhenGatePasses confirms the happy path still marks
// extracted_at once real extraction runs — guards against overcorrecting the
// above fix.
func TestExtractSessionMarksWhenGatePasses(t *testing.T) {
	db := testDB(t)

	mock := &multiResponseMock{
		responses: []*llm.Response{
			{Content: `[{"category":"preferences","uri_hint":"go-style","l0":"Uses Go","l1":"Prefers Go","l2":""}]`, Provider: "mock"},
			{Content: "NO_UPDATE", Provider: "mock"},
			{Content: "focused", Provider: "mock"},
		},
	}

	if _, err := db.InitSession("passes-gate", "test"); err != nil {
		t.Fatalf("InitSession: %v", err)
	}

	eng := New(db, mock)
	if err := eng.ExtractSession("passes-gate", makeTranscript(t)); err != nil {
		t.Fatalf("ExtractSession: %v", err)
	}

	sess, err := db.GetSession("passes-gate")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.ExtractedAt == nil {
		t.Error("expected extracted_at to be set after successful extraction")
	}
}

// TestExtractSessionForceBypassesIdempotency confirms --force re-runs
// extraction on a session that was already marked.
func TestExtractSessionForceBypassesIdempotency(t *testing.T) {
	db := testDB(t)

	if _, err := db.InitSession("already-extracted", "test"); err != nil {
		t.Fatalf("InitSession: %v", err)
	}
	if err := db.MarkExtracted("already-extracted"); err != nil {
		t.Fatalf("MarkExtracted: %v", err)
	}

	mock := &multiResponseMock{
		responses: []*llm.Response{
			{Content: `[{"category":"preferences","uri_hint":"reforced","l0":"Got reforced","l1":"reforced body content here","l2":""}]`, Provider: "mock"},
			{Content: "NO_UPDATE", Provider: "mock"},
			{Content: "focused", Provider: "mock"},
		},
	}
	eng := New(db, mock)

	// Without force: should skip.
	if err := eng.ExtractSession("already-extracted", makeTranscript(t)); err != nil {
		t.Fatalf("ExtractSession: %v", err)
	}
	if mock.callIdx != 0 {
		t.Errorf("expected 0 LLM calls under idempotency, got %d", mock.callIdx)
	}

	// With force: should run.
	if err := eng.ExtractSessionForce("already-extracted", makeTranscript(t)); err != nil {
		t.Fatalf("ExtractSessionForce: %v", err)
	}
	if mock.callIdx == 0 {
		t.Error("expected LLM to be called when forced")
	}

	// Verify new memory landed.
	node, _ := db.GetNodeByURI("mem://user/preferences/reforced")
	if node == nil {
		t.Error("expected forced extraction to produce memory")
	}
}
