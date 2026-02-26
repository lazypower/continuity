package engine

import (
	"context"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

func seedTestNodes(t *testing.T, db *store.DB) []*store.MemNode {
	t.Helper()

	nodes := []*store.MemNode{
		{URI: "mem://user/profile/go-dev", NodeType: "leaf", Category: "profile",
			L0Abstract: "Go developer who prefers minimal dependencies and clean code"},
		{URI: "mem://user/preferences/sqlite", NodeType: "leaf", Category: "preferences",
			L0Abstract: "Uses SQLite with WAL mode for concurrent reads in Go applications"},
		{URI: "mem://agent/patterns/error-handling", NodeType: "leaf", Category: "patterns",
			L0Abstract: "Pattern: graceful error handling with Go error wrapping and fmt.Errorf"},
		{URI: "mem://user/entities/continuity", NodeType: "leaf", Category: "entities",
			L0Abstract: "Project: continuity-go, a persistent memory system for AI coding agents"},
		{URI: "mem://agent/cases/sqlite-wal", NodeType: "leaf", Category: "cases",
			L0Abstract: "Fixed: SQLite concurrent write issue by enabling WAL journal mode"},
	}

	for _, n := range nodes {
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("CreateNode %s: %v", n.URI, err)
		}
	}
	return nodes
}

func embedTestNodes(t *testing.T, db *store.DB, embedder Embedder, nodes []*store.MemNode) {
	t.Helper()
	ctx := context.Background()
	for _, n := range nodes {
		vec, err := embedder.Embed(ctx, n.L0Abstract)
		if err != nil {
			t.Fatalf("Embed %s: %v", n.URI, err)
		}
		if err := db.SaveVector(n.ID, vec, embedder.Model()); err != nil {
			t.Fatalf("SaveVector %s: %v", n.URI, err)
		}
	}
}

func TestFindBasic(t *testing.T) {
	db := testDB(t)
	nodes := seedTestNodes(t, db)

	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("NewTFIDFEmbedder: %v", err)
	}
	embedTestNodes(t, db, embedder, nodes)

	ctx := context.Background()
	results, err := Find(ctx, db, embedder, "Go developer minimal dependencies", SearchOpts{Limit: 5})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}

	// First result should be the Go dev profile (most similar)
	if results[0].Node.URI != "mem://user/profile/go-dev" {
		t.Errorf("top result URI = %q, want mem://user/profile/go-dev", results[0].Node.URI)
	}

	// Scores should be positive and sorted descending
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted: %f > %f at index %d", results[i].Score, results[i-1].Score, i)
		}
	}
}

func TestFindWithCategory(t *testing.T) {
	db := testDB(t)
	nodes := seedTestNodes(t, db)

	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("NewTFIDFEmbedder: %v", err)
	}
	embedTestNodes(t, db, embedder, nodes)

	ctx := context.Background()
	results, err := Find(ctx, db, embedder, "SQLite", SearchOpts{Category: "cases"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	// Should only return cases
	for _, r := range results {
		if r.Node.Category != "cases" {
			t.Errorf("expected category 'cases', got %q for %s", r.Node.Category, r.Node.URI)
		}
	}
}

func TestFindNoEmbedder(t *testing.T) {
	db := testDB(t)

	ctx := context.Background()
	_, err := Find(ctx, db, nil, "test", SearchOpts{})
	if err == nil {
		t.Error("expected error with nil embedder")
	}
}

func TestFindEmptyDB(t *testing.T) {
	db := testDB(t)

	embedder, _ := NewTFIDFEmbedder(db, 512)
	ctx := context.Background()
	results, err := Find(ctx, db, embedder, "test query", SearchOpts{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestSearchWithMockLLM(t *testing.T) {
	db := testDB(t)
	nodes := seedTestNodes(t, db)

	embedder, _ := NewTFIDFEmbedder(db, 512)
	embedTestNodes(t, db, embedder, nodes)

	mockLLM := &llm.MockClient{
		Response: &llm.Response{
			Content: `[{"query": "Go developer", "type": "MEMORY"}, {"query": "SQLite WAL", "type": "RESOURCE"}]`,
		},
	}

	ctx := context.Background()
	results, err := Search(ctx, db, embedder, mockLLM, "How does the user work with Go and SQLite?", SearchOpts{Limit: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected results from Search")
	}

	// Verify LLM was called
	if len(mockLLM.Calls) != 1 {
		t.Errorf("expected 1 LLM call, got %d", len(mockLLM.Calls))
	}
}

func TestSearchFallsBackToFind(t *testing.T) {
	db := testDB(t)
	nodes := seedTestNodes(t, db)

	embedder, _ := NewTFIDFEmbedder(db, 512)
	embedTestNodes(t, db, embedder, nodes)

	// nil LLM client — should fall back to Find
	ctx := context.Background()
	results, err := Search(ctx, db, embedder, nil, "Go developer", SearchOpts{Limit: 5})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected results from fallback Find")
	}
}

func TestParseSubQueries(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{`[{"query": "test", "type": "MEMORY"}]`, 1},
		{`[{"query": "a", "type": "MEMORY"}, {"query": "b", "type": "RESOURCE"}]`, 2},
		{`some text [{"query": "test", "type": "PATTERN"}] more text`, 1},
		{`invalid json`, 0},
		{"```json\n[{\"query\": \"test\", \"type\": \"MEMORY\"}]\n```", 1},
	}

	for _, tt := range tests {
		result := parseSubQueries(tt.input)
		if len(result) != tt.want {
			t.Errorf("parseSubQueries(%q) = %d results, want %d", tt.input, len(result), tt.want)
		}
	}
}
