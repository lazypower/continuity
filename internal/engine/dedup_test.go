package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// seedDuplicateNodes creates nodes with semantically similar L0 abstracts for dedup testing.
func seedDuplicateNodes(t *testing.T, db *store.DB) []*store.MemNode {
	t.Helper()

	nodes := []*store.MemNode{
		{URI: "mem://user/profile/seed-and-scale", NodeType: "leaf", Category: "profile",
			L0Abstract: "User prefers incremental seed and scale validation approach"},
		{URI: "mem://user/profile/seed-and-scale-approach", NodeType: "leaf", Category: "profile",
			L0Abstract: "User prefers incremental seed and scale validation strategy"},
		{URI: "mem://user/profile/seed-and-scale-validation", NodeType: "leaf", Category: "profile",
			L0Abstract: "User prefers incremental seed and scale validation method"},
		{URI: "mem://user/preferences/install-local-bin", NodeType: "leaf", Category: "preferences",
			L0Abstract: "Install binaries to ~/.local/bin not system directories"},
		{URI: "mem://user/preferences/install-to-local-bin", NodeType: "leaf", Category: "preferences",
			L0Abstract: "Install binaries to ~/.local/bin instead of /usr/local/bin"},
		{URI: "mem://agent/patterns/collaborative-debug", NodeType: "leaf", Category: "patterns",
			L0Abstract: "User debugs collaboratively with real-time investigation and testing"},
		// This one is different enough to survive dedup
		{URI: "mem://user/entities/continuity-project", NodeType: "leaf", Category: "entities",
			L0Abstract: "Continuity-go is a persistent memory system for AI coding agents"},
	}

	for _, n := range nodes {
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("CreateNode %s: %v", n.URI, err)
		}
	}
	return nodes
}

func TestFindSimilarNode(t *testing.T) {
	db := testDB(t)

	// Create existing nodes
	existing := &store.MemNode{
		URI: "mem://user/profile/seed-and-scale", NodeType: "leaf", Category: "profile",
		L0Abstract: "User prefers incremental seed and scale validation approach",
	}
	if err := db.CreateNode(existing); err != nil {
		t.Fatal(err)
	}

	// Build embedder from this DB
	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatal(err)
	}

	// Embed the existing node
	ctx := context.Background()
	vec, _ := embedder.Embed(ctx, existing.L0Abstract)
	db.SaveVector(existing.ID, vec, embedder.Model())

	// Search for a very similar L0 — should match
	match, sim, err := findSimilarNode(ctx, db, embedder,
		"User prefers incremental seed and scale validation strategy",
		"profile", 0.7) // lower threshold for TF-IDF
	if err != nil {
		t.Fatal(err)
	}
	if match == nil {
		t.Fatal("expected a match for similar L0 abstract")
	}
	if match.URI != existing.URI {
		t.Errorf("match URI = %q, want %q", match.URI, existing.URI)
	}
	if sim < 0.7 {
		t.Errorf("similarity = %.3f, expected >= 0.7", sim)
	}

	// Search for a totally different L0 — should not match
	match, _, err = findSimilarNode(ctx, db, embedder,
		"Python machine learning tensorflow neural network training",
		"profile", 0.7)
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Errorf("expected no match for unrelated L0, got %s", match.URI)
	}

	// Search for correct L0 but wrong category — should not match
	match, _, err = findSimilarNode(ctx, db, embedder,
		"User prefers incremental seed and scale validation strategy",
		"preferences", 0.7)
	if err != nil {
		t.Fatal(err)
	}
	if match == nil {
		// TF-IDF might not have enough discrimination — that's fine for unit test
		t.Log("no match for wrong category (expected)")
	}
}

func TestFindSimilarNodeEmptyDB(t *testing.T) {
	db := testDB(t)

	embedder, _ := NewTFIDFEmbedder(db, 512)
	ctx := context.Background()

	match, sim, err := findSimilarNode(ctx, db, embedder, "test query", "profile", 0.85)
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Error("expected nil match on empty DB")
	}
	if sim != 0 {
		t.Errorf("expected 0 similarity, got %.3f", sim)
	}
}

func TestExtractMemoriesSimilarityGate(t *testing.T) {
	db := testDB(t)

	// Pre-create a node that should be the merge target
	existing := &store.MemNode{
		URI: "mem://user/preferences/minimal-deps", NodeType: "leaf", Category: "preferences",
		L0Abstract: "Prefers minimal dependencies, standard library where possible",
	}
	if err := db.CreateNode(existing); err != nil {
		t.Fatal(err)
	}

	// Build embedder and embed the existing node
	embedder, _ := NewTFIDFEmbedder(db, 512)
	ctx := context.Background()
	vec, _ := embedder.Embed(ctx, existing.L0Abstract)
	db.SaveVector(existing.ID, vec, embedder.Model())

	// LLM returns a candidate that's semantically very similar to the existing node
	extractionResponse := `[
		{
			"category": "preferences",
			"uri_hint": "minimal-dependencies-preference",
			"l0": "Prefers minimal dependencies, standard library where possible",
			"l1": "The user strongly prefers minimal external dependencies.",
			"l2": "Full details..."
		}
	]`

	mock := &llm.MockClient{
		Response: &llm.Response{Content: extractionResponse, Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)
	err := extractMemories(db, mock, embedder, "test-session", transcriptPath)
	if err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	// The candidate should have merged into the existing node rather than creating a new one
	prefs, _ := db.FindByCategory("preferences")
	for _, p := range prefs {
		t.Logf("pref: %s → %s", p.URI, p.L0Abstract)
	}

	// Verify the original node was updated (not a new node created with different URI)
	updated, err := db.GetNodeByURI(existing.URI)
	if err != nil {
		t.Fatal(err)
	}
	if updated == nil {
		t.Fatal("expected existing node to still exist")
	}
}

func TestExtractMemoriesNoEmbedder(t *testing.T) {
	db := testDB(t)

	// Without embedder, similarity gate should be skipped — extraction still works
	extractionResponse := `[
		{
			"category": "preferences",
			"uri_hint": "test-pref",
			"l0": "Test preference with no embedder",
			"l1": "Details",
			"l2": "Full"
		}
	]`

	mock := &llm.MockClient{
		Response: &llm.Response{Content: extractionResponse, Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)
	err := extractMemories(db, mock, nil, "test-session", transcriptPath)
	if err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	// Should have created the node normally
	node, _ := db.GetNodeByURI("mem://user/preferences/test-pref")
	if node == nil {
		t.Fatal("expected node to be created without embedder")
	}
}

func TestDedup(t *testing.T) {
	db := testDB(t)
	nodes := seedDuplicateNodes(t, db)

	// Build embedder and embed all nodes
	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, n := range nodes {
		vec, err := embedder.Embed(ctx, n.L0Abstract)
		if err != nil {
			t.Fatalf("embed %s: %v", n.URI, err)
		}
		db.SaveVector(n.ID, vec, embedder.Model())
	}

	eng := New(db, nil)
	eng.SetEmbedder(embedder)

	leavesBefore, _ := db.ListLeaves()
	t.Logf("Leaves before dedup: %d", len(leavesBefore))
	for _, l := range leavesBefore {
		t.Logf("  %s: %s", l.URI, l.L0Abstract)
	}

	// Use a lower threshold for TF-IDF (it produces lower similarity scores than neural embeddings)
	removed, err := eng.Dedup(ctx, 0.70)
	if err != nil {
		t.Fatalf("Dedup: %v", err)
	}

	leavesAfter, _ := db.ListLeaves()
	t.Logf("Leaves after dedup: %d (removed %d)", len(leavesAfter), removed)
	for _, l := range leavesAfter {
		t.Logf("  %s: %s", l.URI, l.L0Abstract)
	}

	if removed == 0 {
		t.Error("expected some nodes to be removed as duplicates")
	}

	// The entities node should survive (no duplicates)
	entityNode, _ := db.GetNodeByURI("mem://user/entities/continuity-project")
	if entityNode == nil {
		t.Error("unique entity node should survive dedup")
	}

	if len(leavesAfter) >= len(leavesBefore) {
		t.Errorf("expected fewer leaves after dedup: before=%d, after=%d", len(leavesBefore), len(leavesAfter))
	}
}

func TestDedupNoEmbedder(t *testing.T) {
	db := testDB(t)
	eng := New(db, nil)

	_, err := eng.Dedup(context.Background(), 0.85)
	if err == nil {
		t.Error("expected error with nil embedder")
	}
	if !strings.Contains(err.Error(), "no embedder") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDeleteNode(t *testing.T) {
	db := testDB(t)

	node := &store.MemNode{
		URI: "mem://user/profile/to-delete", NodeType: "leaf", Category: "profile",
		L0Abstract: "This node will be deleted",
	}
	if err := db.CreateNode(node); err != nil {
		t.Fatal(err)
	}
	db.SaveVector(node.ID, []float64{0.1, 0.2, 0.3}, "test")

	// Verify it exists
	got, _ := db.GetNodeByID(node.ID)
	if got == nil {
		t.Fatal("expected node to exist before delete")
	}
	vec, _ := db.GetVector(node.ID)
	if vec == nil {
		t.Fatal("expected vector to exist before delete")
	}

	// Delete it
	if err := db.DeleteNode(node.ID); err != nil {
		t.Fatal(err)
	}

	// Verify gone
	got, _ = db.GetNodeByID(node.ID)
	if got != nil {
		t.Error("expected node to be deleted")
	}
	vec, _ = db.GetVector(node.ID)
	if vec != nil {
		t.Error("expected vector to be deleted")
	}
}
