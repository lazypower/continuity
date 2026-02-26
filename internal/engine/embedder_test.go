package engine

import (
	"context"
	"math"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"Hello World", 2},
		{"Go developer, prefers minimal dependencies.", 5},
		{"a b c", 0},       // single chars skipped
		{"SQLite WAL mode", 3},
		{"", 0},
	}

	for _, tt := range tests {
		tokens := tokenize(tt.input)
		if len(tokens) != tt.want {
			t.Errorf("tokenize(%q) = %d tokens %v, want %d", tt.input, len(tokens), tokens, tt.want)
		}
	}
}

func TestNormalize(t *testing.T) {
	vec := []float64{3, 4}
	normalize(vec)

	expected := 1.0
	norm := math.Sqrt(vec[0]*vec[0] + vec[1]*vec[1])
	if math.Abs(norm-expected) > 1e-10 {
		t.Errorf("normalized magnitude = %f, want %f", norm, expected)
	}
}

func TestNormalizeZero(t *testing.T) {
	vec := []float64{0, 0, 0}
	normalize(vec) // should not panic
	for i, v := range vec {
		if v != 0 {
			t.Errorf("vec[%d] = %f, want 0", i, v)
		}
	}
}

func TestCosineSimilarity(t *testing.T) {
	// Identical vectors
	a := []float64{1, 0, 0}
	b := []float64{1, 0, 0}
	sim := CosineSimilarity(a, b)
	if math.Abs(sim-1.0) > 1e-10 {
		t.Errorf("identical vectors similarity = %f, want 1.0", sim)
	}

	// Orthogonal vectors
	c := []float64{1, 0}
	d := []float64{0, 1}
	sim = CosineSimilarity(c, d)
	if math.Abs(sim) > 1e-10 {
		t.Errorf("orthogonal vectors similarity = %f, want 0.0", sim)
	}

	// Opposite vectors
	e := []float64{1, 0}
	f := []float64{-1, 0}
	sim = CosineSimilarity(e, f)
	if math.Abs(sim-(-1.0)) > 1e-10 {
		t.Errorf("opposite vectors similarity = %f, want -1.0", sim)
	}

	// Different lengths
	sim = CosineSimilarity([]float64{1}, []float64{1, 2})
	if sim != 0 {
		t.Errorf("mismatched lengths = %f, want 0", sim)
	}

	// Empty
	sim = CosineSimilarity([]float64{}, []float64{})
	if sim != 0 {
		t.Errorf("empty vectors = %f, want 0", sim)
	}
}

func TestTFIDFEmbedder(t *testing.T) {
	db := testDB(t)

	// Seed some nodes
	db.CreateNode(&store.MemNode{
		URI: "mem://user/profile/go-dev", NodeType: "leaf", Category: "profile",
		L0Abstract: "Go developer who prefers minimal dependencies",
	})
	db.CreateNode(&store.MemNode{
		URI: "mem://user/profile/sqlite", NodeType: "leaf", Category: "profile",
		L0Abstract: "Uses SQLite with WAL mode for concurrent reads",
	})
	db.CreateNode(&store.MemNode{
		URI: "mem://agent/patterns/error-handling", NodeType: "leaf", Category: "patterns",
		L0Abstract: "Pattern: graceful error handling with Go error wrapping",
	})

	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("NewTFIDFEmbedder: %v", err)
	}

	if embedder.Model() != "tfidf" {
		t.Errorf("model = %q, want tfidf", embedder.Model())
	}

	ctx := context.Background()

	// Embed a query related to Go
	vec, err := embedder.Embed(ctx, "Go developer minimal dependencies")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != embedder.Dimensions() {
		t.Errorf("vec length = %d, want %d", len(vec), embedder.Dimensions())
	}

	// Embed original node text — should have high similarity
	nodeVec, _ := embedder.Embed(ctx, "Go developer who prefers minimal dependencies")
	sim := CosineSimilarity(vec, nodeVec)
	if sim < 0.5 {
		t.Errorf("similar text cosine = %f, want > 0.5", sim)
	}

	// Embed unrelated text — should have lower similarity
	unrelatedVec, _ := embedder.Embed(ctx, "Python machine learning tensorflow")
	unrelatedSim := CosineSimilarity(vec, unrelatedVec)
	if unrelatedSim >= sim {
		t.Errorf("unrelated similarity %f should be less than related %f", unrelatedSim, sim)
	}
}

func TestTFIDFEmbedderEmpty(t *testing.T) {
	db := testDB(t)

	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("NewTFIDFEmbedder: %v", err)
	}

	// Should still work with no data
	vec, err := embedder.Embed(context.Background(), "test query")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != embedder.Dimensions() {
		t.Errorf("vec length = %d, want %d", len(vec), embedder.Dimensions())
	}
}
