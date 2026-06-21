package engine

import (
	"context"
	"math"
	"testing"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"Hello World", 2},
		{"Go developer, prefers minimal dependencies.", 5},
		{"a b c", 0}, // single chars skipped
		{"SQLite WAL mode", 3},
		{"", 0},
		{"wal-mode", 2},        // '-' is a separator → wal, mode
		{"555-123-4567", 3},    // hyphenated digits split → 555, 123, 4567
		{"snake_case_name", 3}, // '_' is a separator too
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

func TestHashEmbedder(t *testing.T) {
	emb, err := NewHashEmbedder(0)
	if err != nil {
		t.Fatalf("NewHashEmbedder: %v", err)
	}

	if emb.Model() != "hashtf" {
		t.Errorf("model = %q, want hashtf", emb.Model())
	}
	if emb.Dimensions() != defaultHashDims {
		t.Errorf("dims = %d, want %d", emb.Dimensions(), defaultHashDims)
	}

	ctx := context.Background()

	vec, err := emb.Embed(ctx, "Go developer minimal dependencies")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != emb.Dimensions() {
		t.Errorf("vec length = %d, want %d", len(vec), emb.Dimensions())
	}

	// Overlapping keywords → high cosine.
	related, _ := emb.Embed(ctx, "Go developer who prefers minimal dependencies")
	sim := CosineSimilarity(vec, related)
	if sim < 0.5 {
		t.Errorf("related cosine = %f, want > 0.5", sim)
	}

	// Disjoint keywords → lower cosine.
	unrelated, _ := emb.Embed(ctx, "Python machine learning tensorflow")
	if us := CosineSimilarity(vec, unrelated); us >= sim {
		t.Errorf("unrelated similarity %f should be less than related %f", us, sim)
	}
}

// TestHashEmbedderEmpty: text with no tokenizable terms embeds to an all-zero
// vector of the fixed dimension (and must not panic).
func TestHashEmbedderEmpty(t *testing.T) {
	emb, err := NewHashEmbedder(0)
	if err != nil {
		t.Fatalf("NewHashEmbedder: %v", err)
	}

	vec, err := emb.Embed(context.Background(), "  ?? !! ")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != emb.Dimensions() {
		t.Errorf("vec length = %d, want %d", len(vec), emb.Dimensions())
	}
	for i, v := range vec {
		if v != 0 {
			t.Fatalf("token-less text must embed to all-zero; vec[%d]=%f", i, v)
		}
	}
}

// TestHashEmbedderReformattedDigitsCollide pins the retraction-gate normalization
// fix (Codex finding): the same PII written with different digit-group separators
// must still land in the same buckets, so a reformatted re-write trips the gate.
// Before the tokenize fix, "555-123-4567" was one opaque token and "555 123 4567"
// was three, so the two barely overlapped and the gate missed identical PII.
func TestHashEmbedderReformattedDigitsCollide(t *testing.T) {
	ctx := context.Background()
	emb, _ := NewHashEmbedder(0)

	hyphen, _ := emb.Embed(ctx, "phone 555-123-4567")
	spaced, _ := emb.Embed(ctx, "phone 555 123 4567")
	if sim := CosineSimilarity(hyphen, spaced); sim < lexicalMatchThreshold {
		t.Errorf("reformatted phone cosine = %f, want >= gate threshold %f", sim, lexicalMatchThreshold)
	}

	ssnH, _ := emb.Embed(ctx, "ssn 123-45-6789")
	ssnS, _ := emb.Embed(ctx, "ssn 123 45 6789")
	if sim := CosineSimilarity(ssnH, ssnS); sim < lexicalMatchThreshold {
		t.Errorf("reformatted ssn cosine = %f, want >= gate threshold %f", sim, lexicalMatchThreshold)
	}
}

// TestHashEmbedderCorpusIndependent is the core regression test for the
// fixed-dimension feature-hashing fix. The legacy corpus-derived TF-IDF rebuilt
// its vocabulary (and thus its coordinate system) from the live corpus, so two
// embedders constructed at different corpus sizes embedded the SAME text into
// DIFFERENT vector spaces — silently defeating the retraction-resurrection gate,
// which compares a fresh candidate vector against stored vectors. The hashed
// embedder's coordinate system is fixed, so the same text always embeds to the
// same vector regardless of corpus, restarts, or — proven here — rare vocabulary
// no corpus has ever seen.
func TestHashEmbedderCorpusIndependent(t *testing.T) {
	ctx := context.Background()
	a, _ := NewHashEmbedder(0)
	b, _ := NewHashEmbedder(0)

	const text = "zebraqua quixotic glyphwerks distinctive unusual"
	va, _ := a.Embed(ctx, text)
	vb, _ := b.Embed(ctx, text)

	if len(va) != len(vb) {
		t.Fatalf("dimension mismatch between independent embedders: %d vs %d", len(va), len(vb))
	}
	for i := range va {
		if va[i] != vb[i] {
			t.Fatalf("independent embedders disagree at bucket %d (%f vs %f) — coordinate system is not stable", i, va[i], vb[i])
		}
	}

	// No OOV: rare vocabulary no corpus has ever indexed must still produce a
	// non-zero vector. Corpus-TF-IDF would drop every such term as out-of-vocab.
	var sumSquares float64
	for _, x := range va {
		sumSquares += x * x
	}
	if sumSquares == 0 {
		t.Error("rare vocabulary embedded to all-zero — feature hashing must never have OOV")
	}
}
