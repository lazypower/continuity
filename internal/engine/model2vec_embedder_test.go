package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// parityFixture is one battery entry: the token ids (POST-[UNK]-filter, i.e.
// exactly what model2vec.StaticModel.tokenize() returns — that method already
// strips [UNK] before returning, see scripts/gen_model2vec_parity.py) and the
// L2-normalized mean-pooled embedding vector, both produced by the real
// Python model2vec package against minishlab/potion-retrieval-32M.
type parityFixture struct {
	IDs []int32   `json:"ids"`
	Vec []float64 `json:"vec"`
}

func loadParityFixture(t *testing.T) map[string]parityFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "model2vec", "parity.json"))
	if err != nil {
		t.Fatalf("read parity fixture: %v", err)
	}
	var fixture map[string]parityFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse parity fixture: %v", err)
	}
	if len(fixture) == 0 {
		t.Fatal("parity fixture is empty")
	}
	return fixture
}

// model2vecTestModelDir resolves where this test loads potion-retrieval-32M
// model files from. HERMETICITY ASSUMPTION (documented per the task spec):
// this test does not perform network I/O and does not shell out to Python — it
// only reads model.safetensors + tokenizer.json off disk. Those files are
// ~32MB (int8-quantized safetensors) and ~1MB (tokenizer.json), far too large
// to commit as a repo fixture, so rather than vendoring them into testdata/
// this test reads them from the SAME default location the production code uses
// (DefaultModel2VecDir(), i.e. ~/.continuity/models/potion-retrieval-32M/),
// which can be overridden via CONTINUITY_MODEL2VEC_TEST_DIR for CI images that
// pre-seed the model elsewhere. If the files are not present, the test SKIPS
// (not fails) with instructions — it never silently passes without checking
// parity, and it never requires network/python AT TEST TIME (only at
// model-acquisition time, same as any other cached model asset).
func model2vecTestModelDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("CONTINUITY_MODEL2VEC_TEST_DIR"); dir != "" {
		return dir
	}
	dir, err := DefaultModel2VecDir()
	if err != nil {
		t.Fatalf("resolve default model2vec dir: %v", err)
	}
	return dir
}

func loadTestEmbedder(t *testing.T) *Model2VecEmbedder {
	t.Helper()
	dir := model2vecTestModelDir(t)
	if !fileExists(filepath.Join(dir, "model.safetensors")) || !fileExists(filepath.Join(dir, "tokenizer.json")) {
		t.Skipf("model2vec model files not present at %s; run `continuity` with CONTINUITY_EMBEDDER=model2vec once "+
			"(or manually fetch minishlab/potion-retrieval-32M's model.safetensors + tokenizer.json into this dir) "+
			"to exercise the parity test — skipping rather than failing since this test must not require network", dir)
	}
	emb, err := LoadModel2VecEmbedder(dir)
	if err != nil {
		t.Fatalf("LoadModel2VecEmbedder(%s): %v", dir, err)
	}
	return emb
}

// TestModel2VecParity is the make-or-break gate: for every text in the
// committed battery fixture, the Go tokenizer's post-UNK-filter token ids
// must EXACTLY equal the real model2vec package's ids, and the resulting
// embedding vector must match at cosine >= 0.9999 (allowing only for float32
// summation-order/library floating point noise, not algorithmic drift).
func TestModel2VecParity(t *testing.T) {
	emb := loadTestEmbedder(t)
	fixture := loadParityFixture(t)
	ctx := context.Background()

	for text, want := range fixture {
		t.Run(safeTestName(text), func(t *testing.T) {
			gotIDs := postFilterIDs(emb, text)
			if !int32SlicesEqual(gotIDs, want.IDs) {
				t.Fatalf("token id mismatch for %q:\n  got:  %v\n  want: %v", text, gotIDs, want.IDs)
			}

			gotVec, err := emb.Embed(ctx, text)
			if err != nil {
				t.Fatalf("Embed(%q): %v", text, err)
			}
			if len(gotVec) != len(want.Vec) {
				t.Fatalf("dimension mismatch for %q: got %d, want %d", text, len(gotVec), len(want.Vec))
			}

			if len(want.IDs) == 0 {
				// Degenerate input: both sides should be the zero vector; cosine
				// is undefined (0/0), so compare directly instead.
				for i, v := range gotVec {
					if v != 0 {
						t.Fatalf("expected zero vector for degenerate input %q, got nonzero at %d: %v", text, i, v)
					}
				}
				return
			}

			sim := CosineSimilarity(gotVec, want.Vec)
			t.Logf("cosine similarity for %q = %.10f (delta from 1.0 = %.2e)", text, sim, 1.0-sim)
			if sim < 0.9999 {
				t.Fatalf("cosine similarity for %q = %.10f, want >= 0.9999", text, sim)
			}
		})
	}
}

// postFilterIDs runs the tokenizer and drops UNK ids, matching what
// model2vec.StaticModel.tokenize() returns (it filters UNK before returning).
func postFilterIDs(emb *Model2VecEmbedder, text string) []int32 {
	raw := emb.tokenizer.Encode(text)
	out := make([]int32, 0, len(raw))
	for _, id := range raw {
		if id == emb.tokenizer.unkID {
			continue
		}
		out = append(out, id)
	}
	return out
}

func int32SlicesEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// safeTestName turns a battery text into a stable subtest name (Go's testing
// package escapes spaces etc. itself, but empty/whitespace-only strings need
// an explicit label to stay distinguishable in test output).
func safeTestName(s string) string {
	if s == "" {
		return "empty"
	}
	allSpace := true
	for _, r := range s {
		if r != ' ' && r != '\t' {
			allSpace = false
			break
		}
	}
	if allSpace {
		return "whitespace_only"
	}
	return s
}

// TestModel2VecEmbedder_Model checks the identity string composition, which
// is what flows into EmbedderIdentity/canonicalIdentity for vector identity
// binding — this is a plain fixed string, so it's tested without needing the
// model files at all.
func TestModel2VecEmbedder_Model(t *testing.T) {
	emb := &Model2VecEmbedder{dims: 512, modelName: Model2VecModelName}
	if got, want := emb.Model(), "model2vec:potion-retrieval-32M"; got != want {
		t.Errorf("Model() = %q, want %q", got, want)
	}
	if got, want := EmbedderIdentity(emb), "model2vec:potion-retrieval-32M:512"; got != want {
		t.Errorf("EmbedderIdentity() = %q, want %q", got, want)
	}
}

// TestModel2VecEmbedder_MatchThreshold pins that model2vec is treated as
// semantic (defaultSimilarityThreshold), not lexical — MatchThreshold only
// special-cases Model()=="hashtf".
func TestModel2VecEmbedder_MatchThreshold(t *testing.T) {
	emb := &Model2VecEmbedder{dims: 512, modelName: Model2VecModelName}
	if got := MatchThreshold(emb); got != defaultSimilarityThreshold {
		t.Errorf("MatchThreshold(model2vec) = %f, want defaultSimilarityThreshold %f", got, defaultSimilarityThreshold)
	}
}

// TestModel2VecEmbedder_DegenerateInput pins that empty/whitespace/pure
// punctuation inputs embed to an all-zero vector of the model dimension
// rather than erroring or panicking, matching the fixture's own zero-id cases.
func TestModel2VecEmbedder_DegenerateInput(t *testing.T) {
	emb := loadTestEmbedder(t)
	ctx := context.Background()

	for _, text := range []string{"", "   ", "\t\n"} {
		vec, err := emb.Embed(ctx, text)
		if err != nil {
			t.Fatalf("Embed(%q): %v", text, err)
		}
		if len(vec) != emb.Dimensions() {
			t.Fatalf("Embed(%q) length = %d, want %d", text, len(vec), emb.Dimensions())
		}
		for i, v := range vec {
			if v != 0 {
				t.Fatalf("Embed(%q)[%d] = %f, want 0 (degenerate input must be all-zero)", text, i, v)
			}
		}
	}
}
