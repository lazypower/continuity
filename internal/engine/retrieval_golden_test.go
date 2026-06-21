package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/goldretrieval"
	"github.com/lazypower/continuity/internal/store"
)

// retrievalFixturePath resolves the golden: CONTINUITY_RETRIEVAL_FIXTURE when set
// (the scheduled drift canary points this at a freshly-minted file), else the
// committed golden — mirroring the migration fixtures' CONTINUITY_FIXTURE_DIR.
func retrievalFixturePath() string {
	if p := os.Getenv("CONTINUITY_RETRIEVAL_FIXTURE"); p != "" {
		return p
	}
	return filepath.Join("testdata", "retrieval", "nomic.json")
}

// These goldens replay REAL nomic-embed-text vectors (recorded into
// testdata/retrieval/nomic.json by scripts/genretrievalfixtures) through the
// REAL Find() path. They are hermetic — no Ollama at test time — and assert
// ranked-order PROPERTIES with score margins, not exact vectors. A failure after
// a fixture regen means current nomic ranks the corpus differently than recorded:
// a real embedder regression that would hit users too.
//
// Regenerate after changing the corpus/queries, or on the drift schedule:
//
//	make retrieval-fixtures   # needs Ollama + nomic-embed-text

// The nomic tier must actually be generated with nomic-embed-text at 768-d — a
// fixture minted with a different model would replay under its own matching
// identity and stay green while no longer proving the intended ranking.
const (
	expectNomicModel = "ollama:nomic-embed-text"
	expectNomicDims  = 768
)

func loadGoldenDB(t *testing.T) (*store.DB, *goldretrieval.ReplayEmbedder) {
	t.Helper()
	fx, err := goldretrieval.Load(retrievalFixturePath())
	if err != nil {
		t.Fatalf("load retrieval fixture (regenerate with `make retrieval-fixtures`): %v", err)
	}
	// Hard-fail on drift: if the corpus/queries changed without regenerating the
	// recorded vectors, the fixture no longer describes the definition under test.
	if fx.Fingerprint != goldretrieval.CorpusFingerprint() {
		t.Fatalf("retrieval fixture is stale — corpus/queries changed without regenerating; run `make retrieval-fixtures`")
	}
	// Bind the fixture to the intended embedder identity, so it can't be quietly
	// regenerated with a non-nomic model and still pass.
	if fx.Model != expectNomicModel || fx.Dims != expectNomicDims {
		t.Fatalf("nomic golden was generated with %s/%d, want %s/%d — regenerate with nomic-embed-text",
			fx.Model, fx.Dims, expectNomicModel, expectNomicDims)
	}

	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, e := range goldretrieval.Corpus() {
		n := &store.MemNode{URI: e.URI, NodeType: "leaf", Category: e.Category, L0Abstract: e.L0}
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("create %s: %v", e.URI, err)
		}
		got, err := db.GetNodeByURI(e.URI)
		if err != nil || got == nil {
			t.Fatalf("lookup %s: %v", e.URI, err)
		}
		vec, ok := fx.CorpusVecs[e.URI]
		if !ok {
			t.Fatalf("fixture missing vector for %s — regenerate", e.URI)
		}
		if err := db.SaveVector(got.ID, vec, fx.Model); err != nil {
			t.Fatalf("save vector %s: %v", e.URI, err)
		}
	}
	return db, fx.ReplayEmbedder()
}

func scoreByURI(res []engine.SearchResult) map[string]float64 {
	m := make(map[string]float64, len(res))
	for _, r := range res {
		m[r.Node.URI] = r.Score
	}
	return m
}

// TestRetrievalGolden_Nomic_TopicalQueries pins that a real semantic embedder
// surfaces the on-topic memory first, by a margin — including the "devbox" case
// the lexical fallback mis-ranks (devbox preference must beat go-sandbox-runtime).
func TestRetrievalGolden_Nomic_TopicalQueries(t *testing.T) {
	db, emb := loadGoldenDB(t)
	ctx := context.Background()

	for _, a := range goldretrieval.Assertions() {
		res, err := engine.Find(ctx, db, emb, a.Query, engine.SearchOpts{Limit: 10})
		if err != nil {
			t.Fatalf("find %q: %v", a.Query, err)
		}
		if len(res) == 0 {
			t.Errorf("query %q: no results", a.Query)
			continue
		}
		if res[0].Node.URI != a.Top {
			t.Errorf("query %q: top = %s (%.4f), want %s", a.Query, res[0].Node.URI, res[0].Score, a.Top)
			continue
		}

		scores := scoreByURI(res)
		gap := 0.0
		switch {
		case a.Above != "":
			aboveScore, ok := scores[a.Above]
			if !ok {
				// The distractor must actually be in the results to prove it was
				// outranked — a missing comparator must FAIL, not score as 0.
				t.Errorf("query %q: comparator %s absent from results — cannot verify it was outranked", a.Query, a.Above)
				continue
			}
			gap = scores[a.Top] - aboveScore
		case len(res) >= 2:
			gap = res[0].Score - res[1].Score
		}
		if gap < a.MinMargin {
			t.Errorf("query %q: margin %.4f < required %.4f (top %s)", a.Query, gap, a.MinMargin, a.Top)
		}
	}
}

// TestRetrievalGolden_HashLexical_SelfRetrieval exercises the REAL hashed lexical
// fallback path hermetically (pure Go, no fixture, no Ollama): embed every node,
// then confirm each retrieves itself at rank 1. Lexical ranking quality is weak
// by design (keyword overlap, not semantic), so this asserts the path WORKS, not
// that it ranks topical queries well — that's the nomic tier's job.
func TestRetrievalGolden_HashLexical_SelfRetrieval(t *testing.T) {
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, e := range goldretrieval.Corpus() {
		n := &store.MemNode{URI: e.URI, NodeType: "leaf", Category: e.Category, L0Abstract: e.L0}
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("create %s: %v", e.URI, err)
		}
	}

	// Construct the hashed lexical embedder (no corpus needed), embed each node.
	emb, err := engine.NewHashEmbedder(0)
	if err != nil {
		t.Fatalf("NewHashEmbedder: %v", err)
	}
	ctx := context.Background()
	for _, e := range goldretrieval.Corpus() {
		got, _ := db.GetNodeByURI(e.URI)
		vec, err := emb.Embed(ctx, e.L0)
		if err != nil {
			t.Fatalf("embed %s: %v", e.URI, err)
		}
		if err := db.SaveVector(got.ID, vec, emb.Model()); err != nil {
			t.Fatalf("save vector %s: %v", e.URI, err)
		}
	}

	for _, e := range goldretrieval.Corpus() {
		res, err := engine.Find(ctx, db, emb, e.L0, engine.SearchOpts{Limit: 5})
		if err != nil {
			t.Fatalf("self-retrieval %s: %v", e.URI, err)
		}
		got := "(none)"
		if len(res) > 0 {
			got = res[0].Node.URI
		}
		if got != e.URI {
			t.Errorf("hash-lexical self-retrieval %s: top = %s, want self", e.URI, got)
		}
	}
}

// TestRetrievalGolden_Nomic_SelfRetrieval pins that each memory retrieves itself
// at rank 1 from its own abstract — the basic coherence floor.
func TestRetrievalGolden_Nomic_SelfRetrieval(t *testing.T) {
	db, emb := loadGoldenDB(t)
	ctx := context.Background()

	for _, e := range goldretrieval.Corpus() {
		res, err := engine.Find(ctx, db, emb, e.L0, engine.SearchOpts{Limit: 5})
		if err != nil {
			t.Fatalf("self-retrieval %s: %v", e.URI, err)
		}
		got := "(none)"
		if len(res) > 0 {
			got = res[0].Node.URI
		}
		if got != e.URI {
			t.Errorf("self-retrieval %s: top = %s, want self", e.URI, got)
		}
	}
}
