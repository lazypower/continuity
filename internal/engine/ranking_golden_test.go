package engine

import (
	"context"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

// TestFindRankingMechanism_CategoryBoost is a tier-1 golden: it uses the
// deterministic stub embedder to pin the SCORING MECHANISM (not retrieval
// quality). Two nodes with identical abstracts embed to identical vectors, so
// their similarity and relevance are equal; the only differentiator is the
// moments category boost (1.3x). This locks the ranking math that was previously
// only inspectable by curling /api/search and reading it by hand.
func TestFindRankingMechanism_CategoryBoost(t *testing.T) {
	db := memTestDB(t)

	mk := func(uri, category, l0 string) {
		n := &store.MemNode{URI: uri, NodeType: "leaf", Category: category, L0Abstract: l0}
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("CreateNode %s: %v", uri, err)
		}
	}
	const text = "identical abstract text for both nodes"
	mk("mem://user/moments/m", "moments", text)
	mk("mem://agent/patterns/p", "patterns", text)

	emb := stubEmbedder{model: "toy", dims: 32}
	e := New(db, nil)
	e.SetEmbedder(emb)
	if _, err := e.ReconcileVectorIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.EmbedMissing(context.Background()); err != nil {
		t.Fatal(err)
	}

	res, err := Find(context.Background(), db, emb, text, SearchOpts{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("want 2 results, got %d", len(res))
	}
	if res[0].Node.Category != "moments" {
		t.Fatalf("moments boost must rank moments first; got %s (%.4f) then %s (%.4f)",
			res[0].Node.Category, res[0].Score, res[1].Node.Category, res[1].Score)
	}
	// Equal similarity and relevance, so the boosted score must be exactly 1.3x
	// the unboosted one (categoryBoost("moments") == 1.3).
	if got := res[0].Score / res[1].Score; got < 1.29 || got > 1.31 {
		t.Fatalf("category boost ratio = %.4f, want ~1.30", got)
	}
}
