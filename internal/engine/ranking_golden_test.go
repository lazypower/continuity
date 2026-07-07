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

// TestFindRankingMechanism_RelevanceIndependence pins the INVERSE of the
// invariant this test used to pin. Under ADR-001's interim score
// (similarity × categoryBoost), relevance must NOT move a find-mode score:
// similarity × relevance buries a scarred memory at similarity 0.9 under
// fresh noise at 0.15 once decay runs honestly (cross-cohort inversion).
// Two nodes with identical text and category, wildly different relevance —
// identical scores, or the relevance term has crept back into Find.
// (History: the original test pinned the relevance term IN; Codex flagged
// the pure-cosine golden blind spot. Same blind spot, opposite contract.)
func TestFindRankingMechanism_RelevanceIndependence(t *testing.T) {
	db := memTestDB(t)
	mk := func(uri string) {
		n := &store.MemNode{URI: uri, NodeType: "leaf", Category: "patterns", L0Abstract: "identical abstract for both nodes"}
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("create %s: %v", uri, err)
		}
	}
	mk("mem://agent/patterns/fresh")
	mk("mem://agent/patterns/faded")
	// Drop one node's relevance (CreateNode forces 1.0); only this differs.
	if _, err := db.Exec("UPDATE mem_nodes SET relevance = 0.3 WHERE uri = ?", "mem://agent/patterns/faded"); err != nil {
		t.Fatal(err)
	}

	emb := stubEmbedder{model: "toy", dims: 32}
	e := New(db, nil)
	e.SetEmbedder(emb)
	if _, err := e.ReconcileVectorIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.EmbedMissing(context.Background()); err != nil {
		t.Fatal(err)
	}

	res, err := Find(context.Background(), db, emb, "identical abstract for both nodes", SearchOpts{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("want 2 results, got %d", len(res))
	}
	// Identical similarity and category ⇒ identical scores. Any gap is the
	// relevance term creeping back into the interim formula.
	if ratio := res[1].Score / res[0].Score; ratio < 0.9999 || ratio > 1.0001 {
		t.Fatalf("score ratio = %.4f, want 1.0 — relevance must not move a find score (ADR-001 interim)", ratio)
	}
	// Relevance still rides the result as metadata for inspection (--explain).
	var faded *SearchResult
	for i := range res {
		if res[i].Node.URI == "mem://agent/patterns/faded" {
			faded = &res[i]
		}
	}
	if faded == nil {
		t.Fatal("faded node missing from results")
	}
	if faded.Node.Relevance != 0.3 {
		t.Errorf("faded relevance metadata = %f, want 0.3 (decay state must survive as metadata)", faded.Node.Relevance)
	}
}
