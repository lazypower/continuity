package engine

import (
	"context"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

// stubEmbedder is a deterministic, dependency-free embedder for tests. It owns a
// declared model/dims so we can exercise identity reconciliation without Ollama
// or a corpus-derived TF-IDF vocabulary.
type stubEmbedder struct {
	model string
	dims  int
}

func (s stubEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, s.dims)
	for i, r := range text {
		v[i%s.dims] += float64(r)
	}
	return v, nil
}
func (s stubEmbedder) Model() string   { return s.model }
func (s stubEmbedder) Dimensions() int { return s.dims }

func memTestDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedLeaf(t *testing.T, db *store.DB, uri, l0 string) int64 {
	t.Helper()
	n := &store.MemNode{URI: uri, NodeType: "leaf", Category: "patterns", L0Abstract: l0}
	if err := db.CreateNode(n); err != nil {
		t.Fatalf("CreateNode %s: %v", uri, err)
	}
	got, err := db.GetNodeByURI(uri)
	if err != nil || got == nil {
		t.Fatalf("GetNodeByURI %s: %v", uri, err)
	}
	return got.ID
}

func TestEmbedderIdentity(t *testing.T) {
	if got := EmbedderIdentity(stubEmbedder{model: "ollama:nomic-embed-text", dims: 768}); got != "ollama:nomic-embed-text:768" {
		t.Fatalf("identity = %q", got)
	}
	if got := EmbedderIdentity(nil); got != "" {
		t.Fatalf("nil embedder identity = %q, want empty", got)
	}
}

func TestReconcileFreshCorpusInitializes(t *testing.T) {
	db := memTestDB(t)
	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "tfidf", dims: 512})

	st, err := e.ReconcileVectorIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !st.Match {
		t.Fatalf("fresh corpus should match: %+v", st)
	}
	if id, ok, _ := db.VectorIdentity(); !ok || id != "tfidf:512" {
		t.Fatalf("identity not initialized: %q ok=%v", id, ok)
	}
	if locked, _ := e.VectorIdentityLocked(); locked {
		t.Fatal("fresh corpus must not lock")
	}
}

func TestReconcileBackfillsFromStoredVectorsAndMatches(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")
	if err := db.SaveVector(id, make([]float64, 768), "ollama:nomic-embed-text"); err != nil {
		t.Fatal(err)
	}

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "ollama:nomic-embed-text", dims: 768})

	st, err := e.ReconcileVectorIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !st.Match {
		t.Fatalf("active matching stored vectors should match: %+v", st)
	}
	if got, ok, _ := db.VectorIdentity(); !ok || got != "ollama:nomic-embed-text:768" {
		t.Fatalf("backfilled identity = %q ok=%v", got, ok)
	}
	if locked, _ := e.VectorIdentityLocked(); locked {
		t.Fatal("matching corpus must not lock")
	}
}

// TestReconcileMismatchLocks is the core regression: a server whose active
// embedder differs from the corpus's vector space must LOCK (search fails
// closed), must NOT silently re-embed, and must preserve the corpus's declared
// identity rather than overwriting it with the active embedder's.
func TestReconcileMismatchLocks(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")
	if err := db.SaveVector(id, make([]float64, 768), "ollama:nomic-embed-text"); err != nil {
		t.Fatal(err)
	}

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "tfidf", dims: 512}) // incompatible with stored nomic/768

	st, err := e.ReconcileVectorIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Match {
		t.Fatal("incompatible active embedder must not match")
	}
	locked, reason := e.VectorIdentityLocked()
	if !locked || reason == "" {
		t.Fatalf("mismatch must lock with a reason: locked=%v reason=%q", locked, reason)
	}

	// No silent re-embed while locked.
	n, err := e.EmbedMissing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("locked EmbedMissing must embed nothing, embedded %d", n)
	}
	// Declared identity stays the corpus's truth, not the active embedder.
	if got, _, _ := db.VectorIdentity(); got != "ollama:nomic-embed-text:768" {
		t.Fatalf("declared identity must remain corpus truth, got %q", got)
	}
}

// TestEmbedMissingDoesNotReembedStale pins the exact bug: a vector under an older
// model must never be silently overwritten by EmbedMissing.
func TestEmbedMissingDoesNotReembedStale(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")
	if err := db.SaveVector(id, make([]float64, 512), "old-model"); err != nil {
		t.Fatal(err)
	}

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "new-model", dims: 64})

	n, err := e.EmbedMissing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("must not re-embed a stale vector, embedded %d", n)
	}
	v, _ := db.GetVector(id)
	if v == nil || v.Model != "old-model" {
		t.Fatalf("stale vector was overwritten: %+v", v)
	}
}

// TestEmbedNodePendingWhileLocked verifies the Pending state: a write during an
// incompatible-embedder period must not embed into the foreign vector space —
// the node stays pending (no vector) for EmbedMissing to fill once compatible.
func TestEmbedNodePendingWhileLocked(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")
	if err := db.SaveVector(id, make([]float64, 768), "ollama:nomic-embed-text"); err != nil {
		t.Fatal(err)
	}
	newID := seedLeaf(t, db, "mem://agent/patterns/b", "beta")

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "tfidf", dims: 512}) // incompatible with nomic/768
	if _, err := e.ReconcileVectorIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}

	node, _ := db.GetNodeByURI("mem://agent/patterns/b")
	if err := e.EmbedNode(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.GetVector(newID); v != nil {
		t.Fatalf("locked EmbedNode must leave node pending (no vector), got %+v", v)
	}
}

func TestEmbedMissingFillsTrulyMissing(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "new-model", dims: 64})

	n, err := e.EmbedMissing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("should embed the 1 missing node, embedded %d", n)
	}
	v, _ := db.GetVector(id)
	if v == nil || v.Model != "new-model" || v.Dimensions != 64 {
		t.Fatalf("missing node not embedded correctly: %+v", v)
	}
}
