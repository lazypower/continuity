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
	if id, ok, _ := db.VectorIdentity(); !ok || id != "tfidf" {
		t.Fatalf("identity not initialized (tfidf is canonical model-only): %q ok=%v", id, ok)
	}
	if locked, _ := e.VectorIdentityLocked(); locked {
		t.Fatal("fresh corpus must not lock")
	}
}

// TestReconcileTFIDFStableAcrossDimChange pins Codex finding #1: TF-IDF's
// corpus-derived dimension must not self-lock the fallback. Stored tfidf at one
// dim + active tfidf at another must MATCH (canonical identity is model-only).
func TestReconcileTFIDFStableAcrossDimChange(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")
	if err := db.SaveVector(id, make([]float64, 300), "tfidf"); err != nil {
		t.Fatal(err)
	}

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "tfidf", dims: 400}) // different dim, same embedder

	st, err := e.ReconcileVectorIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !st.Match {
		t.Fatalf("tfidf must not self-lock on dimension change: %+v", st)
	}
	if locked, _ := e.VectorIdentityLocked(); locked {
		t.Fatal("tfidf dim change must not lock")
	}
	if got, _, _ := db.VectorIdentity(); got != "tfidf" {
		t.Fatalf("tfidf identity must be model-only, got %q", got)
	}
}

// TestReconcileMixedCorpusLocks pins Codex finding #4: a pre-identity corpus with
// MULTIPLE stored identities must fail closed, not silently bless a majority.
func TestReconcileMixedCorpusLocks(t *testing.T) {
	db := memTestDB(t)
	a := seedLeaf(t, db, "mem://agent/patterns/a", "alpha")
	b := seedLeaf(t, db, "mem://agent/patterns/b", "beta")
	if err := db.SaveVector(a, make([]float64, 768), "ollama:nomic-embed-text"); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveVector(b, make([]float64, 512), "old-model"); err != nil {
		t.Fatal(err)
	}

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "ollama:nomic-embed-text", dims: 768})

	st, err := e.ReconcileVectorIdentity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Match {
		t.Fatal("mixed corpus must not match")
	}
	if locked, reason := e.VectorIdentityLocked(); !locked || reason == "" {
		t.Fatalf("mixed corpus must lock with a reason: locked=%v", locked)
	}
	if _, ok, _ := db.VectorIdentity(); ok {
		t.Fatal("mixed corpus must not auto-bind any identity")
	}
}

// TestFindSkipsForeignIdentityVectors pins Codex finding #5: even after the lock
// passes, a stored vector under a foreign identity must not be scored — here a
// foreign vector identical to the query (cosine 1.0) must still be skipped.
func TestFindSkipsForeignIdentityVectors(t *testing.T) {
	db := memTestDB(t)
	good := seedLeaf(t, db, "mem://agent/patterns/good", "hello world")
	bad := seedLeaf(t, db, "mem://agent/patterns/bad", "hello world")

	emb := stubEmbedder{model: "active", dims: 8}
	vec, _ := emb.Embed(context.Background(), "hello world")
	if err := db.SaveVector(good, vec, "active"); err != nil {
		t.Fatal(err)
	}
	// Same vector, FOREIGN model — high cosine, but must be excluded from scoring.
	if err := db.SaveVector(bad, vec, "foreign"); err != nil {
		t.Fatal(err)
	}

	res, err := Find(context.Background(), db, emb, "hello world", SearchOpts{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Node.URI != "mem://agent/patterns/good" {
		t.Fatalf("expected only the active-identity node, got %+v", res)
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

// TestFindRetractedMatchesSkipsWhenLocked pins Codex round-2 finding: the
// retracted-resurrection safety gate must not compare across vector spaces while
// the identity is locked — it skips (like no embedder) rather than scanning.
func TestFindRetractedMatchesSkipsWhenLocked(t *testing.T) {
	db := memTestDB(t)
	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "active", dims: 8})
	e.identityMismatch = true // locked

	matches, err := e.findRetractedMatches(context.Background(), "some candidate text", "patterns", 0.5)
	if err != nil {
		t.Fatal(err)
	}
	if matches != nil {
		t.Fatalf("locked retract gate must return no matches, got %d", len(matches))
	}
}

// TestRememberFailsClosedWhenLocked pins Codex round-3: while the identity is
// locked the retracted-PII gate can't run, so an unacknowledged write must be
// REFUSED (not silently allowed); --acknowledge-retracted still overrides.
func TestRememberFailsClosedWhenLocked(t *testing.T) {
	db := memTestDB(t)
	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "active", dims: 8})
	e.identityMismatch = true // locked

	const body = "a sufficiently long overview body for validation"
	if _, _, err := e.Remember(context.Background(), RememberInput{
		Category: "patterns", Name: "alpha", Summary: "hello world", Body: body,
	}); err == nil {
		t.Fatal("locked Remember without --acknowledge-retracted must refuse")
	}

	uri, _, err := e.Remember(context.Background(), RememberInput{
		Category: "patterns", Name: "beta", Summary: "hello world", Body: body, AcknowledgeRetracted: true,
	})
	if err != nil {
		t.Fatalf("acknowledged locked Remember should proceed: %v", err)
	}
	// It must have stayed Pending — no vector written into the foreign space.
	stored, _ := db.GetNodeByURI(uri)
	if stored != nil {
		if v, _ := db.GetVector(stored.ID); v != nil {
			t.Fatalf("locked Remember must leave node Pending, got vector %+v", v)
		}
	}
}

// TestEmbedNodeClearsStaleVectorWhenLocked pins Codex round-5: when a content
// update happens while locked, the OLD vector must be dropped (not left in place)
// so search can't serve a vector for the previous content after the embedder
// returns. EmbedMissing only fills MISSING vectors, so a survivor would be stale.
func TestEmbedNodeClearsStaleVectorWhenLocked(t *testing.T) {
	db := memTestDB(t)
	id := seedLeaf(t, db, "mem://agent/patterns/a", "updated content")
	if err := db.SaveVector(id, make([]float64, 8), "active"); err != nil { // vector for OLD content
		t.Fatal(err)
	}

	e := New(db, nil)
	e.SetEmbedder(stubEmbedder{model: "active", dims: 8})
	e.identityMismatch = true // locked

	node, _ := db.GetNodeByURI("mem://agent/patterns/a")
	if err := e.EmbedNode(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.GetVector(id); v != nil {
		t.Fatalf("locked EmbedNode must clear the stale vector, got %+v", v)
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
