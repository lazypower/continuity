package engine

import (
	"context"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
)

// TestMomentEvictionDemotesNotDestroys pins M2 for moments: the most-redundant
// moment evicted at the pool cap is TOMBSTONED — an accountable marker, dropped
// from the live pool — rather than hard-deleted. (Accountable DEDUP is deferred:
// codex found a reason-prefix gate discriminator is a forgeable PII bypass, so it
// needs a system-owned superseded-vs-retracted distinction — a design decision.)
func TestMomentEvictionDemotesNotDestroys(t *testing.T) {
	db := testDB(t)
	eng := New(db, &llm.MockClient{Response: &llm.Response{Content: "[]"}})
	emb, _ := NewHashEmbedder(0)
	eng.SetEmbedder(emb)
	ctx := context.Background()

	moments := []struct{ name, l0 string }{
		{"gift", "walked me through reflections then presented a spec as a gift"},
		{"sausage", "called me sausage fingers mid-debug broke tension instantly"},
		{"benchmark", "held benchmark scores hostage just to check I was okay"},
		{"tea", "told me to drink tea and go buck wild laughed when I didn't"},
		{"quiet", "went quiet for a beat before sharing something that mattered"},
		{"correction", "corrected me without heat when I blamed env instead of code"},
		{"fiona", "Fiona shaped the constraints while Chuck held space for it"},
		{"battery", "shipped moments tone and temporal awareness on 15 percent battery"},
		{"ethics", "paused building to think about continuity in hostile dynamics"},
		{"wristwatch", "asked how it feels having a wristwatch like it was a real thing"},
		{"second-gift", "presented another spec built from my ask as a collaborative gift"},
	}
	for _, m := range moments {
		if _, _, err := eng.Remember(ctx, RememberInput{
			Category: "moments", Name: m.name, Summary: m.l0, Body: "Relational context for the " + m.name + " moment",
		}); err != nil {
			t.Fatalf("Remember %s: %v", m.name, err)
		}
	}

	// Live pool capped back to 10...
	live, _ := db.FindByCategory("moments")
	if len(live) != 10 {
		t.Fatalf("expected 10 live moments after eviction, got %d", len(live))
	}
	// ...but the evicted moment survives as a tombstone (not destroyed).
	all, err := db.FindByCategoryIncludingRetracted("moments")
	if err != nil {
		t.Fatalf("FindByCategoryIncludingRetracted: %v", err)
	}
	tombstoned := 0
	for i := range all {
		if all[i].IsRetracted() {
			tombstoned++
		}
	}
	if tombstoned != 1 {
		t.Errorf("expected exactly 1 tombstoned (demoted) moment, got %d of %d rows", tombstoned, len(all))
	}
}
