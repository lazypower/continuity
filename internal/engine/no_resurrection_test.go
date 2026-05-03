package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// snapshotNode captures the full row state of a node for byte-equal comparison
// after running potentially-mutating paths. The retraction contract says
// retracted nodes are inert except via explicit URI inspection — every column
// must be unchanged after any non-inspection write path runs.
func snapshotNode(t *testing.T, db *store.DB, uri string) store.MemNode {
	t.Helper()
	n, err := db.GetNodeByURI(uri)
	if err != nil {
		t.Fatalf("snapshot %s: %v", uri, err)
	}
	if n == nil {
		t.Fatalf("snapshot %s: node not found", uri)
	}
	// Dereference the pointer fields so the snapshot doesn't share storage with
	// the live row. TombstonedAt is the only pointer field on MemNode aside
	// from LastAccess.
	snap := *n
	if n.TombstonedAt != nil {
		v := *n.TombstonedAt
		snap.TombstonedAt = &v
	}
	if n.LastAccess != nil {
		v := *n.LastAccess
		snap.LastAccess = &v
	}
	return snap
}

// assertNoResurrection compares a fresh read of `uri` against `before` and
// fails if any field has changed. Equality is structural — two pointer fields
// are considered equal if their pointed-to values match.
func assertNoResurrection(t *testing.T, db *store.DB, uri string, before store.MemNode) {
	t.Helper()
	after, err := db.GetNodeByURI(uri)
	if err != nil {
		t.Fatalf("read after: %v", err)
	}
	if after == nil {
		t.Fatalf("retracted node %s disappeared (this is also a violation)", uri)
	}

	// Compare value fields directly.
	if !nodesEqualByValue(before, *after) {
		t.Errorf("retracted node mutated after a write path ran:\n  before: %+v\n  after:  %+v",
			fmtNode(before), fmtNode(*after))
	}
}

func nodesEqualByValue(a, b store.MemNode) bool {
	// All scalar fields must match.
	if a.ID != b.ID || a.URI != b.URI || a.ParentURI != b.ParentURI ||
		a.NodeType != b.NodeType || a.Category != b.Category ||
		a.L0Abstract != b.L0Abstract || a.L1Overview != b.L1Overview ||
		a.L2Content != b.L2Content || a.Mergeable != b.Mergeable ||
		a.MergedFrom != b.MergedFrom || a.Relevance != b.Relevance ||
		a.AccessCount != b.AccessCount || a.SourceSession != b.SourceSession ||
		a.CreatedAt != b.CreatedAt || a.UpdatedAt != b.UpdatedAt ||
		a.TombstoneReason != b.TombstoneReason || a.SupersededBy != b.SupersededBy {
		return false
	}
	// Pointer fields: equal if both nil or both non-nil with same pointed value.
	if !pointerEqualInt64(a.TombstonedAt, b.TombstonedAt) {
		return false
	}
	if !pointerEqualInt64(a.LastAccess, b.LastAccess) {
		return false
	}
	return true
}

func pointerEqualInt64(a, b *int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func fmtNode(n store.MemNode) string {
	tombstoned := "<nil>"
	if n.TombstonedAt != nil {
		tombstoned = fmt.Sprintf("%d", *n.TombstonedAt)
	}
	return fmt.Sprintf("URI=%s L0=%q L1=%q TombstonedAt=%s Reason=%q SupersededBy=%q UpdatedAt=%d",
		n.URI, n.L0Abstract, n.L1Overview, tombstoned, n.TombstoneReason, n.SupersededBy, n.UpdatedAt)
}

// TestNoResurrection_FindSimilarNodeDoesNotReturnRetracted is the unit-level
// guard. findSimilarNode is called by the LLM extraction path; if it returns
// a retracted node, the caller will merge new content into the retracted URI
// and effectively un-retract it.
func TestNoResurrection_FindSimilarNodeDoesNotReturnRetracted(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// Seed a node and embed it, then retract it.
	n := &store.MemNode{
		URI: "mem://user/preferences/retracted-pref", NodeType: "leaf", Category: "preferences",
		L0Abstract: "Prefers minimal dependencies and standard library where possible",
		L1Overview: "Body content here for validation thresholds and assertions to land.",
	}
	if err := db.CreateNode(n); err != nil {
		t.Fatal(err)
	}
	embedder, _ := NewTFIDFEmbedder(db, 512)
	vec, _ := embedder.Embed(ctx, n.L0Abstract)
	db.SaveVector(n.ID, vec, embedder.Model())
	if _, err := db.RetractNode(n.URI, "test retraction", ""); err != nil {
		t.Fatal(err)
	}

	// Search for a semantically very similar candidate. findSimilarNode must
	// NOT return the retracted node — even though it's the closest match.
	match, sim, err := findSimilarNode(ctx, db, embedder,
		"Prefers minimal dependencies and standard library where possible", "preferences", 0.5)
	if err != nil {
		t.Fatal(err)
	}
	if match != nil && match.URI == n.URI {
		t.Errorf("findSimilarNode returned the retracted node (sim=%.3f); merging into it would resurrect it", sim)
	}
}

// TestNoResurrection_ExtractMemoriesDoesNotMutateRetracted exercises the
// full extractMemories path. With a retracted node already in the DB, an
// LLM-extracted candidate that's semantically close must NOT mutate the
// retracted row — not its content, not its metadata, not its tombstone state.
//
// Pre-fix this test fails: findSimilarNode returns the retracted node, the
// extractor merges new L0/L1/L2 into it via UpsertNode → UpdateNode, and the
// retracted row's content is silently overwritten while its tombstone stays.
func TestNoResurrection_ExtractMemoriesDoesNotMutateRetracted(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	n := &store.MemNode{
		URI: "mem://user/preferences/minimal-deps", NodeType: "leaf", Category: "preferences",
		L0Abstract: "Prefers minimal dependencies, standard library where possible",
		L1Overview: "ORIGINAL body content with enough length to pass validation thresholds.",
	}
	if err := db.CreateNode(n); err != nil {
		t.Fatal(err)
	}
	embedder, _ := NewTFIDFEmbedder(db, 512)
	vec, _ := embedder.Embed(ctx, n.L0Abstract)
	db.SaveVector(n.ID, vec, embedder.Model())

	if _, err := db.RetractNode(n.URI, "operator decided this preference was wrong", ""); err != nil {
		t.Fatal(err)
	}

	before := snapshotNode(t, db, n.URI)

	// LLM produces a candidate semantically similar to the retracted node.
	extractionResponse := `[
		{
			"category": "preferences",
			"uri_hint": "minimal-dependencies-preference",
			"l0": "Prefers minimal dependencies, standard library where possible",
			"l1": "RESURRECTED body content that should never reach the retracted row.",
			"l2": "Full details from the new extraction"
		}
	]`
	mock := &llm.MockClient{
		Response: &llm.Response{Content: extractionResponse, Provider: "mock"},
	}

	transcriptPath := makeTranscript(t)
	if err := extractMemories(db, mock, embedder, "test-session", transcriptPath); err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	// Full-row equality — every column on the retracted node must be unchanged.
	assertNoResurrection(t, db, n.URI, before)
}

// TestNoResurrection_RememberDoesNotMutateRetracted is the equivalent guard
// for the public Remember API. The dedup-against-retracted gate already blocks
// matching writes, but the URI-collision branch and the regular merge path
// could in principle mutate a retracted row. This test pins both paths.
func TestNoResurrection_RememberDoesNotMutateRetracted(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)
	ctx := context.Background()

	uri := seedAndEmbed(t, eng, "preferences", "doomed-pref",
		"original preference content for testing the no-resurrection guard",
		"ORIGINAL body content with enough length to pass validation thresholds.")
	embedder, _ := NewTFIDFEmbedder(db, 512)
	eng.SetEmbedder(embedder)
	n, _ := db.GetNodeByURI(uri)
	if err := eng.EmbedNode(ctx, n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(uri, "test retraction", ""); err != nil {
		t.Fatal(err)
	}

	before := snapshotNode(t, db, uri)

	// Path 1: write to the same URI — must error, must NOT mutate.
	_, _, err := eng.Remember(ctx, RememberInput{
		Category: "preferences", Name: "doomed-pref",
		Summary:              "different summary text but same URI to test collision",
		Body:                 "Different body content with enough length to pass validation thresholds.",
		AcknowledgeRetracted: true, // even with override, URI-collision path must hold
	})
	if err == nil {
		t.Error("Remember at retracted URI should error, did not")
	}
	assertNoResurrection(t, db, uri, before)

	// Path 2: write to a different URI with similar content — must trigger
	// the dedup-against-retracted gate (no row mutation possible since the gate
	// fires before any write).
	_, _, err = eng.Remember(ctx, RememberInput{
		Category: "preferences", Name: "different-slug",
		Summary: "original preference content for testing the no-resurrection guard",
		Body:    "Different body content with enough length to pass validation thresholds.",
	})
	if err == nil {
		t.Error("Remember with similar content should hit dedup-against-retracted gate, did not")
	}
	assertNoResurrection(t, db, uri, before)
}
