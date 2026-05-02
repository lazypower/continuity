package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
)

func TestHashURI_StableAndOpaque(t *testing.T) {
	uri1 := "mem://user/events/foo"
	uri2 := "mem://user/events/bar"

	h1a := hashURI(uri1)
	h1b := hashURI(uri1)
	h2 := hashURI(uri2)

	if h1a != h1b {
		t.Errorf("hashURI not deterministic: %q vs %q", h1a, h1b)
	}
	if h1a == h2 {
		t.Errorf("different URIs produced same hash: %q", h1a)
	}
	if len(h1a) != 16 {
		t.Errorf("hash length = %d, want 16 hex chars", len(h1a))
	}
	// Hash should not echo any structural prefix from the URI.
	if strings.Contains(h1a, "user") || strings.Contains(h1a, "events") || strings.Contains(h1a, "mem") {
		t.Errorf("hash leaks URI structure: %q", h1a)
	}
}

// seedAndEmbed inserts a leaf node and embeds it, building the embedder from
// the post-seed corpus so TFIDF actually has signal. Returns the URI written.
func seedAndEmbed(t *testing.T, eng *Engine, category, name, summary, body string) string {
	t.Helper()
	uri, _, err := eng.Remember(context.Background(), RememberInput{
		Category: category, Name: name,
		Summary: summary, Body: body,
		AcknowledgeRetracted: true, // skip dedup gate during seeding
	})
	if err != nil {
		t.Fatalf("seed %s/%s: %v", category, name, err)
	}
	return uri
}

func TestFindRetractedMatches_FindsRetractedSkipsLive(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)
	ctx := context.Background()

	// Seed: one live, one retracted, both with overlapping wording.
	live := seedAndEmbed(t, eng, "events", "live-event",
		"Captured user's full home address by accident in conversation",
		"Live memory body content with enough length to pass validation.")
	retracted := seedAndEmbed(t, eng, "events", "retracted-event",
		"Captured user's full home address by mistake during a session",
		"Retracted memory body content with enough length to pass validation.")

	// Build embedder AFTER seeding so the TFIDF corpus is populated. Re-embed.
	embedder, err := NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetEmbedder(embedder)
	for _, uri := range []string{live, retracted} {
		n, _ := db.GetNodeByURI(uri)
		if err := eng.EmbedNode(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.RetractNode(retracted, "PII captured accidentally", ""); err != nil {
		t.Fatal(err)
	}

	matches, err := eng.findRetractedMatches(ctx,
		"Captured user's full home address by error in chat", "events", 0.3)
	if err != nil {
		t.Fatalf("findRetractedMatches: %v", err)
	}

	foundRetracted := false
	for _, m := range matches {
		if m.URI == retracted {
			foundRetracted = true
		}
		if m.URI == live {
			t.Errorf("findRetractedMatches included live node %s", live)
		}
	}
	if !foundRetracted {
		t.Errorf("findRetractedMatches missed retracted node %s; matches=%v", retracted, matches)
	}
}

func TestFindRetractedMatches_RespectsCategory(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)
	ctx := context.Background()

	uri := seedAndEmbed(t, eng, "events", "evt",
		"shared and overlapping vocabulary across categories test",
		"Body content with enough length to pass validation thresholds.")

	embedder, _ := NewTFIDFEmbedder(db, 512)
	eng.SetEmbedder(embedder)
	n, _ := db.GetNodeByURI(uri)
	if err := eng.EmbedNode(ctx, n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(uri, "test", ""); err != nil {
		t.Fatal(err)
	}

	// Same query text but different category → should not match.
	matches, err := eng.findRetractedMatches(ctx,
		"shared and overlapping vocabulary across categories test", "patterns", 0.3)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Errorf("findRetractedMatches should respect category boundary, got %d matches", len(matches))
	}
}

func TestRetract_RejectsNonMemURI(t *testing.T) {
	db := testDB(t)
	eng := New(db, nil)
	_, err := eng.Retract(context.Background(), RetractInput{URI: "not-a-uri", Reason: "x"})
	if err == nil || !strings.Contains(err.Error(), "must start with mem://") {
		t.Errorf("expected mem:// validation error, got: %v", err)
	}
}

func TestRetract_RejectsNonMemSupersededBy(t *testing.T) {
	db := testDB(t)
	eng := New(db, nil)
	_, err := eng.Retract(context.Background(), RetractInput{
		URI: "mem://user/events/foo", Reason: "x", SupersededBy: "not-a-uri",
	})
	if err == nil || !strings.Contains(err.Error(), "must start with mem://") {
		t.Errorf("expected superseded-by validation error, got: %v", err)
	}
}

func TestRemember_RefusesRetractedURICollision(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)
	ctx := context.Background()

	uri := seedAndEmbed(t, eng, "events", "doomed",
		"first version of this fact",
		"Body content with enough length to pass validation thresholds easily.")
	if _, err := db.RetractNode(uri, "test", ""); err != nil {
		t.Fatal(err)
	}

	// Attempt to write to the same URI: refused independently of dedup gate.
	_, _, err := eng.Remember(ctx, RememberInput{
		Category: "events", Name: "doomed",
		Summary:              "second attempt at the same slug after retraction",
		Body:                 "Body content with enough length to pass validation thresholds easily.",
		AcknowledgeRetracted: true,
	})
	if err == nil || !strings.Contains(err.Error(), "is retracted") {
		t.Errorf("expected URI-retracted refusal, got: %v", err)
	}
}

func TestRemember_SurfacesRetractedMatchAsError(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)
	ctx := context.Background()

	retracted := seedAndEmbed(t, eng, "events", "old-pii-event",
		"operator's mother's maiden name discussed in context",
		"Body content with enough length to pass validation thresholds easily.")

	// Build the embedder after seeding so TFIDF has corpus, then re-embed.
	embedder, _ := NewTFIDFEmbedder(db, 512)
	eng.SetEmbedder(embedder)
	n, _ := db.GetNodeByURI(retracted)
	if err := eng.EmbedNode(ctx, n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(retracted, "PII captured accidentally", ""); err != nil {
		t.Fatal(err)
	}

	// New write with semantically similar L0; should hit the gate.
	_, _, err := eng.Remember(ctx, RememberInput{
		Category: "events", Name: "new-event",
		Summary: "operator's mother's maiden name from earlier discussion",
		Body:    "Different body content with enough length to pass validation thresholds.",
	})
	if err == nil {
		t.Fatal("expected RetractedMatchError, got nil")
	}
	isMatch, uris := IsRetractedMatch(err)
	if !isMatch {
		t.Fatalf("error is not RetractedMatchError: %v", err)
	}
	if len(uris) == 0 {
		t.Error("RetractedMatchError carries no URIs")
	}
	for _, u := range uris {
		if u != retracted {
			t.Errorf("matched URI = %q, want %q", u, retracted)
		}
	}

	// Absence-of-leakage: error message must not contain the retraction reason.
	msg := err.Error()
	for _, leak := range []string{"PII", "maiden", "accidentally"} {
		if strings.Contains(msg, leak) {
			t.Errorf("error message leaks retraction reason via %q: %q", leak, msg)
		}
	}
}

func TestRemember_AcknowledgeRetractedBypassesGate(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: "[]"}}
	eng := New(db, mock)
	ctx := context.Background()

	retracted := seedAndEmbed(t, eng, "events", "blocked-pattern",
		"pattern shape that future writes will collide with",
		"Body content with enough length to pass validation thresholds easily.")

	embedder, _ := NewTFIDFEmbedder(db, 512)
	eng.SetEmbedder(embedder)
	n, _ := db.GetNodeByURI(retracted)
	if err := eng.EmbedNode(ctx, n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(retracted, "test", ""); err != nil {
		t.Fatal(err)
	}

	// With acknowledge=true, the gate is bypassed.
	uri, created, err := eng.Remember(ctx, RememberInput{
		Category: "events", Name: "different-event",
		Summary:              "pattern shape that future writes might collide with",
		Body:                 "Body content with enough length to pass validation thresholds easily.",
		AcknowledgeRetracted: true,
	})
	if err != nil {
		t.Fatalf("expected success after acknowledge, got: %v", err)
	}
	if !created {
		t.Error("expected created=true on a fresh slug")
	}
	if uri != "mem://user/events/different-event" {
		t.Errorf("uri = %q, want %q", uri, "mem://user/events/different-event")
	}
}

func TestRetractedMatchError_NoCategoryOrTagInline(t *testing.T) {
	// Even with multiple matches, the error structure must carry only URIs —
	// no category, no tag, no reason. URI alone is the signal.
	err := &RetractedMatchError{MatchedURIs: []string{
		"mem://user/events/a",
		"mem://user/events/b",
	}}

	msg := err.Error()
	// URIs should be present.
	if !strings.Contains(msg, "mem://user/events/a") || !strings.Contains(msg, "mem://user/events/b") {
		t.Errorf("error message missing URIs: %q", msg)
	}
	// Forbidden inline content.
	forbidden := []string{"PII", "wrong-fact", "stale", "category=", "reason=", "tag="}
	for _, f := range forbidden {
		if strings.Contains(msg, f) {
			t.Errorf("error message smuggles %q: %q", f, msg)
		}
	}
}
