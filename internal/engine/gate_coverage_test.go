package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// seedRetracted creates a leaf, embeds it under emb, stores the vector, and
// retracts it — so it participates in the retraction-resurrection gate.
func seedRetracted(t *testing.T, db *store.DB, emb Embedder, uri, category, l0 string) {
	t.Helper()
	n := &store.MemNode{URI: uri, NodeType: "leaf", Category: category, L0Abstract: l0,
		L1Overview: "Body content with enough length to pass validation thresholds easily."}
	if err := db.CreateNode(n); err != nil {
		t.Fatalf("CreateNode %s: %v", uri, err)
	}
	got, err := db.GetNodeByURI(uri)
	if err != nil || got == nil {
		t.Fatalf("GetNodeByURI %s: %v", uri, err)
	}
	vec, err := emb.Embed(context.Background(), l0)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if err := db.SaveVector(got.ID, vec, emb.Model()); err != nil {
		t.Fatalf("SaveVector: %v", err)
	}
	if _, err := db.RetractNode(uri, "PII captured by accident", ""); err != nil {
		t.Fatalf("RetractNode: %v", err)
	}
}

// erroringEmbedder fails on Embed — used to exercise the fail-closed-on-error path.
type erroringEmbedder struct{}

func (erroringEmbedder) Embed(context.Context, string) ([]float64, error) {
	return nil, fmt.Errorf("embed boom")
}
func (erroringEmbedder) Model() string   { return "errmb" }
func (erroringEmbedder) Dimensions() int { return 8 }

// TestExtraction_SkipsRetractedMatch_KeepsRest pins finding #1 + the per-candidate
// rule: an extracted candidate matching a retracted memory must be dropped (no new
// live node), while OTHER candidates in the same batch are still written. The
// pre-existing path only ran findSimilarNode (which skips retracted nodes), so the
// resurrected content was written as a brand-new node.
func TestExtraction_SkipsRetractedMatch_KeepsRest(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)

	const pii = "operator home address one two three main street"
	seedRetracted(t, db, emb, "mem://agent/patterns/old-pii", "patterns", pii)

	// Candidate A reproduces the retracted PII (must be skipped); candidate B is
	// unrelated (must be written).
	resp := `[
		{"category":"patterns","uri_hint":"resurrect-attempt","l0":"operator home address one two three main street","l1":"Body content with enough length to pass validation thresholds easily."},
		{"category":"patterns","uri_hint":"unrelated-note","l0":"golang cobra cli flag parsing helpers","l1":"Body content with enough length to pass validation thresholds easily."}
	]`
	mock := &llm.MockClient{Response: &llm.Response{Content: resp, Provider: "mock"}}

	if err := extractMemories(db, mock, emb, "sess-extract", makeTranscript(t)); err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	if n, _ := db.GetNodeByURI("mem://agent/patterns/resurrect-attempt"); n != nil {
		t.Errorf("resurrection: extraction wrote a live node reproducing retracted PII: %+v", n)
	}
	if n, _ := db.GetNodeByURI("mem://agent/patterns/unrelated-note"); n == nil {
		t.Error("per-candidate rule violated: the unrelated candidate was dropped along with the bad one")
	}
}

// TestExtraction_SkipsExactRetractedURICollision pins the exact-URI guard: an
// LLM uri_hint that resolves to a retracted MERGEABLE node, with DIFFERENT content
// (so the vector gate can't catch it), must not overwrite the retracted row in
// place. No vector is stored, modelling the "no same-identity vector" case.
func TestExtraction_SkipsExactRetractedURICollision(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)

	const uri = "mem://user/preferences/legacy-pref"
	n := &store.MemNode{URI: uri, NodeType: "leaf", Category: "preferences",
		L0Abstract: "original retracted preference content",
		L1Overview: "Body content with enough length to pass validation thresholds easily."}
	if err := db.CreateNode(n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(uri, "user asked to forget this", ""); err != nil {
		t.Fatal(err)
	}
	before := snapshotNode(t, db, uri)

	resp := `[{"category":"preferences","uri_hint":"legacy-pref","l0":"totally different unrelated wording here","l1":"Body content with enough length to pass validation thresholds easily."}]`
	mock := &llm.MockClient{Response: &llm.Response{Content: resp, Provider: "mock"}}

	if err := extractMemories(db, mock, emb, "sess", makeTranscript(t)); err != nil {
		t.Fatalf("extractMemories: %v", err)
	}
	// Full-row equality — the retracted mergeable node must be byte-for-byte intact.
	assertNoResurrection(t, db, uri, before)
}

// TestSignal_SkipsExactRetractedURICollision is the signal-path equivalent.
func TestSignal_SkipsExactRetractedURICollision(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)

	const uri = "mem://user/preferences/legacy-pref"
	n := &store.MemNode{URI: uri, NodeType: "leaf", Category: "preferences",
		L0Abstract: "original retracted preference content",
		L1Overview: "Body content with enough length to pass validation thresholds easily."}
	if err := db.CreateNode(n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(uri, "user asked to forget this", ""); err != nil {
		t.Fatal(err)
	}
	before := snapshotNode(t, db, uri)

	resp := `[{"category":"preferences","uri_hint":"legacy-pref","l0":"totally different unrelated wording here","l1":"Body content with enough length to pass validation thresholds easily."}]`
	mock := &llm.MockClient{Response: &llm.Response{Content: resp, Provider: "mock"}}

	eng := New(db, mock)
	eng.SetEmbedder(emb)
	if err := eng.ExtractSignal(context.Background(), "sess", "remember this"); err != nil {
		t.Fatalf("ExtractSignal: %v", err)
	}
	assertNoResurrection(t, db, uri, before)
}

// TestExtraction_GatesEffectiveCategoryOnCrossCategoryMergeTarget pins the
// merge_target category-mismatch fix: a candidate declared in one category but
// merge_target'd into a LIVE node of another category must be gated against
// retracted nodes of the TARGET category. Otherwise content matching a retracted
// preference, smuggled in as an "events" candidate merged onto a live preference,
// would overwrite that live node — a semantic resurrection bypass.
func TestExtraction_GatesEffectiveCategoryOnCrossCategoryMergeTarget(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)

	// Retracted PREFERENCE carrying the sensitive content.
	const secret = "social security number nine eight seven six five four"
	seedRetracted(t, db, emb, "mem://user/preferences/old-secret", "preferences", secret)

	// A LIVE preference the candidate will try to merge onto.
	live := &store.MemNode{URI: "mem://user/preferences/live-pref", NodeType: "leaf", Category: "preferences",
		L0Abstract: "favorite editor is vim", L1Overview: "Body content with enough length to pass validation thresholds easily."}
	if err := db.CreateNode(live); err != nil {
		t.Fatal(err)
	}

	// Candidate declares category "events" (no retracted events exist) but
	// merge_targets the live preference, with L0 == the retracted preference's PII.
	resp := `[{"category":"events","uri_hint":"innocuous","merge_target":"mem://user/preferences/live-pref","l0":"social security number nine eight seven six five four","l1":"Body content with enough length to pass validation thresholds easily."}]`
	mock := &llm.MockClient{Response: &llm.Response{Content: resp, Provider: "mock"}}

	if err := extractMemories(db, mock, emb, "sess", makeTranscript(t)); err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	got, _ := db.GetNodeByURI("mem://user/preferences/live-pref")
	if got == nil || got.L0Abstract != "favorite editor is vim" {
		t.Errorf("cross-category merge_target resurrected retracted PII into the live preference: L0=%q", func() string {
			if got == nil {
				return "(nil)"
			}
			return got.L0Abstract
		}())
	}
}

// TestExtraction_IgnoresUnresolvableMergeTarget pins the round-3 root fix: a raw
// LLM merge_target is never trusted as a URI. A variant string (here a ?query
// suffix; casing/#frag/trailing-slash behave the same) that doesn't resolve to a
// real node must be IGNORED — the write falls back to the canonical constructed
// uri, where the exact-URI guard then catches the retracted collision. Pre-fix the
// variant was written verbatim, spawning a live node that dodged both the category
// gate and the canonical exact-URI guard.
func TestExtraction_IgnoresUnresolvableMergeTarget(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)

	// Retracted MERGEABLE node with NO vector (so only the exact-URI guard can
	// catch it — the vector gate is blind here, matching Codex's scenario).
	const canonical = "mem://user/preferences/legacy-pref"
	n := &store.MemNode{URI: canonical, NodeType: "leaf", Category: "preferences",
		L0Abstract: "original retracted content", L1Overview: "Body content with enough length to pass validation thresholds easily."}
	if err := db.CreateNode(n); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode(canonical, "forget this", ""); err != nil {
		t.Fatal(err)
	}
	before := snapshotNode(t, db, canonical)

	const variant = canonical + "?bypass=1"
	resp := `[{"category":"preferences","uri_hint":"legacy-pref","merge_target":"` + variant + `","l0":"RESURRECTED content","l1":"Body content with enough length to pass validation thresholds easily."}]`
	mock := &llm.MockClient{Response: &llm.Response{Content: resp, Provider: "mock"}}

	if err := extractMemories(db, mock, emb, "sess", makeTranscript(t)); err != nil {
		t.Fatalf("extractMemories: %v", err)
	}

	if got, _ := db.GetNodeByURI(variant); got != nil {
		t.Errorf("variant merge_target was written verbatim, spawning a live node: %s", variant)
	}
	// The canonical retracted node must be byte-for-byte intact (exact-URI guard).
	assertNoResurrection(t, db, canonical, before)
}

// TestExtraction_DeferredWhenLocked pins finding #2: while the vector identity is
// locked the gate cannot run, so extraction must write NOTHING (fail closed) and
// must not mark the session extracted. Proven non-vacuous by showing the same
// candidate IS written once the lock clears.
func TestExtraction_DeferredWhenLocked(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)
	mock := &llm.MockClient{Response: &llm.Response{Content: `[
		{"category":"patterns","uri_hint":"locked-note","l0":"some freshly extracted pattern worth keeping","l1":"Body content with enough length to pass validation thresholds easily."}
	]`, Provider: "mock"}}

	eng := New(db, mock)
	eng.SetEmbedder(emb)
	eng.identityMismatch = true // locked

	path := makeTranscript(t)
	if err := eng.ExtractSessionForce("sess-locked", path); err != nil {
		t.Fatalf("ExtractSessionForce (locked): %v", err)
	}
	if n, _ := db.GetNodeByURI("mem://agent/patterns/locked-note"); n != nil {
		t.Fatalf("locked extraction wrote a node without running the gate: %+v", n)
	}

	// Clear the lock — the same candidate must now be written, proving the lock is
	// what suppressed it (not a content-gate or parse failure).
	eng.identityMismatch = false
	if err := eng.ExtractSessionForce("sess-locked", path); err != nil {
		t.Fatalf("ExtractSessionForce (unlocked): %v", err)
	}
	if n, _ := db.GetNodeByURI("mem://agent/patterns/locked-note"); n == nil {
		t.Error("candidate not written after lock cleared — test would be vacuous")
	}
}

// TestSignal_SkipsRetractedMatch pins finding #3: the signal-keyword path must run
// the per-candidate retraction gate too.
func TestSignal_SkipsRetractedMatch(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)

	const pii = "user social security number nine eight seven six five"
	seedRetracted(t, db, emb, "mem://user/events/old-ssn", "events", pii)

	mock := &llm.MockClient{Response: &llm.Response{Content: `[
		{"category":"events","uri_hint":"ssn-again","l0":"user social security number nine eight seven six five","l1":"Body content with enough length to pass validation thresholds easily."},
		{"category":"events","uri_hint":"benign-event","l0":"deployed the release on a friday afternoon","l1":"Body content with enough length to pass validation thresholds easily."}
	]`, Provider: "mock"}}

	eng := New(db, mock)
	eng.SetEmbedder(emb)

	if err := eng.ExtractSignal(context.Background(), "sess-signal", "remember this"); err != nil {
		t.Fatalf("ExtractSignal: %v", err)
	}
	if n, _ := db.GetNodeByURI("mem://user/events/ssn-again"); n != nil {
		t.Errorf("resurrection: signal wrote a live node reproducing retracted PII: %+v", n)
	}
	if n, _ := db.GetNodeByURI("mem://user/events/benign-event"); n == nil {
		t.Error("per-candidate rule violated: benign signal candidate was dropped")
	}
}

// TestSignal_DeferredWhenLocked pins finding #2 for the signal path.
func TestSignal_DeferredWhenLocked(t *testing.T) {
	db := testDB(t)
	emb, _ := NewHashEmbedder(0)
	mock := &llm.MockClient{Response: &llm.Response{Content: `[
		{"category":"events","uri_hint":"locked-signal","l0":"a signal the user explicitly flagged","l1":"Body content with enough length to pass validation thresholds easily."}
	]`, Provider: "mock"}}

	eng := New(db, mock)
	eng.SetEmbedder(emb)
	eng.identityMismatch = true // locked

	if err := eng.ExtractSignal(context.Background(), "sess", "remember this"); err != nil {
		t.Fatalf("ExtractSignal (locked): %v", err)
	}
	if n, _ := db.GetNodeByURI("mem://user/events/locked-signal"); n != nil {
		t.Fatalf("locked signal wrote a node without running the gate: %+v", n)
	}
}

// TestRemember_FailsClosedOnGateError pins finding #5: if the retraction gate
// cannot complete (embedding error), Remember must REFUSE the write rather than
// proceed — and --acknowledge-retracted must still bypass it.
func TestRemember_FailsClosedOnGateError(t *testing.T) {
	db := testDB(t)
	eng := New(db, &llm.MockClient{Response: &llm.Response{Content: "[]"}})
	eng.SetEmbedder(erroringEmbedder{})

	const body = "a sufficiently long overview body for validation"
	_, _, err := eng.Remember(context.Background(), RememberInput{
		Category: "patterns", Name: "alpha", Summary: "some candidate text", Body: body,
	})
	if err == nil {
		t.Fatal("gate error must fail closed (refuse the write), got nil error")
	}
	if !strings.Contains(err.Error(), "failing closed") {
		t.Errorf("expected a fail-closed error, got: %v", err)
	}

	// --acknowledge-retracted bypasses the gate entirely, so the write proceeds.
	uri, _, err := eng.Remember(context.Background(), RememberInput{
		Category: "patterns", Name: "beta", Summary: "some candidate text", Body: body, AcknowledgeRetracted: true,
	})
	if err != nil {
		t.Fatalf("acknowledged write should bypass the gate, got: %v", err)
	}
	if n, _ := db.GetNodeByURI(uri); n == nil {
		t.Error("acknowledged write was not stored")
	}
}
