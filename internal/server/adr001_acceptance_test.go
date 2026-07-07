package server

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

// This file pins the ADR-001 §MVP acceptance criteria end-to-end at the HTTP
// surface. Each test names the criterion it enforces.

// acceptanceServer builds a server with a working embedder so /api/search runs.
func acceptanceServer(t *testing.T) *Server {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	eng := engine.New(db, nil)
	emb, err := engine.NewHashEmbedder(0)
	if err != nil {
		t.Fatalf("embedder: %v", err)
	}
	eng.SetEmbedder(emb)

	srv := New(db, eng, "test-version")
	t.Cleanup(srv.Close)
	return srv
}

func seedLeaf(t *testing.T, srv *Server, uri, category, l0, l1 string) {
	t.Helper()
	if err := srv.db.CreateNode(&store.MemNode{
		URI: uri, NodeType: "leaf", Category: category,
		L0Abstract: l0, L1Overview: l1,
	}); err != nil {
		t.Fatalf("seed %s: %v", uri, err)
	}
	n, _ := srv.db.GetNodeByURI(uri)
	if err := srv.engine.EmbedNode(t.Context(), n); err != nil {
		t.Fatalf("embed %s: %v", uri, err)
	}
}

func get(t *testing.T, srv *Server, path string) (int, string) {
	t.Helper()
	req := newTestRequest("GET", path, nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w.Code, w.Body.String()
}

// Criterion: running the same search twice mutates no node state and returns
// identical scores — the only write is one shown event per surfacing.
func TestAcceptance_SearchIsReadIdempotent(t *testing.T) {
	srv := acceptanceServer(t)
	seedLeaf(t, srv, "mem://agent/cases/scar", "cases",
		"migration snapshots caused cross-db data loss", "the PR31 scar tissue body")

	snapshot := func() (float64, int, string) {
		n, _ := srv.db.GetNodeByURI("mem://agent/cases/scar")
		la := "nil"
		if n.LastAccess != nil {
			la = fmt.Sprintf("%d", *n.LastAccess)
		}
		return n.Relevance, n.AccessCount, la
	}
	rel0, acc0, la0 := snapshot()

	code1, body1 := get(t, srv, "/api/search?q=migration+snapshots+data+loss")
	code2, body2 := get(t, srv, "/api/search?q=migration+snapshots+data+loss")
	if code1 != 200 || code2 != 200 {
		t.Fatalf("search status: %d, %d", code1, code2)
	}
	if body1 != body2 {
		t.Errorf("identical queries returned different bodies:\n%s\n---\n%s", body1, body2)
	}

	rel1, acc1, la1 := snapshot()
	if rel0 != rel1 || acc0 != acc1 || la0 != la1 {
		t.Errorf("search mutated node state: relevance %f→%f access %d→%d last_access %s→%s",
			rel0, rel1, acc0, acc1, la0, la1)
	}

	// The only write: shown events (one per returned result per query).
	srv.Close() // deterministic drain barrier
	n, err := srv.db.CountEvents("shown")
	if err != nil {
		t.Fatalf("CountEvents: %v", err)
	}
	if n != 2 {
		t.Errorf("shown events = %d, want 2 (one per query that returned the node)", n)
	}
}

// Criterion: search output carries L0 + URI and no L1 body.
func TestAcceptance_SearchReturnsPointersNotPayloads(t *testing.T) {
	srv := acceptanceServer(t)
	const l1Marker = "L1-PAYLOAD-MARKER must not ride the search surface"
	seedLeaf(t, srv, "mem://agent/patterns/pointer", "patterns",
		"a pattern about pointers and payloads", l1Marker)

	code, body := get(t, srv, "/api/search?q=pointers+and+payloads")
	if code != 200 {
		t.Fatalf("search status %d", code)
	}
	if !strings.Contains(body, "mem://agent/patterns/pointer") {
		t.Fatalf("expected result URI in body: %s", body)
	}
	if strings.Contains(body, l1Marker) || strings.Contains(body, "l1_overview") {
		t.Errorf("search response leaked L1 payload (ADR-001 §2 — pointers, not payloads):\n%s", body)
	}
}

// Criterion: `continuity show <uri>` records exactly one use event and
// refreshes relevance — the fetch IS the use event.
func TestAcceptance_NodeFetchIsTheUseEvent(t *testing.T) {
	srv := acceptanceServer(t)
	seedLeaf(t, srv, "mem://user/preferences/devbox", "preferences",
		"always use devbox for go tooling", "devbox is the whole dev surface")
	if _, err := srv.db.Exec(`UPDATE mem_nodes SET relevance = 0.4 WHERE uri = 'mem://user/preferences/devbox'`); err != nil {
		t.Fatal(err)
	}

	code, _ := get(t, srv, "/api/memories?uri=mem://user/preferences/devbox")
	if code != 200 {
		t.Fatalf("get memory status %d", code)
	}

	n, _ := srv.db.GetNodeByURI("mem://user/preferences/devbox")
	if n.Relevance != 1.0 {
		t.Errorf("relevance = %f, want 1.0 — the fetch is the ONLY path that refreshes relevance", n.Relevance)
	}
	if n.AccessCount != 0 {
		t.Errorf("access_count = %d, want 0 — frozen legacy column", n.AccessCount)
	}

	srv.Close()
	deepened, _ := srv.db.CountEvents("deepened")
	if deepened != 1 {
		t.Errorf("deepened events = %d, want exactly 1", deepened)
	}
}

// Criterion: every injection writes a shown event with its surface; the
// preview writes none.
func TestAcceptance_InjectionWritesShownPreviewWritesNothing(t *testing.T) {
	srv := acceptanceServer(t)
	seedLeaf(t, srv, "mem://user/feedback/name-the-boundary", "feedback",
		"name the responsibility boundary, not the mechanism", "naming guidance body")

	// Preview: zero events.
	_ = srv.renderContext("", true)
	srv.events.Close()
	if n, _ := srv.db.CountEvents(""); n != 0 {
		t.Fatalf("preview wrote %d events — preview must be side-effect free", n)
	}

	// Fresh recorder for the real injection (Close is terminal by design).
	srv.events = newEventRecorder(srv.db)
	_ = srv.buildContext("boot-session")
	srv.events.Close()

	events, err := srv.db.EventsByURI("mem://user/feedback/name-the-boundary")
	if err != nil || len(events) != 1 {
		t.Fatalf("feedback node events = %d (err %v), want 1", len(events), err)
	}
	if events[0].Event != "shown" || events[0].Surface != "tray" || events[0].SessionID != "boot-session" {
		t.Errorf("shown event malformed: %+v", events[0])
	}
}

// Criterion: a contract node untouched for a year still renders in Your
// Profile (decay exemption; the split alone would have created the silent
// contract-loss path).
func TestAcceptance_YearOldContractStillRenders(t *testing.T) {
	srv := acceptanceServer(t)
	seedLeaf(t, srv, "mem://user/preferences/old-standing", "preferences",
		"a standing preference from a year ago", "body")
	// A year without use, and pre-exemption erosion already applied.
	if _, err := srv.db.Exec(`UPDATE mem_nodes SET last_access = 1, created_at = 1 WHERE uri = 'mem://user/preferences/old-standing'`); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.db.DecayAllNodes(); err != nil {
		t.Fatal(err)
	}

	ctx := srv.buildContext("")
	if !strings.Contains(ctx, "a standing preference from a year ago") {
		t.Errorf("year-old contract node missing from cold boot — the repeated-correction failure is back:\n%s", ctx)
	}
}

// Criterion: cold boot contains no episodic ranked section and still contains
// Your Profile.
func TestAcceptance_ColdBootShapeHonorsTheKeystone(t *testing.T) {
	srv := acceptanceServer(t)
	seedLeaf(t, srv, "mem://user/preferences/p1", "preferences", "contract line", "body")
	seedLeaf(t, srv, "mem://agent/events/e1", "events", "episodic event line", "body")
	seedLeaf(t, srv, "mem://agent/patterns/pt1", "patterns", "episodic pattern line", "body")

	ctx := srv.buildContext("")
	if strings.Contains(ctx, "Recent Memories") {
		t.Errorf("Recent Memories section exists (ADR-001 §1 deleted it):\n%s", ctx)
	}
	for _, episodic := range []string{"episodic event line", "episodic pattern line"} {
		if strings.Contains(ctx, episodic) {
			t.Errorf("episodic content on the tray: %q\n%s", episodic, ctx)
		}
	}
	if !strings.Contains(ctx, "### Your Profile") || !strings.Contains(ctx, "contract line") {
		t.Errorf("contract missing from cold boot:\n%s", ctx)
	}
}

// Criterion: a failed shown write never fails or delays the surfacing that
// triggered it — here via a full buffer (drop path) and a stopped recorder.
func TestAcceptance_TelemetryFailureNeverFailsSurfacing(t *testing.T) {
	srv := acceptanceServer(t)
	seedLeaf(t, srv, "mem://agent/cases/c1", "cases", "some case for search", "body")

	// Stopped recorder: record() must be a safe no-op, surfacing unaffected.
	srv.events.Close()
	code, body := get(t, srv, "/api/search?q=some+case+for+search")
	if code != 200 || !strings.Contains(body, "mem://agent/cases/c1") {
		t.Errorf("surfacing failed with stopped telemetry: code=%d body=%s", code, body)
	}

	// Full buffer: drops are counted, never block. Overfill directly.
	rec := newEventRecorder(srv.db)
	defer rec.Close()
	for i := 0; i < eventBuffer*4; i++ {
		rec.record("shown", "search", "mem://agent/cases/c1", "s")
	}
	// No assertion on the exact drop count (drain races the writer by
	// design) — the invariant is that we got here without blocking.
}

// TestAcceptance_SmartModeWritesShownOnlyForReturnedResults pins the §2
// denominator rule at the surface where it's enforced: events are written by
// the handler for returned results, so a mode that expands and re-ranks
// internally (smart mode's subqueries) cannot inflate exposure. With no LLM
// configured, mode=search falls back to Find — the invariant holds by
// construction; this test pins the handler-side accounting.
func TestAcceptance_ShownCountMatchesReturnedResults(t *testing.T) {
	srv := acceptanceServer(t)
	for i := 0; i < 5; i++ {
		seedLeaf(t, srv, fmt.Sprintf("mem://agent/cases/c%d", i), "cases",
			fmt.Sprintf("case number %d about widget failures", i), "body")
	}

	code, body := get(t, srv, "/api/search?q=widget+failures&limit=2&mode=search")
	if code != 200 {
		t.Fatalf("search status %d", code)
	}
	var resp struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("parse: %v", err)
	}

	srv.Close()
	shown, _ := srv.db.CountEvents("shown")
	if shown != resp.Count {
		t.Errorf("shown events = %d, returned results = %d — exposure accounting must match the caller-visible surface exactly", shown, resp.Count)
	}
}
