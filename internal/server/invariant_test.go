package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

// retractedURI is the canonical retracted node used across the invariant tests.
// Any code path that returns this URI in a default-read response is a violation
// of the contract: "default reads must behave as if retracted nodes do not
// exist, except explicit URI inspection."
const retractedURI = "mem://user/events/SECRET-MARKER-must-not-leak"

// retractedReason is the reason text. Any response body that contains this
// substring (outside the explicit `--include-retracted` path) is a leakage.
const retractedReason = "REASON-MARKER-must-not-leak captured PII accidentally"

// retractedSummary is the L0 abstract of the retracted node. Any response body
// that contains this substring outside `--include-retracted` is a content leak.
const retractedSummary = "L0-MARKER-must-not-leak summary content"

// retractedBody is the L1 overview. Same leakage rule applies.
const retractedBody = "L1-MARKER-must-not-leak body content with enough chars to pass validation thresholds."

// seedInvariantWorld sets up a world with one retracted node + several live
// nodes + a retracted node that has been embedded (so it can be matched
// against by similarity search). Returns the configured server.
func seedInvariantWorld(t *testing.T) *Server {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	eng := engine.New(db, nil)

	// Seed live nodes first so they appear in default reads, embeddings get a corpus.
	liveNodes := []*store.MemNode{
		{URI: "mem://user/events/live-1", NodeType: "leaf", Category: "events",
			L0Abstract: "Live event one summary", L1Overview: "Live event one body content here for validation."},
		{URI: "mem://user/events/live-2", NodeType: "leaf", Category: "events",
			L0Abstract: "Live event two summary", L1Overview: "Live event two body content here for validation."},
		{URI: "mem://user/preferences/live-pref", NodeType: "leaf", Category: "preferences",
			L0Abstract: "A live preference summary", L1Overview: "A live preference body content here for validation."},
	}
	for _, n := range liveNodes {
		if err := db.CreateNode(n); err != nil {
			t.Fatalf("seed live: %v", err)
		}
	}

	// Seed the retracted node.
	retracted := &store.MemNode{
		URI: retractedURI, NodeType: "leaf", Category: "events",
		L0Abstract: retractedSummary, L1Overview: retractedBody,
	}
	if err := db.CreateNode(retracted); err != nil {
		t.Fatalf("seed retracted: %v", err)
	}

	// Embed everything so dedup-against-retracted has signal to find.
	embedder, err := engine.NewTFIDFEmbedder(db, 512)
	if err != nil {
		t.Fatalf("embedder: %v", err)
	}
	eng.SetEmbedder(embedder)
	for _, uri := range []string{liveNodes[0].URI, liveNodes[1].URI, liveNodes[2].URI, retractedURI} {
		n, _ := db.GetNodeByURI(uri)
		if err := eng.EmbedNode(t.Context(), n); err != nil {
			t.Fatal(err)
		}
	}

	// Now retract.
	if _, err := db.RetractNode(retractedURI, retractedReason, ""); err != nil {
		t.Fatalf("retract: %v", err)
	}

	return New(db, eng, "test-version")
}

// assertNoLeak runs the leakage assertions against a body of text (response
// body, log line, etc.). The retracted URI MAY appear when listing direct
// children, but only as a metadata-only entry; reason text and original
// content (L0/L1) MUST NOT appear in any default response.
func assertNoLeak(t *testing.T, where, body string) {
	t.Helper()
	if strings.Contains(body, retractedReason) {
		t.Errorf("[%s] leaked retraction REASON content: %q", where, retractedReason)
	}
	if strings.Contains(body, retractedSummary) {
		t.Errorf("[%s] leaked L0 SUMMARY of retracted node: %q", where, retractedSummary)
	}
	if strings.Contains(body, retractedBody) {
		t.Errorf("[%s] leaked L1 BODY of retracted node: %q", where, retractedBody)
	}
}

// assertURINotPresent fails if the retracted URI appears in a context where
// it should be excluded entirely (search results, context injection, etc.).
// For tree listings, the URI may appear as a metadata-only entry — use
// assertNoLeak there instead.
func assertURINotPresent(t *testing.T, where, body string) {
	t.Helper()
	if strings.Contains(body, retractedURI) {
		t.Errorf("[%s] retracted URI should be absent: %q", where, retractedURI)
	}
}

// TestInvariant_StoreLayerReadPathsExcludeRetracted asserts the three
// default multi-row read methods filter the retracted node out.
func TestInvariant_StoreLayerReadPathsExcludeRetracted(t *testing.T) {
	srv := seedInvariantWorld(t)

	t.Run("FindByCategory(events)", func(t *testing.T) {
		got, err := srv.db.FindByCategory("events")
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range got {
			if n.URI == retractedURI {
				t.Errorf("FindByCategory returned retracted node %s", n.URI)
			}
		}
	})

	t.Run("ListLeaves", func(t *testing.T) {
		got, err := srv.db.ListLeaves()
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range got {
			if n.URI == retractedURI {
				t.Errorf("ListLeaves returned retracted node %s", n.URI)
			}
		}
	})

	t.Run("GetChildren(events)", func(t *testing.T) {
		got, err := srv.db.GetChildren("mem://user/events")
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range got {
			if n.URI == retractedURI {
				t.Errorf("GetChildren returned retracted node %s", n.URI)
			}
		}
	})
}

// TestInvariant_HTTPDefaultRoutesExcludeRetracted asserts that every default
// HTTP read route excludes retracted content. This is the route-level
// counterpart to the store-layer test — it catches the case where a route
// somehow re-derives data without going through the filtered store methods.
func TestInvariant_HTTPDefaultRoutesExcludeRetracted(t *testing.T) {
	srv := seedInvariantWorld(t)

	type tt struct {
		name string
		path string
		// useURIPresence: if true, only assert content absence (URI may appear
		// as a metadata-only entry, e.g. tree listings). If false, the URI
		// itself must be absent from the response.
		uriMayAppear bool
	}

	cases := []tt{
		{"tree roots", "/api/tree", true /* parent dirs may include retracted's parent */},
		{"tree under events", "/api/tree?uri=mem://user/events", true /* metadata-only entry allowed */},
		{"search for retracted summary", "/api/search?q=L0-MARKER-must-not-leak", false},
		{"search for live data", "/api/search?q=live", false /* retracted URI must not appear in results */},
		{"profile", "/api/profile", false},
		{"context injection", "/api/context?session_id=test-session", false},
		{"timeline", "/api/timeline", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := newTestRequest("GET", c.path, nil)
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)
			body := w.Body.String()

			// A failing route would emit an error body that doesn't contain the
			// markers and would pass the leak assertions for the wrong reason —
			// false negative on the invariant we're defending. Assert success
			// status before inspecting content.
			if w.Code != 200 {
				t.Fatalf("expected 200, got %d (body: %s)", w.Code, body)
			}

			assertNoLeak(t, c.name, body)
			if !c.uriMayAppear {
				assertURINotPresent(t, c.name, body)
			}
		})
	}
}

// TestInvariant_TreeListingMetadataOnlyForRetracted asserts that when a
// retracted node appears as a child in a tree listing, it carries the
// retracted marker but no content fields. This is the "absence-not-empty"
// check at the tree route — content keys must be absent, not empty/null.
func TestInvariant_TreeListingMetadataOnlyForRetracted(t *testing.T) {
	srv := seedInvariantWorld(t)

	req := newTestRequest("GET", "/api/tree?uri=mem://user/events", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var resp struct {
		Nodes []map[string]any `json:"nodes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var found map[string]any
	for _, n := range resp.Nodes {
		if uri, _ := n["uri"].(string); uri == retractedURI {
			found = n
			break
		}
	}
	if found == nil {
		// Default tree listing excludes retracted entirely — that's also a
		// valid implementation of the invariant (and is what GetChildren does).
		// Just ensure the URI didn't leak.
		assertURINotPresent(t, "tree default", w.Body.String())
		return
	}

	// If the implementation chose to surface retracted-as-metadata in the
	// default tree, content fields must be absent.
	for _, key := range []string{"l0_abstract", "l1_overview", "tombstone_reason"} {
		if _, ok := found[key]; ok {
			t.Errorf("retracted tree entry leaks content key %q with value %v", key, found[key])
		}
	}
}

// TestInvariant_ShowRetractedRespectsAbsenceContract asserts that the
// /api/memories?uri= endpoint, when called without include_retracted, returns
// the retracted node as metadata-only — content keys absent, not empty.
func TestInvariant_ShowRetractedRespectsAbsenceContract(t *testing.T) {
	srv := seedInvariantWorld(t)

	req := newTestRequest("GET", "/api/memories?uri="+retractedURI, nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	body := w.Body.String()
	assertNoLeak(t, "show without flag", body)

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"summary", "body", "detail", "tombstone_reason"} {
		if _, ok := resp[key]; ok {
			t.Errorf("retracted show without flag leaks key %q with value %v", key, resp[key])
		}
	}
	if r, ok := resp["retracted"]; !ok || r != true {
		t.Errorf("retracted marker missing or false: %v", resp["retracted"])
	}
}

// TestInvariant_ShowWithFlagRevealsRetracted is the inverse — the explicit
// URI inspection path SHOULD return the reason and content. This is the
// "explicit URI inspection" exception named in the contract.
func TestInvariant_ShowWithFlagRevealsRetracted(t *testing.T) {
	srv := seedInvariantWorld(t)

	req := newTestRequest("GET", "/api/memories?uri="+retractedURI+"&include_retracted=true", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	body := w.Body.String()
	if !strings.Contains(body, retractedReason) {
		t.Errorf("show --include-retracted should reveal reason; got: %s", body)
	}
	if !strings.Contains(body, retractedSummary) {
		t.Errorf("show --include-retracted should reveal L0 summary; got: %s", body)
	}
}

// TestInvariant_ContextInjectionExcludesRetracted is the load-bearing
// regression for the buildContext relational-profile bug. If the relational
// profile is retracted, it must not appear in the injected context.
//
// The public retract verb refuses the relational profile URI (it's system-
// owned), so this test seeds the retraction state directly via SQL — that's
// the operator-uses-Beekeeper friction path the spec acknowledges. Defense-
// in-depth: even if the row gets retracted by that channel, buildContext
// must hold the line.
func TestInvariant_ContextInjectionExcludesRetractedRelationalProfile(t *testing.T) {
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if err := db.CreateNode(&store.MemNode{
		URI: "mem://user/profile/communication", NodeType: "leaf", Category: "profile",
		L0Abstract: "RELATIONAL-PROFILE-MARKER",
		L1Overview: "RELATIONAL-PROFILE-MARKER long-form profile that should NOT appear post-retraction.",
	}); err != nil {
		t.Fatal(err)
	}
	// Bypass the public-verb guardrail to simulate an operator SQL edit.
	if _, err := db.Exec(`UPDATE mem_nodes SET tombstoned_at = ?, tombstone_reason = ? WHERE uri = ?`,
		1700000000000, "test retraction via direct SQL",
		"mem://user/profile/communication"); err != nil {
		t.Fatal(err)
	}

	srv := New(db, engine.New(db, nil), "test-version")
	ctx := srv.buildContext("")

	if strings.Contains(ctx, "RELATIONAL-PROFILE-MARKER") {
		t.Errorf("retracted relational profile leaked into session context:\n%s", ctx)
	}
	if strings.Contains(ctx, "Working With You") {
		t.Errorf("Working With You section emitted despite retracted profile:\n%s", ctx)
	}
}
