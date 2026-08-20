package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

// Tests for the project-affine corpus index and project-scoped Recent
// Sessions (#79, ADR-001 §3).

// seedSessionNode creates a sessions row for the project and a leaf node
// affine to it via source_session.
func seedSessionNode(t *testing.T, srv *Server, sessionID, project, uri, category, l0 string) {
	t.Helper()
	if _, err := srv.db.InitSession(sessionID, project); err != nil {
		t.Fatalf("InitSession %s: %v", sessionID, err)
	}
	if err := srv.db.CreateNode(&store.MemNode{
		URI: uri, NodeType: "leaf", Category: category,
		L0Abstract: l0, L1Overview: "L1 body of " + uri,
		SourceSession: sessionID,
	}); err != nil {
		t.Fatalf("CreateNode %s: %v", uri, err)
	}
}

// indexSection extracts the ### Memory Index section from a rendered context.
func indexSection(t *testing.T, ctx string) string {
	t.Helper()
	start := strings.Index(ctx, "### Memory Index")
	if start < 0 {
		t.Fatalf("no ### Memory Index section:\n%s", ctx)
	}
	rest := ctx[start:]
	if end := strings.Index(rest[1:], "\n### "); end >= 0 {
		return rest[:end+1]
	}
	if end := strings.Index(rest, "</context>"); end >= 0 {
		return rest[:end]
	}
	return rest
}

// Acceptance (#79): for a project with affine nodes the context contains the
// index — shape counts plus L0 + URI pointers — scoped to that project only,
// with no L1 payloads.
func TestMemoryIndex_ProjectAffinePointers(t *testing.T) {
	srv := testServer(t)
	seedSessionNode(t, srv, "s-alpha", "/repo/alpha",
		"mem://agent/cases/alpha-scar", "cases", "alpha migration scar tissue")
	seedSessionNode(t, srv, "s-beta", "/repo/beta",
		"mem://agent/cases/beta-scar", "cases", "beta connection pool bug")
	// Sessionless write: counts only, never a pointer (accepted limit, #79).
	if err := srv.db.CreateNode(&store.MemNode{
		URI: "mem://agent/cases/sessionless", NodeType: "leaf", Category: "cases",
		L0Abstract: "a sessionless case",
	}); err != nil {
		t.Fatal(err)
	}

	ctx := srv.renderContext("cur", "/repo/alpha", false)
	section := indexSection(t, ctx)

	if !strings.Contains(section, "cases 3") {
		t.Errorf("shape counts missing or wrong (want cases 3):\n%s", section)
	}
	if !strings.Contains(section, "This project:") {
		t.Errorf("affine pointer block missing:\n%s", section)
	}
	if !strings.Contains(section, "alpha migration scar tissue") ||
		!strings.Contains(section, "mem://agent/cases/alpha-scar") {
		t.Errorf("affine node's L0 + URI missing:\n%s", section)
	}
	if strings.Contains(section, "beta connection pool bug") {
		t.Errorf("cross-project node leaked into the index:\n%s", section)
	}
	if strings.Contains(section, "a sessionless case") {
		t.Errorf("sessionless node rendered a pointer (must appear in counts only):\n%s", section)
	}
	if strings.Contains(ctx, "L1 body of") {
		t.Errorf("index leaked an L1 payload (pointers, not payloads):\n%s", ctx)
	}
}

// Acceptance (#79): project unknown at boot → shape-only counts, no L0 lines.
func TestMemoryIndex_UnknownProjectShapeOnly(t *testing.T) {
	srv := testServer(t)
	seedSessionNode(t, srv, "s-alpha", "/repo/alpha",
		"mem://agent/cases/alpha-scar", "cases", "alpha migration scar tissue")

	ctx := srv.renderContext("", "", false)
	section := indexSection(t, ctx)

	if !strings.Contains(section, "cases 1") {
		t.Errorf("shape counts missing:\n%s", section)
	}
	if strings.Contains(section, "This project:") || strings.Contains(section, "mem://") {
		t.Errorf("unknown project must render shape-only (no pointers):\n%s", section)
	}
}

// Acceptance (#79): known project with zero affine nodes → shape-only.
func TestMemoryIndex_ZeroAffineShapeOnly(t *testing.T) {
	srv := testServer(t)
	seedSessionNode(t, srv, "s-beta", "/repo/beta",
		"mem://agent/cases/beta-scar", "cases", "beta connection pool bug")

	ctx := srv.renderContext("", "/repo/alpha", false)
	section := indexSection(t, ctx)

	if strings.Contains(section, "This project:") || strings.Contains(section, "mem://") {
		t.Errorf("zero affine nodes must render shape-only:\n%s", section)
	}
}

// Empty corpus → no index section at all (nothing to map).
func TestMemoryIndex_EmptyCorpusNoSection(t *testing.T) {
	srv := testServer(t)
	ctx := srv.renderContext("", "/repo/alpha", false)
	if strings.Contains(ctx, "### Memory Index") {
		t.Errorf("index rendered for an empty corpus:\n%s", ctx)
	}
}

// Acceptance (#79, ADR-001 shown-is-not-use): index rendering writes no node
// state — no relevance mutation, no counters, no last_access. The only write
// is `shown` telemetry with surface "index" on the real path.
func TestMemoryIndex_RenderingWritesNoNodeState(t *testing.T) {
	srv := testServer(t)
	seedSessionNode(t, srv, "s-alpha", "/repo/alpha",
		"mem://agent/cases/alpha-scar", "cases", "alpha migration scar tissue")
	if _, err := srv.db.Exec(`UPDATE mem_nodes SET relevance = 0.4 WHERE uri = 'mem://agent/cases/alpha-scar'`); err != nil {
		t.Fatal(err)
	}

	snapshot := func() (float64, int, string) {
		n, _ := srv.db.GetNodeByURI("mem://agent/cases/alpha-scar")
		la := "nil"
		if n.LastAccess != nil {
			la = fmt.Sprintf("%d", *n.LastAccess)
		}
		return n.Relevance, n.AccessCount, la
	}
	rel0, acc0, la0 := snapshot()

	ctx := srv.renderContext("cur", "/repo/alpha", false)
	if !strings.Contains(ctx, "mem://agent/cases/alpha-scar") {
		t.Fatalf("precondition: node should render in the index:\n%s", ctx)
	}

	rel1, acc1, la1 := snapshot()
	if rel0 != rel1 || acc0 != acc1 || la0 != la1 {
		t.Errorf("index render mutated node state: relevance %f→%f access %d→%d last_access %s→%s",
			rel0, rel1, acc0, acc1, la0, la1)
	}

	srv.Close() // drain telemetry
	events, err := srv.db.EventsByURI("mem://agent/cases/alpha-scar")
	if err != nil || len(events) != 1 {
		t.Fatalf("index events = %d (err %v), want 1 shown", len(events), err)
	}
	if events[0].Event != "shown" || events[0].Surface != "index" {
		t.Errorf("index event malformed: %+v", events[0])
	}
}

// Preview renders the same index but writes nothing, not even telemetry.
func TestMemoryIndex_PreviewWritesNothing(t *testing.T) {
	srv := testServer(t)
	seedSessionNode(t, srv, "s-alpha", "/repo/alpha",
		"mem://agent/cases/alpha-scar", "cases", "alpha migration scar tissue")

	ctx := srv.renderContext("cur", "/repo/alpha", true)
	if !strings.Contains(ctx, "mem://agent/cases/alpha-scar") {
		t.Fatalf("preview should render the index:\n%s", ctx)
	}
	srv.Close()
	if n, _ := srv.db.CountEvents(""); n != 0 {
		t.Errorf("preview wrote %d events, want 0", n)
	}
}

// Acceptance (#79): a session row holding a pre-normalization raw worktree
// path still matches its repository's normalized identity — the cheap
// prefix compare in projectMatch.
func TestMemoryIndex_RawWorktreeRowMatchesNormalizedProject(t *testing.T) {
	srv := testServer(t)
	seedSessionNode(t, srv, "s-wt", "/repo/alpha/.claude/worktrees/agent-x",
		"mem://agent/cases/wt-scar", "cases", "worktree-born scar tissue")

	ctx := srv.renderContext("cur", "/repo/alpha", false)
	if !strings.Contains(ctx, "mem://agent/cases/wt-scar") {
		t.Errorf("raw worktree session row did not match normalized project:\n%s", ctx)
	}
	if !strings.Contains(ctx, "### Recent Sessions") {
		t.Errorf("worktree session missing from Recent Sessions:\n%s", ctx)
	}
}

// The index char budget holds: pointers stop before the section exceeds
// maxIndexContext, no matter how many affine nodes exist.
func TestMemoryIndex_BudgetEnforced(t *testing.T) {
	srv := testServer(t)
	if _, err := srv.db.InitSession("s-alpha", "/repo/alpha"); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("scar tissue detail ", 10) // ~190 chars
	for i := 0; i < 10; i++ {
		if err := srv.db.CreateNode(&store.MemNode{
			URI: fmt.Sprintf("mem://agent/cases/big-%d", i), NodeType: "leaf", Category: "cases",
			L0Abstract: long, SourceSession: "s-alpha",
		}); err != nil {
			t.Fatal(err)
		}
	}

	ctx := srv.renderContext("cur", "/repo/alpha", false)
	section := indexSection(t, ctx)
	if len(section) > maxIndexContext {
		t.Errorf("index section %d chars, budget %d:\n%s", len(section), maxIndexContext, section)
	}
	if !strings.Contains(section, "mem://agent/cases/big-") {
		t.Errorf("budget enforcement rendered no pointers at all:\n%s", section)
	}
}

// Pinned affine nodes must not starve the index: pins render in ### Pinned
// and are skipped here, but each skip must leave a candidate behind it — a
// project whose most recent nodes are all pinned still gets pointers for the
// rest, never a false shape-only render.
func TestMemoryIndex_PinnedNodesDoNotStarvePointers(t *testing.T) {
	srv := testServer(t)
	if _, err := srv.db.InitSession("s-alpha", "/repo/alpha"); err != nil {
		t.Fatal(err)
	}
	// maxIndexAffineNodes newest nodes all pinned...
	for i := 0; i < maxIndexAffineNodes; i++ {
		uri := fmt.Sprintf("mem://agent/cases/pinned-%d", i)
		if err := srv.db.CreateNode(&store.MemNode{
			URI: uri, NodeType: "leaf", Category: "cases",
			L0Abstract: fmt.Sprintf("pinned scar %d", i), SourceSession: "s-alpha",
		}); err != nil {
			t.Fatal(err)
		}
		if i < store.MaxPins {
			if _, err := srv.db.PinNode(uri); err != nil {
				t.Fatal(err)
			}
		}
	}
	// ...plus one older unpinned node that must still surface.
	if err := srv.db.CreateNode(&store.MemNode{
		URI: "mem://agent/cases/unpinned-survivor", NodeType: "leaf", Category: "cases",
		L0Abstract: "the unpinned survivor", SourceSession: "s-alpha",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.db.Exec(
		`UPDATE mem_nodes SET updated_at = 1 WHERE uri = 'mem://agent/cases/unpinned-survivor'`); err != nil {
		t.Fatal(err)
	}

	ctx := srv.renderContext("cur", "/repo/alpha", false)
	section := indexSection(t, ctx)
	if !strings.Contains(section, "mem://agent/cases/unpinned-survivor") {
		t.Errorf("pinned nodes starved the index into shape-only:\n%s", section)
	}
	for i := 0; i < store.MaxPins; i++ {
		if strings.Contains(section, fmt.Sprintf("mem://agent/cases/pinned-%d (", i)) &&
			strings.Contains(section, fmt.Sprintf("- pinned scar %d (mem://agent/cases/pinned-%d)", i, i)) {
			t.Errorf("pinned node %d rendered twice (tray + index):\n%s", i, section)
		}
	}
}

// One oversized line (a pathologically long URI) must not starve the shorter
// pointers behind it — the loop skips what doesn't fit and keeps going.
func TestMemoryIndex_OversizedLineDoesNotStarveRest(t *testing.T) {
	srv := testServer(t)
	if _, err := srv.db.InitSession("s-alpha", "/repo/alpha"); err != nil {
		t.Fatal(err)
	}
	hugeURI := "mem://agent/cases/" + strings.Repeat("very-long-slug-", 40) // ~620 chars
	if err := srv.db.CreateNode(&store.MemNode{
		URI: hugeURI, NodeType: "leaf", Category: "cases",
		L0Abstract: "oversized node", SourceSession: "s-alpha",
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.db.CreateNode(&store.MemNode{
		URI: "mem://agent/cases/short", NodeType: "leaf", Category: "cases",
		L0Abstract: "short survivor", SourceSession: "s-alpha",
	}); err != nil {
		t.Fatal(err)
	}
	// Make the oversized node the newest so it is considered first.
	if _, err := srv.db.Exec(`UPDATE mem_nodes SET updated_at = 9999999999999 WHERE uri = ?`, hugeURI); err != nil {
		t.Fatal(err)
	}

	ctx := srv.renderContext("cur", "/repo/alpha", false)
	section := indexSection(t, ctx)
	if !strings.Contains(section, "mem://agent/cases/short") {
		t.Errorf("oversized line starved the pointers behind it:\n%s", section)
	}
	if len(section) > maxIndexContext {
		t.Errorf("index section %d chars, budget %d", len(section), maxIndexContext)
	}
}

// Acceptance (#79): Recent Sessions lists only the current project's sessions
// (last 1-3); project unknown → one line, the most recent session overall.
func TestRecentSessions_ProjectScoped(t *testing.T) {
	srv := testServer(t)
	// Stagger started_at for deterministic recency ordering.
	for i, spec := range []struct{ id, project string }{
		{"s-alpha-1", "/repo/alpha"},
		{"s-alpha-2", "/repo/alpha"},
		{"s-alpha-3", "/repo/alpha"},
		{"s-alpha-4", "/repo/alpha"},
		{"s-beta-1", "/repo/beta"},
	} {
		if _, err := srv.db.InitSession(spec.id, spec.project); err != nil {
			t.Fatal(err)
		}
		if _, err := srv.db.Exec(`UPDATE sessions SET started_at = ? WHERE session_id = ?`, int64(1000+i), spec.id); err != nil {
			t.Fatal(err)
		}
	}

	recentLines := func(ctx string) []string {
		start := strings.Index(ctx, "### Recent Sessions")
		if start < 0 {
			return nil
		}
		var lines []string
		for _, l := range strings.Split(ctx[start:], "\n") {
			if strings.HasPrefix(l, "- [") {
				lines = append(lines, l)
			}
		}
		return lines
	}

	// Known project: alpha sessions only, capped at 3.
	ctx := srv.renderContext("cur", "/repo/alpha", false)
	lines := recentLines(ctx)
	if len(lines) != 3 {
		t.Errorf("project-scoped Recent Sessions = %d lines, want 3:\n%v", len(lines), lines)
	}
	for _, l := range lines {
		if strings.Contains(l, "beta") {
			t.Errorf("cross-project session leaked into Recent Sessions: %s", l)
		}
	}

	// Unknown project: one line, most recent overall (beta, highest started_at).
	ctx = srv.renderContext("cur", "", false)
	lines = recentLines(ctx)
	if len(lines) != 1 {
		t.Fatalf("unknown-project Recent Sessions = %d lines, want 1:\n%v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "beta") {
		t.Errorf("unknown-project line should be the most recent session overall: %s", lines[0])
	}
}

// The current session never lists itself, even when it is the project's most
// recent session.
func TestRecentSessions_ExcludesCurrent(t *testing.T) {
	srv := testServer(t)
	if _, err := srv.db.InitSession("s-cur", "/repo/alpha"); err != nil {
		t.Fatal(err)
	}
	ctx := srv.renderContext("s-cur", "/repo/alpha", false)
	if strings.Contains(ctx, "### Recent Sessions") {
		t.Errorf("current session listed itself:\n%s", ctx)
	}
}
