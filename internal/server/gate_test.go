package server

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/config"
	"github.com/lazypower/continuity/internal/store"
)

// The prompt gate's issue #80 acceptance criteria, pinned at the HTTP surface.
// Hook-side degraded states (server stopped, past budget, malformed body) are
// pinned in internal/hooks/gate_test.go.

func postGate(t *testing.T, srv *Server, sessionID, project, prompt string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"session_id": sessionID,
		"project":    project,
		"prompt":     prompt,
	})
	req := newTestRequest("POST", "/api/gate", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w.Code, w.Body.String()
}

type gateResp struct {
	Mode          string  `json:"mode"`
	MaxSimilarity float64 `json:"max_similarity"`
	Tau           float64 `json:"tau"`
	Inject        []struct {
		URI        string  `json:"uri"`
		L0Abstract string  `json:"l0_abstract"`
		Similarity float64 `json:"similarity"`
	} `json:"inject"`
}

func decodeGate(t *testing.T, body string) gateResp {
	t.Helper()
	var r gateResp
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatalf("decode gate response %q: %v", body, err)
	}
	return r
}

// seedProjectLeaf seeds an episodic leaf whose source session belongs to the
// given project, so it is in the gate's project scope.
func seedProjectLeaf(t *testing.T, srv *Server, uri, category, l0, project, sessionID string) {
	t.Helper()
	if _, err := srv.db.InitSession(sessionID, project); err != nil {
		t.Fatalf("init session %s: %v", sessionID, err)
	}
	if err := srv.db.CreateNode(&store.MemNode{
		URI: uri, NodeType: "leaf", Category: category,
		L0Abstract: l0, L1Overview: l0 + " (L1 body)",
		SourceSession: sessionID,
	}); err != nil {
		t.Fatalf("seed %s: %v", uri, err)
	}
	n, _ := srv.db.GetNodeByURI(uri)
	if err := srv.engine.EmbedNode(t.Context(), n); err != nil {
		t.Fatalf("embed %s: %v", uri, err)
	}
}

// Criterion: a terse prompt logs a calibration event and injects nothing.
func TestGate_TersePromptLogsAndInjectsNothing(t *testing.T) {
	srv := acceptanceServer(t) // default mode: shadow
	seedLeaf(t, srv, "mem://agent/cases/scar", "cases",
		"migration snapshots caused cross-db data loss", "the PR31 scar body")

	code, body := postGate(t, srv, "sess-terse", "", "yes")
	if code != 200 {
		t.Fatalf("gate status = %d: %s", code, body)
	}
	r := decodeGate(t, body)
	if r.Mode != "shadow" {
		t.Errorf("mode = %q, want shadow", r.Mode)
	}
	if len(r.Inject) != 0 {
		t.Errorf("shadow mode injected %d hit(s)", len(r.Inject))
	}

	srv.Close() // drain barrier for the buffered recorder
	n, err := srv.db.CountGateCalibration()
	if err != nil {
		t.Fatalf("count calibration: %v", err)
	}
	if n != 1 {
		t.Errorf("calibration rows = %d, want 1", n)
	}
}

// Criterion: a prompt naming a scarred topic logs high max-similarity and
// still injects nothing in shadow mode — and writes NO shown events (shadow
// hits were shown to nobody; journaling them would corrupt used-given-shown).
func TestGate_ShadowLogsHighSimInjectsNothing(t *testing.T) {
	srv := acceptanceServer(t)
	seedProjectLeaf(t, srv, "mem://agent/cases/scar", "cases",
		"migration snapshots caused cross-db data loss", "/repo/alpha", "sess-src")

	code, body := postGate(t, srv, "sess-1", "/repo/alpha", "migration snapshots caused cross-db data loss")
	if code != 200 {
		t.Fatalf("gate status = %d: %s", code, body)
	}
	r := decodeGate(t, body)
	if r.MaxSimilarity < r.Tau {
		t.Fatalf("max_similarity = %v, want >= tau %v (identical text should clear it)", r.MaxSimilarity, r.Tau)
	}
	if len(r.Inject) != 0 {
		t.Errorf("shadow mode injected %d hit(s)", len(r.Inject))
	}

	srv.Close()
	if n, _ := srv.db.CountEvents("shown"); n != 0 {
		t.Errorf("shadow gate wrote %d shown event(s), want 0", n)
	}
	if n, _ := srv.db.CountGateCalibration(); n != 1 {
		t.Errorf("calibration rows = %d, want 1", n)
	}
}

// Criterion: repeated identical prompts mutate no node state.
func TestGate_RepeatedPromptsMutateNoNodeState(t *testing.T) {
	srv := acceptanceServer(t)
	srv.SetGate(config.GateOn, 0.5) // even with injection on
	seedProjectLeaf(t, srv, "mem://agent/cases/scar", "cases",
		"migration snapshots caused cross-db data loss", "/repo/alpha", "sess-src")

	snapshot := func() string {
		n, _ := srv.db.GetNodeByURI("mem://agent/cases/scar")
		la := "nil"
		if n.LastAccess != nil {
			la = fmt.Sprintf("%d", *n.LastAccess)
		}
		return fmt.Sprintf("rel=%v acc=%d la=%s upd=%d", n.Relevance, n.AccessCount, la, n.UpdatedAt)
	}
	before := snapshot()
	for i := 0; i < 3; i++ {
		if code, body := postGate(t, srv, "sess-1", "/repo/alpha", "migration snapshots caused cross-db data loss"); code != 200 {
			t.Fatalf("gate status = %d: %s", code, body)
		}
	}
	srv.Close() // let telemetry drain, then prove it touched no node column
	if after := snapshot(); after != before {
		t.Errorf("gate mutated node state: %s → %s", before, after)
	}
}

// Criterion (injection flag on): a hit at or above τ injects L0 + URI once;
// the second occurrence is deduped by the session ledger; a sub-τ prompt
// injects zero (median-zero behavior).
func TestGate_InjectionOnceThenDeduped(t *testing.T) {
	srv := acceptanceServer(t)
	srv.SetGate(config.GateOn, 0.5)
	seedProjectLeaf(t, srv, "mem://agent/cases/scar", "cases",
		"migration snapshots caused cross-db data loss", "/repo/alpha", "sess-src")

	// First occurrence: injected.
	code, body := postGate(t, srv, "sess-1", "/repo/alpha", "migration snapshots caused cross-db data loss")
	if code != 200 {
		t.Fatalf("gate status = %d: %s", code, body)
	}
	r := decodeGate(t, body)
	if len(r.Inject) != 1 {
		t.Fatalf("inject = %d hit(s), want 1: %s", len(r.Inject), body)
	}
	if r.Inject[0].URI != "mem://agent/cases/scar" {
		t.Errorf("injected URI = %q", r.Inject[0].URI)
	}
	if r.Inject[0].L0Abstract == "" {
		t.Error("injected hit missing L0")
	}

	// Second occurrence, same session: deduped to zero.
	_, body2 := postGate(t, srv, "sess-1", "/repo/alpha", "migration snapshots caused cross-db data loss")
	if r2 := decodeGate(t, body2); len(r2.Inject) != 0 {
		t.Errorf("second occurrence injected %d hit(s), want 0 (ledger)", len(r2.Inject))
	}

	// Unrelated (median) prompt: zero.
	_, body3 := postGate(t, srv, "sess-1", "/repo/alpha", "thanks, looks good")
	if r3 := decodeGate(t, body3); len(r3.Inject) != 0 {
		t.Errorf("median prompt injected %d hit(s), want 0", len(r3.Inject))
	}

	// Exactly one shown event, surface gate, for the one real injection.
	srv.Close()
	events, err := srv.db.EventsByURI("mem://agent/cases/scar")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	shown := 0
	for _, e := range events {
		if e.Event == "shown" && e.Surface == "gate" {
			shown++
			if e.SessionID != "sess-1" {
				t.Errorf("shown event session = %q, want sess-1", e.SessionID)
			}
		}
	}
	if shown != 1 {
		t.Errorf("shown/gate events = %d, want 1", shown)
	}
}

// The dedupe ledger covers EVERY injection surface, not just prior gate hits:
// a URI already journaled as shown to the session (tray, index, moments,
// search) is never re-injected. Pass-2 calibration: redundancy with already-
// injected context is the dominant false-positive mode.
func TestGate_DedupesAgainstOtherSurfaces(t *testing.T) {
	srv := acceptanceServer(t)
	srv.SetGate(config.GateOn, 0.5)
	seedProjectLeaf(t, srv, "mem://agent/cases/scar", "cases",
		"migration snapshots caused cross-db data loss", "/repo/alpha", "sess-src")

	// The tray already surfaced this URI to the session (e.g. via the corpus
	// index). Insert the journal row synchronously, as the store allows.
	if err := srv.db.InsertEvent(store.MemEvent{
		NodeURI: "mem://agent/cases/scar", Event: "shown", Surface: "index", SessionID: "sess-1",
	}); err != nil {
		t.Fatalf("insert shown: %v", err)
	}

	_, body := postGate(t, srv, "sess-1", "/repo/alpha", "migration snapshots caused cross-db data loss")
	if r := decodeGate(t, body); len(r.Inject) != 0 {
		t.Errorf("gate re-injected a URI already shown on the index surface: %s", body)
	}
}

// Episodic hits are project-scoped: a node from another project's session
// does not surface, while contract categories match globally.
func TestGate_ProjectScopeAndGlobalContract(t *testing.T) {
	srv := acceptanceServer(t)
	srv.SetGate(config.GateOn, 0.5)
	seedProjectLeaf(t, srv, "mem://agent/cases/other-project", "cases",
		"kubernetes ingress certificate rotation playbook", "/repo/beta", "sess-beta")
	seedProjectLeaf(t, srv, "mem://user/preferences/git-remotes", "preferences",
		"git remotes are named by forge not origin", "/repo/beta", "sess-beta2")

	// Episodic node from /repo/beta must not fire for /repo/alpha.
	_, body := postGate(t, srv, "sess-1", "/repo/alpha", "kubernetes ingress certificate rotation playbook")
	if r := decodeGate(t, body); len(r.Inject) != 0 || r.MaxSimilarity >= 0.5 {
		t.Errorf("cross-project episodic hit leaked: %s", body)
	}

	// Contract category (preferences) matches regardless of project affinity.
	_, body2 := postGate(t, srv, "sess-1", "/repo/alpha", "git remotes are named by forge not origin")
	r2 := decodeGate(t, body2)
	if len(r2.Inject) != 1 || r2.Inject[0].URI != "mem://user/preferences/git-remotes" {
		t.Errorf("global contract hit missing: %s", body2)
	}
}

// Mode off answers without searching or logging; no embedder fails closed 503.
func TestGate_OffAndDegradedStates(t *testing.T) {
	srv := acceptanceServer(t)
	srv.SetGate(config.GateOff, 0.5)
	code, body := postGate(t, srv, "sess-1", "", "anything")
	if code != 200 || !strings.Contains(body, `"off"`) {
		t.Fatalf("off mode: %d %s", code, body)
	}
	srv.Close()
	if n, _ := srv.db.CountGateCalibration(); n != 0 {
		t.Errorf("off mode logged %d calibration row(s), want 0", n)
	}

	// No engine at all → 503 (the hook swallows this into silence).
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	bare := New(db, nil, "test")
	t.Cleanup(bare.Close)
	if code, _ := postGate(t, bare, "sess-1", "", "anything"); code != 503 {
		t.Errorf("no-engine gate status = %d, want 503", code)
	}
}

// SetGate never lets a non-"on" string enable injection, and a garbage tau
// keeps the default rather than widening the gate.
func TestSetGate_FailSafeNormalization(t *testing.T) {
	srv := acceptanceServer(t)
	srv.SetGate("inject-everything", -3)
	if srv.gateMode != config.GateShadow {
		t.Errorf("gateMode = %q, want shadow", srv.gateMode)
	}
	if srv.gateTau != config.DefaultGateTau {
		t.Errorf("gateTau = %v, want default %v", srv.gateTau, config.DefaultGateTau)
	}
}

// The in-memory ledger is bounded: sessions beyond the cap evict FIFO and
// the journal remains the durable backstop.
func TestGateLedger_BoundedFIFO(t *testing.T) {
	l := newGateLedger()
	for i := 0; i < maxLedgerSessions+5; i++ {
		l.claim(fmt.Sprintf("sess-%d", i), []string{"mem://x"})
	}
	if len(l.sessions) != maxLedgerSessions {
		t.Errorf("ledger holds %d sessions, cap is %d", len(l.sessions), maxLedgerSessions)
	}
	if _, ok := l.sessions["sess-0"]; ok {
		t.Error("oldest session not evicted")
	}
	// claim is check-and-set: the second claim of the same URI returns nothing.
	if fresh := l.claim("sess-new", []string{"mem://a", "mem://b"}); len(fresh) != 2 {
		t.Fatalf("first claim = %d fresh, want 2", len(fresh))
	}
	if fresh := l.claim("sess-new", []string{"mem://a", "mem://c"}); len(fresh) != 1 || fresh[0] != "mem://c" {
		t.Errorf("second claim = %v, want [mem://c]", fresh)
	}
}
