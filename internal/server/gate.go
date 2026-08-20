package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/lazypower/continuity/internal/config"
	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

// Prompt gate (ADR-001 §4, #80): a per-prompt, project-scoped, pure-vector
// recall check. Find-mode ONLY — LLM-assisted search is banned on this path,
// permanently: the gate is synchronous ahead of the user's prompt, and the
// agent's budget carries content.
//
// The gate fails closed as a contract: every degraded state on the server side
// (no engine, no embedder, identity lock, scope-query failure, find failure)
// resolves to a non-200 that the hook swallows into silence with exit 0. The
// only 200 bodies this handler produces are ones that are safe to act on.
const (
	// gateTopK is how many hit pointers ride each calibration row.
	gateTopK = 5
	// gateFindBudget bounds the embed+scan on the server side. The hook's own
	// gate client budget (hooks.gateTimeout) is the outer wall; this inner one
	// keeps a slow scan from holding the request until the client gives up.
	gateFindBudget = 800 * time.Millisecond
	// gateMaxPromptChars caps how much prompt text is embedded. A giant paste
	// is bounded compute, not an error: the leading window carries the query
	// signal, and the ctx budget alone cannot interrupt a local embedder
	// mid-computation.
	gateMaxPromptChars = 8192
)

// gateGlobalCategories are matched WITHOUT project affinity (#80 design call):
// contract categories are user-global by taxonomy — a preference or feedback
// rule is about how the operator works, not about any repository — so project
// scoping would only hide them. The backtest's strongest exemplars were global
// preferences; anything already on the tray is suppressed by the dedupe
// ledger, so the overlap costs nothing. Episodic categories stay project-
// scoped to trim cross-project noise. Moments are excluded outright — they
// have their own diversity-sampled tray channel (and their categoryBoost
// would otherwise let score ordering diverge from the pure-similarity τ).
var gateGlobalCategories = map[string]bool{
	"profile":     true,
	"preferences": true,
	"feedback":    true,
}

// maxLedgerSessions bounds the in-memory dedupe ledger. Sessions beyond the
// cap evict FIFO; the mem_events journal remains the durable backstop, so an
// evicted session degrades to journal-only dedupe, never to unbounded memory.
const maxLedgerSessions = 64

// gateLedger is the synchronous half of the per-session dedupe ledger (#80).
// The durable half is the mem_events `shown` journal — but tray/index/moments
// rows arrive through the buffered recorder, and even the gate's own journal
// write lands after the response, so two prompts in quick succession could
// both clear the journal check before either row exists. This map closes that
// window: injection decisions are recorded here synchronously, under the
// lock, before the response is written.
type gateLedger struct {
	mu       sync.Mutex
	sessions map[string]map[string]bool
	order    []string // insertion order for FIFO eviction
}

func newGateLedger() *gateLedger {
	return &gateLedger{sessions: make(map[string]map[string]bool)}
}

// claim marks the URIs as surfaced to the session and reports which of them
// were NOT already claimed. Check-and-set is a single critical section so two
// concurrent gate calls for the same session cannot both claim the same URI.
func (l *gateLedger) claim(sessionID string, uris []string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	set, ok := l.sessions[sessionID]
	if !ok {
		set = make(map[string]bool)
		l.sessions[sessionID] = set
		l.order = append(l.order, sessionID)
		if len(l.order) > maxLedgerSessions {
			delete(l.sessions, l.order[0])
			l.order = l.order[1:]
		}
	}
	var fresh []string
	for _, u := range uris {
		if !set[u] {
			set[u] = true
			fresh = append(fresh, u)
		}
	}
	return fresh
}

// gateHit is one injectable pointer: L0 + mem:// URI, never a payload
// (ADR-001 §4 — the agent deepens to L2 by choice, which is the use signal).
type gateHit struct {
	URI        string  `json:"uri"`
	L0Abstract string  `json:"l0_abstract"`
	Similarity float64 `json:"similarity"`
}

// handleGate runs the prompt gate for one user prompt. POST body:
// {session_id, project, prompt}. The prompt text is embedded and discarded —
// it is never persisted (the calibration row records only its length).
func (s *Server) handleGate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
		Project   string `json:"project"`
		Prompt    string `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Prompt == "" {
		// Nothing to embed; Claude Code never submits an empty prompt, so this
		// is a malformed caller, not a terse prompt ("yes" still gets a row).
		jsonError(w, "prompt required", http.StatusBadRequest)
		return
	}

	mode := config.NormalizedGateMode(s.gateMode)
	if mode == config.GateOff {
		w.Header().Set("Content-Type", "application/json")
		encodeJSON(w, map[string]any{"mode": config.GateOff})
		return
	}

	// Degraded states fail closed (ADR-001 §4): no embedder, or a vector
	// identity mismatch (τ is embedder-specific — a mismatch auto-disables the
	// gate until recalibrated) → non-200, which the hook resolves to silence.
	if s.engine == nil || s.engine.Embedder == nil {
		jsonError(w, "gate not available — no embedder configured", http.StatusServiceUnavailable)
		return
	}
	if locked, reason := s.engine.VectorIdentityLocked(); locked {
		jsonError(w, reason, http.StatusServiceUnavailable)
		return
	}

	// Project scope: episodic nodes must be affine to the session's project
	// via source_session → sessions.project (#79's join, as a set).
	var affine map[string]bool
	if req.Project != "" {
		var err error
		affine, err = s.db.SessionIDsForProject(req.Project)
		if err != nil {
			log.Printf("gate: session ids for project: %v", err)
			jsonError(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	scope := func(n store.MemNode) bool {
		if n.Category == "moments" {
			return false
		}
		if gateGlobalCategories[n.Category] {
			return true
		}
		return n.SourceSession != "" && affine[n.SourceSession]
	}

	prompt := req.Prompt
	if len(prompt) > gateMaxPromptChars {
		prompt = prompt[:gateMaxPromptChars]
	}

	ctx, cancel := context.WithTimeout(r.Context(), gateFindBudget)
	defer cancel()

	// Find is read-idempotent on node state (ADR-001 §2), which is what makes
	// repeated identical prompts mutate nothing — the gate adds only its own
	// bounded calibration row, in its own table.
	results, err := engine.Find(ctx, s.db, s.engine.Embedder, prompt, engine.SearchOpts{
		Limit: gateTopK,
		Scope: scope,
	})
	if err != nil {
		log.Printf("gate: find: %v", err)
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	// One calibration row per prompt, terse or not — the τ distribution needs
	// the misses as much as the hits. Buffered fire-and-forget: the prompt
	// never waits for its own telemetry, and the write bounds the table (#72).
	maxSim := 0.0
	type simHit struct {
		URI string  `json:"uri"`
		Sim float64 `json:"sim"`
	}
	topHits := make([]simHit, 0, len(results))
	for _, res := range results {
		if res.Similarity > maxSim {
			maxSim = res.Similarity
		}
		topHits = append(topHits, simHit{URI: res.Node.URI, Sim: res.Similarity})
	}
	hitsJSON, _ := json.Marshal(topHits)
	s.events.recordCalibration(store.GateCalibration{
		SessionID:   req.SessionID,
		Project:     req.Project,
		PromptChars: len(req.Prompt),
		MaxSim:      maxSim,
		TopHits:     string(hitsJSON),
	})

	// Injection (mode "on" only — shadow returns an empty inject list, always):
	// hits at or above τ, deduped against everything this session has already
	// been shown on ANY surface (tray, index, moments, search, prior gate
	// hits). The pass-2 calibration finding says redundancy with already-
	// injected context is the dominant false-positive mode, so the ledger is a
	// precision feature, not hygiene.
	var inject []gateHit
	if mode == config.GateOn {
		var above []engine.SearchResult
		for _, res := range results {
			if res.Similarity >= s.gateTau {
				above = append(above, res)
			}
		}
		if len(above) > 0 {
			shown, err := s.db.ShownURIsForSession(req.SessionID)
			if err != nil {
				// Fail closed to silence: without the durable ledger we cannot
				// prove a hit is fresh, and a duplicate line is the known
				// failure mode. Calibration is already recorded.
				log.Printf("gate: shown uris for session: %v (injecting nothing)", err)
				above = nil
			}
			var candidates []string
			byURI := make(map[string]engine.SearchResult, len(above))
			for _, res := range above {
				if !shown[res.Node.URI] {
					candidates = append(candidates, res.Node.URI)
					byURI[res.Node.URI] = res
				}
			}
			// Synchronous claim closes the async-journal race; a claim whose
			// response write later fails costs one suppressed re-injection,
			// which the precision-over-recall stance accepts.
			for _, uri := range s.gateSessions.claim(req.SessionID, candidates) {
				res := byURI[uri]
				inject = append(inject, gateHit{
					URI:        res.Node.URI,
					L0Abstract: res.Node.L0Abstract,
					Similarity: res.Similarity,
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(map[string]any{
		"mode":           mode,
		"max_similarity": maxSim,
		"tau":            s.gateTau,
		"inject":         inject,
	})

	// `shown` journal rows for injected hits only, and only after the response
	// write succeeds (ADR-001 §5: a canceled client must not inflate the
	// used-given-shown denominator). Shadow mode reaches here with an empty
	// inject list and journals nothing — shadow hits were shown to nobody.
	//
	// Written SYNCHRONOUSLY, not through the buffered recorder: these rows are
	// the durable half of the dedupe ledger, and a dropped row plus a ledger
	// eviction or restart would re-inject the same URI (Codex round 1). The
	// client already has its response, so only this handler goroutine waits —
	// the prompt-latency contract is untouched, and a failed insert costs one
	// possible duplicate line, logged.
	if err != nil {
		log.Printf("gate: response write failed, %d hit(s) not journaled as shown: %v", len(inject), err)
		return
	}
	for _, h := range inject {
		if err := s.db.InsertEvent(store.MemEvent{
			NodeURI: h.URI, Event: "shown", Surface: "gate", SessionID: req.SessionID,
		}); err != nil {
			log.Printf("gate: shown journal write failed for %s: %v (dedupe falls back to the in-memory ledger)", h.URI, err)
		}
	}
}
