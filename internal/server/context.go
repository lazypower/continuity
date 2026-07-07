package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

func (s *Server) handleGetContext(w http.ResponseWriter, r *http.Request) {
	// preview=true renders the cold-boot window WITHOUT mutating rotation state
	// (the Cold Boot UI uses this). A real SessionStart injection omits the flag
	// so moment rotation advances. A preview that consumed rotation would change
	// the very thing it claims to show — the panel is an honesty instrument.
	preview := r.URL.Query().Get("preview") == "true"
	ctx := s.renderContext(r.URL.Query().Get("session_id"), preview)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"context": ctx,
	})
}

// Context injection budgets.
// These are defense-in-depth limits — if extraction and validation are working
// correctly, content should already fit. When these fire, it means upstream
// limits drifted and we log a warning so the problem is visible.
const (
	maxContextTotal      = 4000 // total character budget for entire context block
	maxRelationalContext = 1000 // budget for relational profile section
	maxItemContext       = 200  // budget per L0 memory item
	maxContextItems      = 15   // max items considered (budget usually cuts off earlier)
	// maxPinnedItems is the cold-boot cap on the ### Pinned section. It tracks
	// store.MaxPins, which is enforced at pin *write* time — so this cap is
	// defense-in-depth that never actually fires (listed pins == injected pins).
	maxPinnedItems = store.MaxPins
)

// buildContext creates the context markdown for a real session injection.
// Side effects — moment rotation bookkeeping (AdvanceRotation) and `shown`
// telemetry — fire on this path only; this is the SessionStart path. For a
// side-effect-free render (the Cold Boot preview), use renderContext(sessionID, true).
func (s *Server) buildContext(currentSessionID string) string {
	return s.renderContext(currentSessionID, false)
}

// renderContext builds the context markdown. When preview is true, it makes no
// writes — moment rotation is NOT advanced — so callers can show exactly what a
// cold SessionStart would inject without consuming the rotation that injection
// would. Enforces a hard character budget to prevent context bloat.
func (s *Server) renderContext(currentSessionID string, preview bool) string {
	var b strings.Builder
	budget := maxContextTotal

	now := time.Now()
	header := fmt.Sprintf("<context>\n## Continuity — Session Memory\nCurrent: %s\n", now.Format("2006-01-02 15:04 (Mon)"))
	b.WriteString(header)
	budget -= len(header)

	// Gap signal: if last session on this project was >7 days ago, flag it
	if lastSessions, err := s.db.GetRecentSessions(1); err == nil && len(lastSessions) > 0 {
		last := lastSessions[0]
		if last.SessionID != currentSessionID {
			gap := now.Sub(time.UnixMilli(last.StartedAt))
			if gap.Hours() > 7*24 {
				gapLine := fmt.Sprintf("Last session: %d days ago (%s)\n",
					int(gap.Hours()/24),
					time.UnixMilli(last.StartedAt).Format("Jan 2"))
				b.WriteString(gapLine)
				budget -= len(gapLine)
			}
		}
	}

	// Relational profile (Working With You) — capped portion of budget.
	// A retracted profile must not be injected: silently injecting retracted
	// content into every future session would defeat the retraction. No
	// fallback path fills this section if the profile is retracted; the
	// session simply lacks a "Working With You" block until the profile is
	// re-synthesized by a future extraction.
	relProfile, err := s.db.GetNodeByURI("mem://user/profile/communication")
	if err == nil && relProfile != nil && !relProfile.IsRetracted() && relProfile.L1Overview != "" {
		section := "\n### Working With You\n"
		content := relProfile.L1Overview
		if len(content) > maxRelationalContext {
			log.Printf("context: relational profile truncated at output (%d → %d chars) — extraction may be drifting", len(content), maxRelationalContext)
			content = truncateAtSentence(content, maxRelationalContext)
		}
		section += content + "\n"
		b.WriteString(section)
		budget -= len(section)
		if !preview {
			s.events.record("shown", "tray", relProfile.URI, currentSessionID)
		}
	}

	// Operator pins (declared contract) — the highest-priority resident content
	// after the relational profile. These are memories the operator explicitly
	// pinned to the tray; they ride every cold boot regardless of recency or
	// relevance. ListPinned excludes retracted nodes at the store layer, so a
	// pinned-then-retracted memory goes silent here without any check in this
	// function — the single retraction chokepoint. pinnedURIs records what was
	// shown so the ranked sections below don't render the same node twice.
	pinnedURIs := make(map[string]bool)
	if pinned, err := s.db.ListPinned(); err == nil && len(pinned) > 0 {
		const pinnedHeader = "\n### Pinned\n"
		section := pinnedHeader
		used := 0
		for _, p := range pinned {
			if used >= maxPinnedItems {
				log.Printf("context: pinned section capped at %d (operator has %d pins)", maxPinnedItems, len(pinned))
				break
			}
			// The relational profile has its own "Working With You" section above;
			// mark it shown but don't render it twice if the operator pinned it.
			if p.URI == "mem://user/profile/communication" {
				pinnedURIs[p.URI] = true
				continue
			}
			if p.L0Abstract == "" {
				continue
			}
			l0 := p.L0Abstract
			if len(l0) > maxItemContext {
				l0 = truncateAtSentence(l0, maxItemContext)
			}
			line := fmt.Sprintf("- [%s] %s\n", p.Category, l0)
			if budget-len(section)-len(line) < 0 {
				log.Printf("context: budget exhausted in pinned section after %d items", used)
				break
			}
			section += line
			pinnedURIs[p.URI] = true
			used++
			if !preview {
				s.events.record("shown", "tray", p.URI, currentSessionID)
			}
		}
		if section != pinnedHeader {
			b.WriteString(section)
			budget -= len(section)
		}
	}

	// Reserve space for session footer (~300 chars for 5 sessions + current)
	const footerReserve = 400
	itemBudget := budget - footerReserve
	if itemBudget < 0 {
		itemBudget = 0
	}

	// Inject moments — small, permanent, high-value relational anchors
	// Uses diversity sampling: rotation via last_access, greedy max-diversity selection
	moments, err := s.db.FindByCategory("moments")
	if err == nil && len(moments) > 0 {
		// Drop any moment already shown as a pin so it isn't rendered twice.
		if len(pinnedURIs) > 0 {
			live := moments[:0]
			for _, m := range moments {
				if !pinnedURIs[m.URI] {
					live = append(live, m)
				}
			}
			moments = live
		}
		selected := s.selectDiverseMoments(moments, 3)
		if len(selected) > 0 {
			section := "\n### Moments\n"
			for _, m := range selected {
				l0 := m.L0Abstract
				if len(l0) > maxItemContext {
					l0 = truncateAtSentence(l0, maxItemContext)
				}
				section += fmt.Sprintf("- %s\n", l0)
				// Rotation bookkeeping only — last_access moves so the next
				// session deprioritizes these; relevance and counters do NOT
				// (exposure is not use, ADR-001 §2). Skipped in preview: a
				// preview must not consume the rotation it shows.
				if !preview {
					s.db.AdvanceRotation(m.URI)
					s.events.record("shown", "moments", m.URI, currentSessionID)
				}
			}
			b.WriteString(section)
			budget -= len(section)
		}
	}

	// Contract categories only (ADR-001 §1). The episodic ranked window
	// ("Recent Memories" — patterns/events/cases/entities/reference scored by
	// relevance × access popularity) is deleted, not relocated: at t=0 there
	// is no query, so any episodic ranking is prediction from priors, and the
	// measured window surfaced only the already-most-retrieved (12×
	// amplification, issue #50). Episodic surfacing is pull (search) until the
	// §3 index and §4 prompt gate land. What remains on the tray is the
	// contract — profile, preferences, and feedback collapse into "Your
	// Profile" without category tags (feedback is directional guidance that
	// shapes how the agent acts, issue #24). No scoring: contract nodes are
	// decay-exempt with relevance frozen at full, so a rank would order on a
	// dead signal. Category iteration order IS the priority; within a
	// category, FindByCategory's ordering applies. (Flat contract ordering
	// under the exemption is a flagged open item — ADR-001 ⚑.)
	type contractItem struct {
		uri string
		l0  string
	}
	var items []contractItem

	for _, cat := range []string{"profile", "preferences", "feedback"} {
		nodes, err := s.db.FindByCategory(cat)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			if n.URI == "mem://user/profile/communication" {
				continue // already shown above
			}
			if pinnedURIs[n.URI] {
				continue // already shown in the Pinned section
			}
			if n.L0Abstract == "" {
				continue
			}
			items = append(items, contractItem{n.URI, n.L0Abstract})
		}
	}

	if len(items) > maxContextItems {
		log.Printf("context: contract exceeds item cap (%d > %d) — time to curate (merge/retract), see ADR-001 ⚑", len(items), maxContextItems)
		items = items[:maxContextItems]
	}

	var profileLines []string
	var profileURIs []string
	itemsUsed := 0

	for _, it := range items {
		l0 := it.l0
		if len(l0) > maxItemContext {
			log.Printf("context: L0 truncated at output (%d → %d chars) — extraction may be drifting", len(l0), maxItemContext)
			l0 = truncateAtSentence(l0, maxItemContext)
		}

		line := fmt.Sprintf("- %s\n", l0)
		if itemBudget-len(line) < 0 {
			log.Printf("context: budget exhausted after %d items (dropped %d)", itemsUsed, len(items)-itemsUsed)
			break
		}
		itemBudget -= len(line)
		itemsUsed++
		profileLines = append(profileLines, line)
		profileURIs = append(profileURIs, it.uri)
	}

	if len(profileLines) > 0 {
		b.WriteString("\n### Your Profile\n")
		for _, line := range profileLines {
			b.WriteString(line)
		}
		if !preview {
			for _, uri := range profileURIs {
				s.events.record("shown", "tray", uri, currentSessionID)
			}
		}
	}

	// Recent sessions
	sessions, err := s.db.GetRecentSessions(5)
	if err == nil && len(sessions) > 0 {
		b.WriteString("\n### Recent Sessions\n")
		for _, sess := range sessions {
			if sess.SessionID == currentSessionID {
				continue
			}
			ts := time.UnixMilli(sess.StartedAt).Format("2006-01-02 15:04")
			project := sess.Project
			if project == "" {
				project = "unknown"
			} else {
				project = filepath.Base(project)
			}
			toneSuffix := ""
			if sess.Tone != nil && *sess.Tone != "" {
				toneSuffix = fmt.Sprintf(" — %s", *sess.Tone)
			}
			b.WriteString(fmt.Sprintf("- [%s] %s: %s (%d tools used)%s\n", ts, project, sess.Status, sess.ToolCount, toneSuffix))
		}
	}

	// Current session info
	if currentSessionID != "" {
		count, err := s.db.GetSessionObservationCount(currentSessionID)
		if err == nil && count > 0 {
			b.WriteString(fmt.Sprintf("\n### Current Session\n%d tool uses recorded this session\n", count))
		}
	}

	b.WriteString("</context>")
	return b.String()
}

// truncateAtSentence truncates to maxLen, preferring sentence boundaries.
// Falls back to word boundary if no sentence end is found.
func truncateAtSentence(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	truncated := s[:maxLen]
	// Try to find last sentence boundary
	for _, sep := range []string{". ", ".\n", "! ", "? "} {
		if idx := strings.LastIndex(truncated, sep); idx > maxLen/2 {
			return strings.TrimSpace(truncated[:idx+1])
		}
	}
	// Fall back to word boundary
	if idx := strings.LastIndex(truncated, " "); idx > maxLen-100 {
		return strings.TrimSpace(truncated[:idx])
	}
	return strings.TrimSpace(truncated)
}

// selectDiverseMoments picks up to n moments maximizing diversity.
// Algorithm:
//  1. Sort by last_access ascending (null first) — rotation bias toward unseen moments
//  2. First pick = least recently seen moment
//  3. Each subsequent pick = the moment with lowest max-similarity to already-selected
//     (greedy diversity maximization)
//
// Falls back to access-count ordering when embedder is unavailable.
func (s *Server) selectDiverseMoments(moments []store.MemNode, n int) []store.MemNode {
	if len(moments) <= n {
		return moments
	}

	// Sort by last_access ascending — nulls (never injected) first, then oldest
	sort.Slice(moments, func(i, j int) bool {
		if moments[i].LastAccess == nil && moments[j].LastAccess == nil {
			return moments[i].CreatedAt < moments[j].CreatedAt
		}
		if moments[i].LastAccess == nil {
			return true
		}
		if moments[j].LastAccess == nil {
			return false
		}
		return *moments[i].LastAccess < *moments[j].LastAccess
	})

	// Try to load vectors for diversity calculation
	type momentVec struct {
		node store.MemNode
		vec  []float64
	}
	var pool []momentVec
	for _, m := range moments {
		if m.L0Abstract == "" {
			continue
		}
		v, err := s.db.GetVector(m.ID)
		if err != nil || v == nil {
			pool = append(pool, momentVec{m, nil})
			continue
		}
		pool = append(pool, momentVec{m, v.Embedding})
	}

	if len(pool) == 0 {
		return nil
	}

	// Check if we have enough vectors for diversity calculation
	hasVectors := false
	for _, mv := range pool {
		if mv.vec != nil {
			hasVectors = true
			break
		}
	}

	// Fallback: no vectors, just take the first n (already sorted by rotation)
	if !hasVectors {
		result := make([]store.MemNode, 0, n)
		for i := 0; i < n && i < len(pool); i++ {
			result = append(result, pool[i].node)
		}
		return result
	}

	// Greedy diversity selection
	selected := make([]int, 0, n)
	used := make(map[int]bool)

	// First pick: least recently seen (already sorted, so index 0)
	selected = append(selected, 0)
	used[0] = true

	// Remaining picks: minimize max similarity to already-selected
	for len(selected) < n && len(selected) < len(pool) {
		bestIdx := -1
		bestMaxSim := 2.0 // higher than any cosine similarity

		for i := range pool {
			if used[i] || pool[i].vec == nil {
				continue
			}

			// Compute max similarity to any already-selected moment
			maxSim := -1.0
			for _, selIdx := range selected {
				if pool[selIdx].vec == nil {
					continue
				}
				sim := engine.CosineSimilarity(pool[i].vec, pool[selIdx].vec)
				if sim > maxSim {
					maxSim = sim
				}
			}

			// We want the candidate with the LOWEST max-similarity
			// (most different from everything already selected)
			if maxSim < bestMaxSim {
				bestMaxSim = maxSim
				bestIdx = i
			}
		}

		if bestIdx < 0 {
			break
		}
		selected = append(selected, bestIdx)
		used[bestIdx] = true
	}

	result := make([]store.MemNode, 0, len(selected))
	for _, idx := range selected {
		result = append(result, pool[idx].node)
	}
	return result
}

