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
	// project is the normalized repository identity forwarded by the start
	// hook (#79) — it scopes the corpus index and Recent Sessions at t=0,
	// before the sessions row exists.
	ctx := s.renderContext(r.URL.Query().Get("session_id"), r.URL.Query().Get("project"), preview)

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
	// maxPinnedItems is the cold-boot cap on the ### Pinned section. It tracks
	// store.MaxPins, which is enforced at pin *write* time — so this cap is
	// defense-in-depth that never actually fires (listed pins == injected pins).
	maxPinnedItems = store.MaxPins
	// maxIndexContext caps the ### Memory Index section (#79, ADR-001 §3):
	// shape line plus project-affine L0 pointers — a fraction of what the
	// retired Recent Memories window spent.
	maxIndexContext = 600
	// maxIndexAffineNodes caps how many affine pointers may render; the char
	// budget above usually cuts in first. The query overfetches by the pin
	// cap so nodes already surfaced on the tray (pins, the relational
	// profile) can't occupy every result slot and starve the index into a
	// false shape-only render.
	maxIndexAffineNodes = 8
	// maxRecentProjectSessions caps Recent Sessions for a known project.
	// Project unknown → one line, the most recent session overall.
	maxRecentProjectSessions = 3
)

// buildContext creates the context markdown for a real session injection.
// Side effects — moment rotation bookkeeping (AdvanceRotation) and `shown`
// telemetry — fire on this path only; this is the SessionStart path. For a
// side-effect-free render (the Cold Boot preview), use renderContext(sessionID, project, true).
func (s *Server) buildContext(currentSessionID string) string {
	return s.renderContext(currentSessionID, "", false)
}

// renderContext builds the context markdown. project is the normalized
// repository identity (empty = unknown); it scopes the corpus index and
// Recent Sessions. When preview is true, it makes no
// writes — moment rotation is NOT advanced — so callers can show exactly what a
// cold SessionStart would inject without consuming the rotation that injection
// would. Enforces a hard character budget to prevent context bloat.
func (s *Server) renderContext(currentSessionID, project string, preview bool) string {
	var b strings.Builder
	budget := maxContextTotal

	// A resumed session whose start hook predates #79's project forwarding
	// still has a sessions row that knows the project.
	if project == "" && currentSessionID != "" {
		if sess, err := s.db.GetSession(currentSessionID); err == nil && sess != nil {
			project = sess.Project
		}
	}

	now := time.Now()
	header := fmt.Sprintf("<context>\n## Continuity — Session Memory\nCurrent: %s\n", now.Format("2006-01-02 15:04 (Mon)"))
	b.WriteString(header)
	budget -= len(header)

	// Gap signal: if last session on this project was >7 days ago, flag it.
	// Project-scoped when identity is known (#79) — a gap on someone else's
	// project says nothing about this one.
	lastSessions, lastErr := s.db.GetRecentSessions(1)
	if project != "" {
		lastSessions, lastErr = s.db.GetRecentProjectSessions(project, 1)
	}
	if lastErr == nil && len(lastSessions) > 0 {
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

	// surfacedURIs records every node already rendered on the tray so later
	// sections (pins dedupe, moments, the corpus index) don't render the same
	// node twice.
	surfacedURIs := make(map[string]bool)

	// ledgerURIs mirrors the `shown` events recorded on this (real) injection
	// path. The journal rows ride the buffered recorder and may still be
	// pending when the first prompt hits the gate, so they are also claimed
	// into the gate's synchronous in-memory ledger at the end of this render —
	// otherwise the gate could re-inject a URI the tray just surfaced
	// (#80, Codex round 2).
	var ledgerURIs []string

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
		surfacedURIs[relProfile.URI] = true
		if !preview {
			s.events.record("shown", "tray", relProfile.URI, currentSessionID)
			ledgerURIs = append(ledgerURIs, relProfile.URI)
		}
	}

	// Operator pins (declared contract) — the highest-priority resident content
	// after the relational profile. These are memories the operator explicitly
	// pinned to the tray; they ride every cold boot regardless of recency or
	// relevance. ListPinned excludes retracted nodes at the store layer, so a
	// pinned-then-retracted memory goes silent here without any check in this
	// function — the single retraction chokepoint.
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
				surfacedURIs[p.URI] = true
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
			surfacedURIs[p.URI] = true
			used++
			if !preview {
				s.events.record("shown", "tray", p.URI, currentSessionID)
				ledgerURIs = append(ledgerURIs, p.URI)
			}
		}
		if section != pinnedHeader {
			b.WriteString(section)
			budget -= len(section)
		}
	}

	// Inject moments — small, permanent, high-value relational anchors
	// Uses diversity sampling: rotation via last_access, greedy max-diversity selection
	moments, err := s.db.FindByCategory("moments")
	if err == nil && len(moments) > 0 {
		// Drop any moment already shown as a pin so it isn't rendered twice.
		if len(surfacedURIs) > 0 {
			live := moments[:0]
			for _, m := range moments {
				if !surfacedURIs[m.URI] {
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
					ledgerURIs = append(ledgerURIs, m.URI)
				}
			}
			b.WriteString(section)
			budget -= len(section)
		}
	}

	// The enumerated contract tray ("### Your Profile" — profile/preferences/
	// feedback dumped as a ranked list) is removed. It was auto-injected noise:
	// mis-categorized and cross-project entries leaked into every cold boot, and
	// nothing downstream depended on it (retrieval is semantic; injection is
	// earned). The stance that earns the push lives in the synthesized "Working
	// With You" profile above and in the Pinned section; every other memory is
	// pull (search) — recognition over recall. A tags/index refactor that lets
	// the human browse loosely while the agent searches is the next-release
	// adjustment; see the memory-injection north star ("make categorization
	// irrelevant, not better").

	// Corpus index (#79, ADR-001 §3): the tray carries the corpus's shape —
	// per-category counts — plus L0 pointers for nodes affine to the current
	// project. Push the pointers, pull the payloads: the agent cannot search
	// for what it does not know exists. Project unknown, or zero affine nodes
	// → shape-only; less content, never guessed content. Rendering mutates no
	// node state (shown is not use, ADR-001 §2) — the only write is `shown`
	// telemetry on the real injection path, recorded after the section commits
	// so a budget-dropped section can't inflate the exposure denominator.
	if counts, err := s.db.CountLeavesByCategory(); err == nil && len(counts) > 0 {
		section := "\n### Memory Index\n" + indexShapeLine(counts)
		var indexShown []string
		if project != "" {
			// Overfetch by the pin cap + 1 (relational profile): every
			// surfaced node skipped below still leaves a candidate behind it.
			if affine, err := s.db.FindProjectAffine(project, maxIndexAffineNodes+maxPinnedItems+1); err == nil {
				const affineHeader = "This project:\n"
				lines := ""
				for _, n := range affine {
					if len(indexShown) >= maxIndexAffineNodes {
						break
					}
					if surfacedURIs[n.URI] || n.L0Abstract == "" {
						continue
					}
					l0 := n.L0Abstract
					if len(l0) > maxItemContext {
						l0 = truncateAtSentence(l0, maxItemContext)
					}
					line := fmt.Sprintf("- %s (%s)\n", l0, n.URI)
					// Skip, don't stop: one oversized line (a long URI) must
					// not starve every shorter pointer behind it.
					if len(section)+len(affineHeader)+len(lines)+len(line) > maxIndexContext {
						continue
					}
					lines += line
					indexShown = append(indexShown, n.URI)
				}
				if lines != "" {
					section += affineHeader + lines
				}
			}
		}
		if budget-len(section) >= 0 {
			b.WriteString(section)
			budget -= len(section)
			if !preview {
				for _, uri := range indexShown {
					s.events.record("shown", "index", uri, currentSessionID)
				}
				ledgerURIs = append(ledgerURIs, indexShown...)
			}
		} else {
			log.Printf("context: budget exhausted before memory index (%d chars left)", budget)
		}
	}

	// Recent Sessions — project-scoped (#79, ADR-001 §3): booting into a
	// project, the likeliest continuation is that project's last session, so
	// resumption is tray-worthy before knowing the operation. Cross-project
	// history lives behind the index as counts. Project unknown → one line,
	// the most recent session overall. Fixed recency, no ranking, no touch
	// mechanics.
	maxRecent := maxRecentProjectSessions
	var sessions []store.Session
	var sessErr error
	if project != "" {
		sessions, sessErr = s.db.GetRecentProjectSessions(project, maxRecent+1)
	} else {
		maxRecent = 1
		sessions, sessErr = s.db.GetRecentSessions(maxRecent + 1)
	}
	if sessErr == nil && len(sessions) > 0 {
		var lines strings.Builder
		rendered := 0
		for _, sess := range sessions {
			if sess.SessionID == currentSessionID {
				continue
			}
			if rendered >= maxRecent {
				break
			}
			ts := time.UnixMilli(sess.StartedAt).Format("2006-01-02 15:04")
			name := sess.Project
			if name == "" {
				name = "unknown"
			} else {
				name = filepath.Base(name)
			}
			toneSuffix := ""
			if sess.Tone != nil && *sess.Tone != "" {
				toneSuffix = fmt.Sprintf(" — %s", *sess.Tone)
			}
			lines.WriteString(fmt.Sprintf("- [%s] %s: %s (%d tools used)%s\n", ts, name, sess.Status, sess.ToolCount, toneSuffix))
			rendered++
		}
		if lines.Len() > 0 {
			b.WriteString("\n### Recent Sessions\n")
			b.WriteString(lines.String())
		}
	}

	// Current session info
	if currentSessionID != "" {
		count, err := s.db.GetSessionObservationCount(currentSessionID)
		if err == nil && count > 0 {
			b.WriteString(fmt.Sprintf("\n### Current Session\n%d tool uses recorded this session\n", count))
		}
	}

	// Seed the gate's synchronous ledger with everything this render surfaced,
	// so the async journal write can never race the session's first prompt
	// into a double-injection (see ledgerURIs above).
	if !preview && currentSessionID != "" && len(ledgerURIs) > 0 {
		s.gateSessions.claim(currentSessionID, ledgerURIs)
	}

	b.WriteString("</context>")
	return b.String()
}

// indexCategoryOrder fixes the shape line's rendering order: contract
// categories first, then the episodic corpus, then moments. Purely
// presentational — the counts carry no ranking.
var indexCategoryOrder = []string{"profile", "preferences", "feedback", "entities", "events", "patterns", "cases", "reference", "moments"}

// indexShapeLine renders the corpus shape — total plus per-category live-leaf
// counts (#79). Categories outside the fixed order (schema drift) still
// render, sorted, so the shape never silently under-reports the corpus.
func indexShapeLine(counts map[string]int) string {
	total := 0
	for _, n := range counts {
		total += n
	}
	parts := make([]string, 0, len(counts))
	seen := make(map[string]bool, len(counts))
	for _, c := range indexCategoryOrder {
		if counts[c] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", c, counts[c]))
		}
		seen[c] = true
	}
	var extras []string
	for c := range counts {
		if !seen[c] {
			extras = append(extras, c)
		}
	}
	sort.Strings(extras)
	for _, c := range extras {
		parts = append(parts, fmt.Sprintf("%s %d", c, counts[c]))
	}
	return fmt.Sprintf("%d memories: %s\n", total, strings.Join(parts, ", "))
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
