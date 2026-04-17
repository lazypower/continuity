package server

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

func (s *Server) handleGetContext(w http.ResponseWriter, r *http.Request) {
	ctx := s.buildContext(r.URL.Query().Get("session_id"))

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
	maxContextTotal       = 4000 // total character budget for entire context block
	maxRelationalContext  = 1000 // budget for relational profile section
	maxItemContext        = 200  // budget per L0 memory item
	maxContextItems       = 15   // max items considered (budget usually cuts off earlier)
)

// buildContext creates the context markdown for session injection.
// Enforces a hard character budget to prevent context bloat.
func (s *Server) buildContext(currentSessionID string) string {
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

	// Relational profile (Working With You) — capped portion of budget
	relProfile, err := s.db.GetNodeByURI("mem://user/profile/communication")
	if err == nil && relProfile != nil && relProfile.L1Overview != "" {
		section := "\n### Working With You\n"
		content := relProfile.L1Overview
		if len(content) > maxRelationalContext {
			log.Printf("context: relational profile truncated at output (%d → %d chars) — extraction may be drifting", len(content), maxRelationalContext)
			content = truncateAtSentence(content, maxRelationalContext)
		}
		section += content + "\n"
		b.WriteString(section)
		budget -= len(section)
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
		selected := s.selectDiverseMoments(moments, 3)
		if len(selected) > 0 {
			section := "\n### Moments\n"
			for _, m := range selected {
				l0 := m.L0Abstract
				if len(l0) > maxItemContext {
					l0 = truncateAtSentence(l0, maxItemContext)
				}
				section += fmt.Sprintf("- %s\n", l0)
				// Touch for rotation tracking — next session deprioritizes these
				s.db.TouchNode(m.URI)
			}
			b.WriteString(section)
			budget -= len(section)
		}
	}

	// Collect all non-relational leaves, rank by signal strength
	type rankedItem struct {
		category string
		l0       string
		score    float64
	}
	var items []rankedItem

	for _, cat := range []string{"profile", "preferences", "patterns", "events", "cases", "entities"} {
		nodes, err := s.db.FindByCategory(cat)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			if n.URI == "mem://user/profile/communication" {
				continue // already shown above
			}
			if n.L0Abstract == "" || n.Relevance < 0.3 {
				continue
			}
			score := nodeScore(n)
			items = append(items, rankedItem{cat, n.L0Abstract, score})
		}
	}

	// Sort by score descending
	sort.Slice(items, func(i, j int) bool {
		return items[i].score > items[j].score
	})
	if len(items) > maxContextItems {
		items = items[:maxContextItems]
	}

	// Split into profile/prefs vs other, enforcing per-item and total budget
	var profileLines, memoryLines []string
	itemsUsed := 0

	for _, it := range items {
		l0 := it.l0
		if len(l0) > maxItemContext {
			log.Printf("context: L0 truncated at output for [%s] (%d → %d chars) — extraction may be drifting", it.category, len(l0), maxItemContext)
			l0 = truncateAtSentence(l0, maxItemContext)
		}

		var line string
		if it.category == "profile" || it.category == "preferences" {
			line = fmt.Sprintf("- %s\n", l0)
		} else {
			line = fmt.Sprintf("- [%s] %s\n", it.category, l0)
		}

		if itemBudget-len(line) < 0 {
			log.Printf("context: budget exhausted after %d items (dropped %d)", itemsUsed, len(items)-itemsUsed)
			break
		}
		itemBudget -= len(line)
		itemsUsed++

		if it.category == "profile" || it.category == "preferences" {
			profileLines = append(profileLines, line)
		} else {
			memoryLines = append(memoryLines, line)
		}
	}

	if len(profileLines) > 0 {
		b.WriteString("\n### Your Profile\n")
		for _, line := range profileLines {
			b.WriteString(line)
		}
	}

	if len(memoryLines) > 0 {
		b.WriteString("\n### Recent Memories\n")
		for _, line := range memoryLines {
			b.WriteString(line)
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

// nodeScore ranks a memory node for context injection priority.
// Higher = more important to include. Combines relevance (decay-adjusted)
// with access frequency (memories the agent actually uses stay prominent).
func nodeScore(n store.MemNode) float64 {
	accessBoost := 1.0
	if n.AccessCount > 0 {
		// Diminishing returns: log2(access+1) gives 1→1.0, 2→1.58, 4→2.32, 8→3.17
		accessBoost = 1.0 + math.Log2(float64(n.AccessCount))
	}
	return n.Relevance * accessBoost
}
