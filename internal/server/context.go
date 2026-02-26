package server

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/store"
)

func (s *Server) handleGetContext(w http.ResponseWriter, r *http.Request) {
	ctx := s.buildContext(r.URL.Query().Get("session_id"))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"context": ctx,
	})
}

// buildContext creates the context markdown for session injection.
// Phase 2 upgrade: includes relational profile, memory L1s, and session list.
func (s *Server) buildContext(currentSessionID string) string {
	var b strings.Builder

	b.WriteString("<context>\n## Continuity — Session Memory\n")

	// Relational profile (Working With You)
	relProfile, err := s.db.GetNodeByURI("mem://user/profile/communication")
	if err == nil && relProfile != nil && relProfile.L1Overview != "" {
		b.WriteString("\n### Working With You\n")
		b.WriteString(relProfile.L1Overview)
		b.WriteString("\n")
	}

	// Collect all non-relational leaves, rank by signal strength, cap at 15 total.
	// This prevents the context wall-of-text problem.
	const maxContextItems = 15

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
			// Score: relevance weighted by access frequency
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

	// Split into profile/prefs vs other for display
	var profileItems, memoryItems []rankedItem
	for _, it := range items {
		if it.category == "profile" || it.category == "preferences" {
			profileItems = append(profileItems, it)
		} else {
			memoryItems = append(memoryItems, it)
		}
	}

	if len(profileItems) > 0 {
		b.WriteString("\n### Your Profile\n")
		for _, n := range profileItems {
			b.WriteString(fmt.Sprintf("- %s\n", n.l0))
		}
	}

	if len(memoryItems) > 0 {
		b.WriteString("\n### Recent Memories\n")
		for _, m := range memoryItems {
			b.WriteString(fmt.Sprintf("- [%s] %s\n", m.category, m.l0))
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
			b.WriteString(fmt.Sprintf("- [%s] %s: %s (%d tools used)\n", ts, project, sess.Status, sess.ToolCount))
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
