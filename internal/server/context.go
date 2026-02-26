package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"
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

	// User profile + preferences
	profileNodes, _ := s.db.FindByCategory("profile")
	prefNodes, _ := s.db.FindByCategory("preferences")
	userNodes := append(profileNodes, prefNodes...)
	// Filter out the relational profile (already shown above)
	var filtered []struct{ l0, uri string }
	for _, n := range userNodes {
		if n.URI == "mem://user/profile/communication" {
			continue
		}
		if n.L0Abstract != "" {
			filtered = append(filtered, struct{ l0, uri string }{n.L0Abstract, n.URI})
		}
	}
	if len(filtered) > 0 {
		b.WriteString("\n### Your Profile\n")
		for _, n := range filtered {
			b.WriteString(fmt.Sprintf("- %s\n", n.l0))
		}
	}

	// Recent memories (patterns, events, cases)
	var recentMemories []struct {
		category string
		l0       string
	}
	for _, cat := range []string{"patterns", "events", "cases", "entities"} {
		nodes, err := s.db.FindByCategory(cat)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			if n.L0Abstract != "" && n.Relevance >= 0.3 {
				recentMemories = append(recentMemories, struct {
					category string
					l0       string
				}{cat, n.L0Abstract})
			}
		}
	}
	if len(recentMemories) > 0 {
		b.WriteString("\n### Recent Memories\n")
		limit := 10
		if len(recentMemories) < limit {
			limit = len(recentMemories)
		}
		for _, m := range recentMemories[:limit] {
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
