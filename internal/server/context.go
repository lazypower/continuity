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

// buildContext creates the Phase 1 minimal context markdown.
func (s *Server) buildContext(currentSessionID string) string {
	var b strings.Builder

	b.WriteString("<context>\n## Continuity — Session Memory\n")

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

	// Previous session tool summary
	if len(sessions) > 0 {
		var prevSession *struct {
			sessionID string
			project   string
		}
		for _, sess := range sessions {
			if sess.SessionID != currentSessionID {
				prevSession = &struct {
					sessionID string
					project   string
				}{sess.SessionID, sess.Project}
				break
			}
		}
		if prevSession != nil {
			obs, err := s.db.GetObservations(prevSession.sessionID)
			if err == nil && len(obs) > 0 {
				project := prevSession.project
				if project == "" {
					project = "unknown"
				} else {
					project = filepath.Base(project)
				}
				b.WriteString(fmt.Sprintf("\n### Previous Session\nLast worked on: %s\n- ", project))
				tools := make([]string, 0, len(obs))
				seen := make(map[string]bool)
				for _, o := range obs {
					if !seen[o.ToolName] && len(tools) < 5 {
						tools = append(tools, o.ToolName)
						seen[o.ToolName] = true
					}
				}
				b.WriteString(strings.Join(tools, ", "))
				b.WriteString("\n")
			}
		}
	}

	b.WriteString("</context>")
	return b.String()
}
