package store

import (
	"database/sql"
	"fmt"
	"time"
)

// Session represents a Claude Code session.
type Session struct {
	ID           int64
	SessionID    string
	Project      string
	StartedAt    int64
	EndedAt      *int64
	Status       string
	SummaryNode  *int64
	MessageCount int
	ToolCount    int
	ExtractedAt  *int64
}

// InitSession creates or resumes a session. If the session_id already exists
// and is active, it returns the existing session.
func (db *DB) InitSession(sessionID, project string) (*Session, error) {
	now := time.Now().UnixMilli()

	// Try to find existing active session
	var s Session
	err := db.QueryRow(`
		SELECT id, session_id, project, started_at, ended_at, status, summary_node, message_count, tool_count, extracted_at
		FROM sessions WHERE session_id = ? AND status = 'active'
	`, sessionID).Scan(&s.ID, &s.SessionID, &s.Project, &s.StartedAt, &s.EndedAt, &s.Status, &s.SummaryNode, &s.MessageCount, &s.ToolCount, &s.ExtractedAt)
	if err == nil {
		return &s, nil
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("check existing session: %w", err)
	}

	// Create new session
	result, err := db.Exec(`
		INSERT INTO sessions (session_id, project, started_at, status)
		VALUES (?, ?, ?, 'active')
	`, sessionID, project, now)
	if err != nil {
		return nil, fmt.Errorf("insert session: %w", err)
	}

	id, _ := result.LastInsertId()
	return &Session{
		ID:        id,
		SessionID: sessionID,
		Project:   project,
		StartedAt: now,
		Status:    "active",
	}, nil
}

// GetSession returns a session by its session_id.
func (db *DB) GetSession(sessionID string) (*Session, error) {
	var s Session
	err := db.QueryRow(`
		SELECT id, session_id, project, started_at, ended_at, status, summary_node, message_count, tool_count, extracted_at
		FROM sessions WHERE session_id = ?
	`, sessionID).Scan(&s.ID, &s.SessionID, &s.Project, &s.StartedAt, &s.EndedAt, &s.Status, &s.SummaryNode, &s.MessageCount, &s.ToolCount, &s.ExtractedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}
	return &s, nil
}

// CompleteSession marks a session as completed (called on Stop hook).
func (db *DB) CompleteSession(sessionID string) error {
	now := time.Now().UnixMilli()
	result, err := db.Exec(`
		UPDATE sessions SET status = 'completed', ended_at = ?
		WHERE session_id = ? AND status = 'active'
	`, now, sessionID)
	if err != nil {
		return fmt.Errorf("complete session: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("no active session found for %s", sessionID)
	}
	return nil
}

// EndSession finalizes a session (called on SessionEnd hook).
// If still active, marks it as completed.
func (db *DB) EndSession(sessionID string) error {
	now := time.Now().UnixMilli()
	_, err := db.Exec(`
		UPDATE sessions SET status = 'completed', ended_at = COALESCE(ended_at, ?)
		WHERE session_id = ? AND status = 'active'
	`, now, sessionID)
	if err != nil {
		return fmt.Errorf("end session: %w", err)
	}
	return nil
}

// GetRecentSessions returns the most recent sessions, ordered by started_at DESC.
func (db *DB) GetRecentSessions(limit int) ([]Session, error) {
	rows, err := db.Query(`
		SELECT id, session_id, project, started_at, ended_at, status, summary_node, message_count, tool_count, extracted_at
		FROM sessions ORDER BY started_at DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent sessions: %w", err)
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		var s Session
		if err := rows.Scan(&s.ID, &s.SessionID, &s.Project, &s.StartedAt, &s.EndedAt, &s.Status, &s.SummaryNode, &s.MessageCount, &s.ToolCount, &s.ExtractedAt); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

// MarkExtracted sets extracted_at for a session, preventing duplicate extraction.
func (db *DB) MarkExtracted(sessionID string) error {
	now := time.Now().UnixMilli()
	_, err := db.Exec(`UPDATE sessions SET extracted_at = ? WHERE session_id = ?`, now, sessionID)
	if err != nil {
		return fmt.Errorf("mark extracted: %w", err)
	}
	return nil
}

// IncrementToolCount increments the tool_count for a session.
func (db *DB) IncrementToolCount(sessionID string) error {
	_, err := db.Exec(`
		UPDATE sessions SET tool_count = tool_count + 1
		WHERE session_id = ? AND status = 'active'
	`, sessionID)
	if err != nil {
		return fmt.Errorf("increment tool count: %w", err)
	}
	return nil
}
