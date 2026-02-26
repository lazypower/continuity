package store

import (
	"fmt"
	"time"
)

// maxToolResponseSize is the maximum size of tool_response stored in the DB.
// Prevents bloat — Phase 2 extraction processes full transcript anyway.
const maxToolResponseSize = 10 * 1024 // 10KB

// Observation represents a single tool use recorded during a session.
type Observation struct {
	ID           int64
	SessionID    string
	ToolName     string
	ToolInput    string
	ToolResponse string
	CreatedAt    int64
}

// AddObservation stores a tool use observation. Truncates tool_response to 10KB.
func (db *DB) AddObservation(sessionID, toolName, toolInput, toolResponse string) error {
	if len(toolResponse) > maxToolResponseSize {
		toolResponse = toolResponse[:maxToolResponseSize]
	}

	now := time.Now().UnixMilli()
	_, err := db.Exec(`
		INSERT INTO observations (session_id, tool_name, tool_input, tool_response, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, sessionID, toolName, toolInput, toolResponse, now)
	if err != nil {
		return fmt.Errorf("add observation: %w", err)
	}
	return nil
}

// GetObservations returns all observations for a session, ordered by created_at.
func (db *DB) GetObservations(sessionID string) ([]Observation, error) {
	rows, err := db.Query(`
		SELECT id, session_id, tool_name, tool_input, tool_response, created_at
		FROM observations WHERE session_id = ? ORDER BY created_at
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("get observations: %w", err)
	}
	defer rows.Close()

	var obs []Observation
	for rows.Next() {
		var o Observation
		if err := rows.Scan(&o.ID, &o.SessionID, &o.ToolName, &o.ToolInput, &o.ToolResponse, &o.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan observation: %w", err)
		}
		obs = append(obs, o)
	}
	return obs, rows.Err()
}

// GetRecentObservations returns the most recent observations across all sessions.
func (db *DB) GetRecentObservations(limit int) ([]Observation, error) {
	rows, err := db.Query(`
		SELECT id, session_id, tool_name, tool_input, tool_response, created_at
		FROM observations ORDER BY created_at DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent observations: %w", err)
	}
	defer rows.Close()

	var obs []Observation
	for rows.Next() {
		var o Observation
		if err := rows.Scan(&o.ID, &o.SessionID, &o.ToolName, &o.ToolInput, &o.ToolResponse, &o.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan observation: %w", err)
		}
		obs = append(obs, o)
	}
	return obs, rows.Err()
}

// GetSessionObservationCount returns the number of observations for a session.
func (db *DB) GetSessionObservationCount(sessionID string) (int, error) {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM observations WHERE session_id = ?
	`, sessionID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count observations: %w", err)
	}
	return count, nil
}
