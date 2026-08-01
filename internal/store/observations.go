package store

import (
	"fmt"
	"log"
	"os"
	"time"
)

// maxToolFieldSize is the maximum size of tool_input and tool_response stored in the DB.
// Prevents bloat — Phase 2 extraction processes full transcript anyway.
const maxToolFieldSize = 10 * 1024 // 10KB

// Observation represents a single tool use recorded during a session.
type Observation struct {
	ID           int64
	SessionID    string
	ToolName     string
	ToolInput    string
	ToolResponse string
	CreatedAt    int64
}

// AddObservation stores a tool use observation. Truncates large fields to prevent DB bloat.
func (db *DB) AddObservation(sessionID, toolName, toolInput, toolResponse string) error {
	if len(toolInput) > maxToolFieldSize {
		log.Printf("observation: tool_input truncated for session %s: %d → %d bytes", sessionID, len(toolInput), maxToolFieldSize)
		toolInput = toolInput[:maxToolFieldSize]
	}
	if len(toolResponse) > maxToolFieldSize {
		log.Printf("observation: tool_response truncated for session %s: %d → %d bytes", sessionID, len(toolResponse), maxToolFieldSize)
		toolResponse = toolResponse[:maxToolFieldSize]
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

// spentObservationsWhere is the single predicate that defines a "spent"
// observation. An observation exists to serve its own session's live context
// header — GetSessionObservationCount is the only reader in the system. Once a
// session is no longer in flight, nothing will ever read its observations
// again, so they are spent.
//
// Deliberately NOT keyed to extraction: extraction reads the transcript file,
// not this table, and its content gate skips thin sessions without ever marking
// them extracted. Keying retention to extracted_at would couple two unrelated
// things and strand every unextractable session's rows forever.
//
// "In flight" is the mechanism: a session row that is still 'active' AND was
// started recently enough to be plausibly alive. A session left 'active' by a
// crashed or killed client never completes, so past the zombie horizon we stop
// treating it as live. An observation with no session row at all is orphaned
// and therefore not in flight.
//
// Bound parameters, in order: grace cutoff, zombie cutoff.
const spentObservationsWhere = `
	o.created_at < ?
	AND NOT EXISTS (
	    SELECT 1 FROM sessions s
	    WHERE s.session_id = o.session_id
	      AND s.status = 'active'
	      AND s.started_at >= ?
	)`

// CountSpentObservations reports how many rows the sweep would reclaim right
// now (same predicate, no delete), so a growing pile is visible in /api/health
// rather than only after it has already cost someone a debugging session.
func (db *DB) CountSpentObservations(graceCutoffMs, zombieCutoffMs int64) (int64, error) {
	var n int64
	err := db.QueryRow(`
		SELECT COUNT(*) FROM observations o WHERE `+spentObservationsWhere,
		graceCutoffMs, zombieCutoffMs).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count spent observations: %w", err)
	}
	return n, nil
}

// PruneSpentObservations deletes spent observations (see spentObservationsWhere)
// and returns how many rows were removed. Unlike the mem_nodes GC sweep this
// needs no snapshot and ships enabled: observations are raw tool-use scaffolding
// for a session that has ended, not memories. Deleting them destroys nothing a
// reader can ask for.
//
// Note this does not shrink the database file — freed pages are reused but not
// returned to the filesystem. Callers wanting the space back must VACUUM.
func (db *DB) PruneSpentObservations(graceCutoffMs, zombieCutoffMs int64) (int64, error) {
	result, err := db.Exec(`
		DELETE FROM observations WHERE id IN (
		    SELECT o.id FROM observations o WHERE `+spentObservationsWhere+`
		)`,
		graceCutoffMs, zombieCutoffMs)
	if err != nil {
		return 0, fmt.Errorf("prune spent observations: %w", err)
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

// Vacuum repacks the database file, returning freed pages to the filesystem.
// Retention alone will not shrink an already-bloated file, and a fragmented
// file is itself a performance problem: search scans all of mem_vectors on
// every query, so vectors interleaved among millions of dead observation pages
// turn a sequential read into scattered I/O.
func (db *DB) Vacuum() error {
	if _, err := db.Exec(`VACUUM`); err != nil {
		return fmt.Errorf("vacuum: %w", err)
	}
	return nil
}

// SizeOnDisk reports the database's footprint in bytes, including the WAL —
// which can itself be substantial between checkpoints. Returns 0 for in-memory
// or path-less databases.
func (db *DB) SizeOnDisk() int64 {
	if db.Path == "" {
		return 0
	}
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if fi, err := os.Stat(db.Path + suffix); err == nil {
			total += fi.Size()
		}
	}
	return total
}
