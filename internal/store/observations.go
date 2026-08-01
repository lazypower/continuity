package store

import (
	"fmt"
	"os"
	"time"
)

// Observation represents a single tool use recorded during a session.
//
// ToolInput and ToolResponse are retained on the struct and in the schema, but
// are no longer populated — see AddObservation.
type Observation struct {
	ID           int64
	SessionID    string
	ToolName     string
	ToolInput    string
	ToolResponse string
	CreatedAt    int64
}

// AddObservation records that a tool ran. It deliberately does NOT persist the
// tool's input or its response.
//
// Those two columns used to hold the full call and its result, truncated at
// 10KB each — so a Read's response was file contents and a Bash call's was
// command output, written to disk on every PostToolUse hook at roughly 230MB
// per active user per month (issue #72).
//
// Nothing ever read them. Memory extraction reads the session transcript, not
// this table. The only production reader of `observations` is a COUNT(*) behind
// the "N tool uses recorded this session" line in the injected context, and
// even that number is already maintained independently on sessions.tool_count.
// GetObservations and GetRecentObservations, the two functions that return the
// content, have no callers outside tests.
//
// The payload existed to serve RFC.md's promise of browsable per-session
// history at mem://sessions/<id>/observations. That promise was withdrawn (see
// the revision note in RFC.md §4.3); the write path outlived it. The name is
// inherited from claude-mem, where an "observation" was a DISTILLED MEMORY that
// the viewer paginated — the justification for persisting it came from that
// meaning and survived onto this one, which is raw capture.
//
// Writing data nothing reads is cost without benefit, and the cost is paid in
// disk and in whatever the user's own tool traffic happened to contain. The
// columns stay so the schema is unchanged and old rows still read back; new
// rows carry empty strings. Removing the table, and the retention machinery
// that exists only to bound it, is a deliberate follow-up.
func (db *DB) AddObservation(sessionID, toolName string) error {
	now := time.Now().UnixMilli()
	_, err := db.Exec(`
		INSERT INTO observations (session_id, tool_name, tool_input, tool_response, created_at)
		VALUES (?, ?, '', '', ?)
	`, sessionID, toolName, now)
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
// "In flight" is the mechanism: a session row still marked 'active' whose most
// recent activity is inside the zombie horizon. A session left 'active' by a
// crashed or killed client never completes, so past that horizon we stop
// treating it as live. An observation with no session row at all is orphaned
// and therefore not in flight.
//
// Liveness comes from sessions.last_active_at, which is stamped on session
// init, on resume, and on every recorded tool use. Neither older column answers
// the question: started_at is not refreshed when InitSession reactivates a
// resumed session, and status alone cannot tell "in use" from "abandoned by a
// client that crashed before firing Stop". Deriving it from
// MAX(observations.created_at) instead was correct only after the resumed
// session's first new observation landed — a sweep inside that window would
// delete the history of a session the user had just reopened.
//
// Evaluated once over the (small, indexed) set of active sessions rather than
// correlated per observation row, which keeps the count cheap.
//
// Bound parameter: zombie cutoff.
const liveSessionsSelect = `
	SELECT s.session_id FROM sessions s
	WHERE s.status = 'active'
	  AND COALESCE(s.last_active_at, s.started_at) >= ?`

// spentObservationsWhere binds, in order: zombie cutoff, grace cutoff.
const spentObservationsWhere = `
	session_id NOT IN (` + liveSessionsSelect + `)
	AND created_at < ?`

// CountSpentObservations reports how many rows the sweep would reclaim right
// now (same predicate, no delete), so a growing pile is visible in /api/health
// rather than only after it has already cost someone a debugging session.
func (db *DB) CountSpentObservations(graceCutoffMs, zombieCutoffMs int64) (int64, error) {
	var n int64
	err := db.QueryRow(`
		SELECT COUNT(*) FROM observations WHERE `+spentObservationsWhere,
		zombieCutoffMs, graceCutoffMs).Scan(&n)
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
		DELETE FROM observations WHERE `+spentObservationsWhere,
		zombieCutoffMs, graceCutoffMs)
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
