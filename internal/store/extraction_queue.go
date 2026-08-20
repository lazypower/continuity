package store

import (
	"database/sql"
	"fmt"
	"time"
)

// ExtractionJob is one durable pending extraction claimed from extraction_queue.
type ExtractionJob struct {
	ID        int64
	SessionID string
	Kind      string // "session" | "signal" | "relational"
	Payload   string // transcript path (session, relational) or prompt (signal)
	Force     bool
	Attempts  int
}

// EnqueueExtraction records a pending extraction so the work survives a crash or
// restart: the row is deleted only after the extraction succeeds (H1). kind is
// "session" or "relational" (payload = transcript path) or "signal"
// (payload = prompt).
func (db *DB) EnqueueExtraction(sessionID, kind, payload string, force bool) error {
	f := 0
	if force {
		f = 1
	}
	_, err := db.Exec(
		`INSERT INTO extraction_queue (session_id, kind, payload, force, attempts, queued_at)
		 VALUES (?, ?, ?, ?, 0, ?)`,
		sessionID, kind, payload, f, time.Now().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("enqueue extraction: %w", err)
	}
	return nil
}

// NextExtraction returns the next still-eligible pending extraction, or nil if
// none remain. Jobs that have exhausted maxAttempts are excluded — they are
// PARKED, not deleted, so a job that never succeeds stays in the queue for
// inspection/replay instead of silently losing the capture. Ordering prefers
// fewer-failed then older jobs so a repeatedly-failing job cannot head-of-line-
// block fresh work.
func (db *DB) NextExtraction(maxAttempts int) (*ExtractionJob, error) {
	var j ExtractionJob
	var force int
	err := db.QueryRow(
		`SELECT id, session_id, kind, payload, force, attempts
		 FROM extraction_queue WHERE attempts < ?
		 ORDER BY attempts ASC, id ASC LIMIT 1`,
		maxAttempts,
	).Scan(&j.ID, &j.SessionID, &j.Kind, &j.Payload, &force, &j.Attempts)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("next extraction: %w", err)
	}
	j.Force = force != 0
	return &j, nil
}

// DeleteExtraction removes a job from the queue — called after it succeeds, or
// after it is abandoned as poison (see BumpExtractionAttempts).
func (db *DB) DeleteExtraction(id int64) error {
	if _, err := db.Exec(`DELETE FROM extraction_queue WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete extraction %d: %w", id, err)
	}
	return nil
}

// BumpExtractionAttempts increments the retry count for a failed job and returns
// the new count so the caller can decide when to abandon a job that never
// succeeds. Returns 0 if the row no longer exists.
func (db *DB) BumpExtractionAttempts(id int64) (int, error) {
	if _, err := db.Exec(`UPDATE extraction_queue SET attempts = attempts + 1 WHERE id = ?`, id); err != nil {
		return 0, fmt.Errorf("bump extraction attempts %d: %w", id, err)
	}
	var attempts int
	err := db.QueryRow(`SELECT attempts FROM extraction_queue WHERE id = ?`, id).Scan(&attempts)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read attempts %d: %w", id, err)
	}
	return attempts, nil
}

// PendingExtractions returns the number of jobs waiting in the queue. Used by
// doctor/health to surface a backlog rather than let it hide.
func (db *DB) PendingExtractions() (int, error) {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM extraction_queue`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending extractions: %w", err)
	}
	return n, nil
}
