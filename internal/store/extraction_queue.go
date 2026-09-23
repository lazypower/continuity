package store

import (
	"database/sql"
	"fmt"
	"strings"
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
//
// kinds, when given, restricts selection to those job kinds; the worker uses it
// to keep draining work that is safe while other kinds are deferred.
func (db *DB) NextExtraction(maxAttempts int, kinds ...string) (*ExtractionJob, error) {
	query := `SELECT id, session_id, kind, payload, force, attempts
		 FROM extraction_queue WHERE attempts < ?`
	args := []any{maxAttempts}
	if len(kinds) > 0 {
		query += ` AND kind IN (?` + strings.Repeat(`, ?`, len(kinds)-1) + `)`
		for _, k := range kinds {
			args = append(args, k)
		}
	}
	query += ` ORDER BY attempts ASC, id ASC LIMIT 1`

	var j ExtractionJob
	var force int
	err := db.QueryRow(query, args...).Scan(&j.ID, &j.SessionID, &j.Kind, &j.Payload, &force, &j.Attempts)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("next extraction: %w", err)
	}
	j.Force = force != 0
	return &j, nil
}

// ParkExtraction parks a job immediately by raising its attempts to
// maxAttempts, for a failure that retrying cannot fix. The row is kept, as with
// any parked job: NextExtraction skips it and health reports it as parked.
func (db *DB) ParkExtraction(id int64, maxAttempts int) error {
	if _, err := db.Exec(`UPDATE extraction_queue SET attempts = MAX(attempts, ?) WHERE id = ?`, maxAttempts, id); err != nil {
		return fmt.Errorf("park extraction %d: %w", id, err)
	}
	return nil
}

// ExtractionQueueDepth splits the queue into runnable jobs (attempts below
// maxAttempts) and parked ones, from one read so the pair is consistent. Health
// reports them apart so jobs nothing will retry do not read as a backlog.
func (db *DB) ExtractionQueueDepth(maxAttempts int) (pending, parked int, err error) {
	if err := db.QueryRow(`
		SELECT COALESCE(SUM(attempts < ?), 0), COALESCE(SUM(attempts >= ?), 0)
		FROM extraction_queue
	`, maxAttempts, maxAttempts).Scan(&pending, &parked); err != nil {
		return 0, 0, fmt.Errorf("extraction queue depth: %w", err)
	}
	return pending, parked, nil
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
