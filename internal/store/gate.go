package store

import (
	"fmt"
	"time"
)

// Gate calibration retention bounds (#80, #72 rule: telemetry must never
// outgrow the corpus it measures). Both are enforced inside InsertGateCalibration
// so the table is bounded by design at the only write site — there is no
// background sweeper whose absence could let it grow.
//
// Sizing: a row is a few hundred bytes (five uri+sim pointers), so the cap is
// ~2–3 MB worst case; calibration needs ≥200 prompts (ADR-001 §MVP), so 10k
// rows is two orders of magnitude of headroom, and 90 days matches the decay
// half-life the rest of the substrate reasons in.
const (
	GateCalibrationMaxRows = 10000
	GateCalibrationMaxAge  = 90 * 24 * time.Hour
)

// GateCalibration is one shadow-log row for the prompt gate (ADR-001 §4, #80):
// what the gate WOULD have surfaced for one prompt. It deliberately carries no
// prompt text and no node payloads — only similarity geometry and pointers.
type GateCalibration struct {
	ID          int64
	SessionID   string
	Project     string
	PromptChars int
	MaxSim      float64
	TopHits     string // JSON [{"uri":...,"sim":...}], top-k pointers only
	CreatedAt   int64
}

// InsertGateCalibration appends one calibration row and enforces the retention
// bounds in the same call. Callers on the prompt path must not call this
// inline — the server routes it through the buffered fire-and-forget recorder,
// same contract as mem_events (telemetry may lose a row; the prompt never
// waits for one).
func (db *DB) InsertGateCalibration(e GateCalibration) error {
	return db.insertGateCalibrationBounded(e, GateCalibrationMaxRows, GateCalibrationMaxAge)
}

// insertGateCalibrationBounded is InsertGateCalibration with explicit bounds,
// so tests can pin the retention rule without paying for 10k inserts.
func (db *DB) insertGateCalibrationBounded(e GateCalibration, maxRows int, maxAge time.Duration) error {
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().UnixMilli()
	}
	if e.TopHits == "" {
		e.TopHits = "[]"
	}
	if _, err := db.Exec(`
		INSERT INTO gate_calibration (session_id, project, prompt_chars, max_sim, top_hits, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, e.SessionID, e.Project, e.PromptChars, e.MaxSim, e.TopHits, e.CreatedAt); err != nil {
		return fmt.Errorf("insert gate calibration: %w", err)
	}

	// Bounded at write time (#72): age out, then trim to the row cap. This
	// runs on the buffered recorder's goroutine, never on the prompt path, and
	// a ≤maxRows table keeps both deletes cheap. Enforcing here means the
	// bound holds after every single write rather than eventually.
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	if _, err := db.Exec(`DELETE FROM gate_calibration WHERE created_at < ?`, cutoff); err != nil {
		return fmt.Errorf("age out gate calibration: %w", err)
	}
	if _, err := db.Exec(`
		DELETE FROM gate_calibration WHERE id NOT IN (
			SELECT id FROM gate_calibration ORDER BY id DESC LIMIT ?
		)
	`, maxRows); err != nil {
		return fmt.Errorf("trim gate calibration: %w", err)
	}
	return nil
}

// GateCalibrationMaxSims returns every logged max-similarity, ascending. The
// calibration report derives count/median/percentiles/fire-rates from this;
// the table is write-bounded to GateCalibrationMaxRows so the full read stays
// small by construction.
func (db *DB) GateCalibrationMaxSims() ([]float64, error) {
	rows, err := db.Query(`SELECT max_sim FROM gate_calibration ORDER BY max_sim ASC`)
	if err != nil {
		return nil, fmt.Errorf("gate calibration sims: %w", err)
	}
	defer rows.Close()

	var sims []float64
	for rows.Next() {
		var s float64
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("scan gate calibration sim: %w", err)
		}
		sims = append(sims, s)
	}
	return sims, rows.Err()
}

// CountGateCalibration returns the number of retained calibration rows.
// Inspection/test helper.
func (db *DB) CountGateCalibration() (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM gate_calibration`).Scan(&n)
	return n, err
}
