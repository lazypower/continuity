package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

const metricsDailyWindow = 30 // days of trend history surfaced to the dashboard

// DailyPoint is one day on the Memory Health trend. Captures are derived live
// from created_at (so full history is available immediately); retrievals and the
// freshness buckets come from daily snapshots, so they accrue going forward and
// are only meaningful where HasSnapshot is true.
type DailyPoint struct {
	Date        string `json:"date"` // 'YYYY-MM-DD' (UTC)
	Captures    int    `json:"captures"`
	Retrievals  int    `json:"retrievals"`
	ActiveTotal int    `json:"active_total"`
	Fresh       int    `json:"fresh"`
	Fading      int    `json:"fading"`
	Stale       int    `json:"stale"`
	HasSnapshot bool   `json:"has_snapshot"`
}

func utcDate(t time.Time) string { return t.UTC().Format("2006-01-02") }

// RollupDailySnapshot records today's health buckets + cumulative access total.
// Idempotent: re-running on the same day overwrites that day's row, so an hourly
// tick keeps "today" current. This writes a snapshot row but never touches any
// memory — the no-mutate-on-view contract is about the measured nodes, not the
// trend ledger, and this runs on a timer rather than on a dashboard read.
func (db *DB) RollupDailySnapshot() error {
	m, err := db.ComputeMetrics()
	if err != nil {
		return err
	}

	var totalAccess int64
	if err := db.QueryRow(
		`SELECT COALESCE(SUM(access_count), 0) FROM mem_nodes WHERE node_type = 'leaf'`,
	).Scan(&totalAccess); err != nil {
		return fmt.Errorf("sum access: %w", err)
	}

	now := time.Now().UTC()
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
	var captures int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM mem_nodes WHERE node_type = 'leaf' AND created_at >= ?`, dayStart,
	).Scan(&captures); err != nil {
		return fmt.Errorf("count captures: %w", err)
	}

	catCounts := map[string]int{}
	for _, c := range m.Categories {
		catCounts[c.Category] = c.Count
	}
	catJSON, _ := json.Marshal(catCounts)

	_, err = db.Exec(`
		INSERT INTO metrics_daily
			(date, active_total, retracted_total, fresh, fading, stale, never_retrieved,
			 total_access, captures, category_counts, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(date) DO UPDATE SET
			active_total=excluded.active_total, retracted_total=excluded.retracted_total,
			fresh=excluded.fresh, fading=excluded.fading, stale=excluded.stale,
			never_retrieved=excluded.never_retrieved, total_access=excluded.total_access,
			captures=excluded.captures, category_counts=excluded.category_counts,
			updated_at=excluded.updated_at
	`, utcDate(now), m.Summary.ActiveTotal, m.Summary.RetractedTotal,
		m.Summary.Fresh, m.Summary.Fading, m.Summary.Stale, m.Summary.NeverRetrieved,
		totalAccess, captures, string(catJSON), now.UnixMilli())
	if err != nil {
		return fmt.Errorf("upsert metrics_daily: %w", err)
	}
	return nil
}

// BuildDailySeries returns the last `days` days of trend points. Captures come
// from created_at (full history); retrievals are day-over-day diffs of the
// snapshotted cumulative access total; freshness buckets come from snapshots.
func (db *DB) BuildDailySeries(days int) ([]DailyPoint, error) {
	if days < 1 {
		days = 1
	}
	now := time.Now().UTC()
	startDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -(days - 1))
	startMs := startDay.UnixMilli()

	// Captures per day, derived from created_at (covers all history in-window).
	capByDate := map[string]int{}
	rows, err := db.Query(`
		SELECT date(created_at/1000, 'unixepoch') AS d, COUNT(*)
		FROM mem_nodes
		WHERE node_type = 'leaf' AND created_at >= ?
		GROUP BY d
	`, startMs)
	if err != nil {
		return nil, fmt.Errorf("captures by date: %w", err)
	}
	for rows.Next() {
		var d string
		var c int
		if err := rows.Scan(&d, &c); err != nil {
			rows.Close()
			return nil, err
		}
		capByDate[d] = c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// All snapshots up to today, ordered, so retrievals = diff of total_access.
	type snap struct {
		active, fresh, fading, stale int
		totalAccess                  int64
	}
	snapByDate := map[string]snap{}
	retrievalsByDate := map[string]int{}
	startDate := utcDate(startDay)

	// Seed the diff baseline from the most recent snapshot BEFORE the window, so
	// the snapshot read stays O(window) instead of O(all history) as the ledger
	// grows. retrievals-per-day is a day-over-day diff of cumulative access; the
	// first in-window day diffs against this baseline.
	var prevAccess int64
	var havePrev bool
	switch err := db.QueryRow(
		`SELECT total_access FROM metrics_daily WHERE date < ? ORDER BY date DESC LIMIT 1`, startDate,
	).Scan(&prevAccess); err {
	case nil:
		havePrev = true
	case sql.ErrNoRows:
		// No prior snapshot; the first in-window day simply has no retrievals figure.
	default:
		return nil, fmt.Errorf("seed retrievals baseline: %w", err)
	}

	srows, err := db.Query(`
		SELECT date, active_total, fresh, fading, stale, total_access
		FROM metrics_daily WHERE date >= ? ORDER BY date ASC
	`, startDate)
	if err != nil {
		return nil, fmt.Errorf("read snapshots: %w", err)
	}
	for srows.Next() {
		var d string
		var s snap
		if err := srows.Scan(&d, &s.active, &s.fresh, &s.fading, &s.stale, &s.totalAccess); err != nil {
			srows.Close()
			return nil, err
		}
		snapByDate[d] = s
		if havePrev {
			diff := int(s.totalAccess - prevAccess)
			if diff < 0 {
				diff = 0
			}
			retrievalsByDate[d] = diff
		}
		prevAccess = s.totalAccess
		havePrev = true
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return nil, err
	}

	out := make([]DailyPoint, 0, days)
	for i := 0; i < days; i++ {
		d := utcDate(startDay.AddDate(0, 0, i))
		p := DailyPoint{Date: d, Captures: capByDate[d]}
		if s, ok := snapByDate[d]; ok {
			p.HasSnapshot = true
			p.ActiveTotal = s.active
			p.Fresh = s.fresh
			p.Fading = s.fading
			p.Stale = s.stale
			p.Retrievals = retrievalsByDate[d]
		}
		out = append(out, p)
	}
	return out, nil
}
