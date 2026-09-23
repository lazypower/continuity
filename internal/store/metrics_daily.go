package store

import (
	"encoding/json"
	"fmt"
	"time"
)

const metricsDailyWindow = 30 // days of trend history surfaced to the dashboard

// DailyPoint is one day on the Memory Health trend. Captures and retrievals are
// derived live from timestamps (captures from mem_nodes.created_at, retrievals
// from `deepened` events in the mem_events journal), so both cover every day
// the source retains. The freshness buckets come from daily snapshots, so they
// accrue going forward and are only meaningful where HasSnapshot is true.
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

// RollupDailySnapshot records today's health buckets. Idempotent: re-running on
// the same day overwrites that day's row, so an hourly tick keeps "today"
// current. This writes a snapshot row but never touches any memory — the
// no-mutate-on-view contract is about the measured nodes, not the trend ledger,
// and this runs on a timer rather than on a dashboard read.
//
// metrics_daily.total_access is no longer written: it snapshotted
// SUM(mem_nodes.access_count), a column that froze when the mem_events journal
// became the authority on use (#77). Retrievals are counted from the journal.
func (db *DB) RollupDailySnapshot() error {
	m, err := db.ComputeMetrics()
	if err != nil {
		return err
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
			 captures, category_counts, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(date) DO UPDATE SET
			active_total=excluded.active_total, retracted_total=excluded.retracted_total,
			fresh=excluded.fresh, fading=excluded.fading, stale=excluded.stale,
			never_retrieved=excluded.never_retrieved,
			captures=excluded.captures, category_counts=excluded.category_counts,
			updated_at=excluded.updated_at
	`, utcDate(now), m.Summary.ActiveTotal, m.Summary.RetractedTotal,
		m.Summary.Fresh, m.Summary.Fading, m.Summary.Stale, m.Summary.NeverRetrieved,
		captures, string(catJSON), now.UnixMilli())
	if err != nil {
		return fmt.Errorf("upsert metrics_daily: %w", err)
	}
	return nil
}

// countByDay runs a `SELECT <utc date>, COUNT(*) ... GROUP BY` query and
// returns the counts keyed by 'YYYY-MM-DD'.
func (db *DB) countByDay(query string, args ...any) (map[string]int, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var d string
		var c int
		if err := rows.Scan(&d, &c); err != nil {
			return nil, err
		}
		out[d] = c
	}
	return out, rows.Err()
}

// BuildDailySeries returns the last `days` days of trend points. Captures come
// from created_at, retrievals from journaled `deepened` events, and the
// freshness buckets from snapshots.
func (db *DB) BuildDailySeries(days int) ([]DailyPoint, error) {
	if days < 1 {
		days = 1
	}
	now := time.Now().UTC()
	startDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -(days - 1))
	startMs := startDay.UnixMilli()

	capByDate, err := db.countByDay(`
		SELECT date(created_at/1000, 'unixepoch') AS d, COUNT(*)
		FROM mem_nodes
		WHERE node_type = 'leaf' AND created_at >= ?
		GROUP BY d
	`, startMs)
	if err != nil {
		return nil, fmt.Errorf("captures by date: %w", err)
	}

	// Served by idx_events_created.
	retrievalsByDate, err := db.countByDay(`
		SELECT date(created_at/1000, 'unixepoch') AS d, COUNT(*)
		FROM mem_events
		WHERE event = 'deepened' AND created_at >= ?
		GROUP BY d
	`, startMs)
	if err != nil {
		return nil, fmt.Errorf("retrievals by date: %w", err)
	}

	type snap struct {
		active, fresh, fading, stale int
	}
	snapByDate := map[string]snap{}
	srows, err := db.Query(`
		SELECT date, active_total, fresh, fading, stale
		FROM metrics_daily WHERE date >= ? ORDER BY date ASC
	`, utcDate(startDay))
	if err != nil {
		return nil, fmt.Errorf("read snapshots: %w", err)
	}
	for srows.Next() {
		var d string
		var s snap
		if err := srows.Scan(&d, &s.active, &s.fresh, &s.fading, &s.stale); err != nil {
			srows.Close()
			return nil, err
		}
		snapByDate[d] = s
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return nil, err
	}

	out := make([]DailyPoint, 0, days)
	for i := 0; i < days; i++ {
		d := utcDate(startDay.AddDate(0, 0, i))
		p := DailyPoint{Date: d, Captures: capByDate[d], Retrievals: retrievalsByDate[d]}
		if s, ok := snapByDate[d]; ok {
			p.HasSnapshot = true
			p.ActiveTotal = s.active
			p.Fresh = s.fresh
			p.Fading = s.fading
			p.Stale = s.stale
		}
		out = append(out, p)
	}
	return out, nil
}
