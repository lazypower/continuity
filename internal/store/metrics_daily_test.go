package store

import (
	"testing"
	"time"
)

// TestDailySeries_BaselineFromBeforeWindow proves the bounded snapshot read
// still computes correct retrieval diffs: a snapshot OUTSIDE the window must
// still seed the first in-window day's day-over-day diff.
func TestDailySeries_BaselineFromBeforeWindow(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()

	older := now.AddDate(0, 0, -5).Format("2006-01-02") // outside a 2-day window
	today := now.Format("2006-01-02")
	if _, err := db.Exec(`INSERT INTO metrics_daily (date, total_access, updated_at) VALUES (?, ?, ?)`,
		older, 10, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO metrics_daily (date, total_access, updated_at) VALUES (?, ?, ?)`,
		today, 18, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}

	series, err := db.BuildDailySeries(2) // window = [yesterday, today]; older row excluded
	if err != nil {
		t.Fatalf("BuildDailySeries: %v", err)
	}
	var todayPt *DailyPoint
	for i := range series {
		if series[i].Date == today {
			todayPt = &series[i]
		}
	}
	if todayPt == nil {
		t.Fatal("today not in 2-day window")
	}
	// 18 (today) − 10 (pre-window baseline) = 8, even though the older row is
	// outside the window and never scanned by the main query.
	if todayPt.Retrievals != 8 {
		t.Errorf("today retrievals = %d, want 8 (18−10 via pre-window baseline)", todayPt.Retrievals)
	}
}

func TestRollupAndDailySeries(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()
	day := int64(24 * 60 * 60 * 1000)

	mk := func(uri string) {
		if err := db.CreateNode(&MemNode{URI: uri, NodeType: "leaf", Category: "patterns", L0Abstract: "x"}); err != nil {
			t.Fatalf("create %s: %v", uri, err)
		}
	}
	mk("mem://user/patterns/a")
	mk("mem://user/patterns/b")
	mk("mem://user/patterns/c")
	// Backdate one capture 5 days into the window.
	created5d := now.AddDate(0, 0, -5)
	if _, err := db.Exec(`UPDATE mem_nodes SET created_at=? WHERE uri=?`,
		created5d.UnixMilli(), "mem://user/patterns/c"); err != nil {
		t.Fatal(err)
	}

	// Captures are derived from created_at — full history, no snapshot needed.
	series, err := db.BuildDailySeries(30)
	if err != nil {
		t.Fatalf("BuildDailySeries: %v", err)
	}
	if len(series) != 30 {
		t.Fatalf("series len = %d, want 30", len(series))
	}
	totalCap := 0
	for _, p := range series {
		totalCap += p.Captures
	}
	if totalCap != 3 {
		t.Errorf("total captures = %d, want 3", totalCap)
	}
	// The backdated capture lands on its own UTC date.
	wantDate := created5d.Format("2006-01-02")
	var found bool
	for _, p := range series {
		if p.Date == wantDate && p.Captures == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("backdated capture not found on %s", wantDate)
	}

	// Seed a prior-day snapshot, give a node access, roll up today → retrievals diff.
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	if _, err := db.Exec(`INSERT INTO metrics_daily (date, total_access, updated_at) VALUES (?, ?, ?)`,
		yesterday, 5, now.UnixMilli()-day); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE mem_nodes SET access_count=8 WHERE uri=?`, "mem://user/patterns/a"); err != nil {
		t.Fatal(err)
	}

	if err := db.RollupDailySnapshot(); err != nil {
		t.Fatalf("RollupDailySnapshot: %v", err)
	}

	// Today's snapshot must exist with the live active total + cumulative access.
	var active int
	var totalAccess int64
	today := now.Format("2006-01-02")
	if err := db.QueryRow(`SELECT active_total, total_access FROM metrics_daily WHERE date=?`, today).
		Scan(&active, &totalAccess); err != nil {
		t.Fatalf("read today snapshot: %v", err)
	}
	if active != 3 {
		t.Errorf("snapshot active_total = %d, want 3", active)
	}
	if totalAccess != 8 {
		t.Errorf("snapshot total_access = %d, want 8", totalAccess)
	}

	// Retrievals for today = today's total_access (8) − yesterday's (5) = 3.
	series2, err := db.BuildDailySeries(30)
	if err != nil {
		t.Fatal(err)
	}
	var todayPt *DailyPoint
	for i := range series2 {
		if series2[i].Date == today {
			todayPt = &series2[i]
		}
	}
	if todayPt == nil {
		t.Fatal("today not in series")
	}
	if !todayPt.HasSnapshot {
		t.Error("today should have a snapshot")
	}
	if todayPt.Retrievals != 3 {
		t.Errorf("today retrievals = %d, want 3 (8−5)", todayPt.Retrievals)
	}
}
