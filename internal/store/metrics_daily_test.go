package store

import (
	"testing"
	"time"
)

// Retrievals are journaled `deepened` events per UTC day, read live like
// captures: no snapshot is needed, and shown events and the frozen
// access_count column never contribute (#77).
func TestDailySeries_RetrievalsFromJournal(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()
	if err := db.CreateNode(&MemNode{URI: "mem://user/patterns/a", NodeType: "leaf", Category: "patterns", L0Abstract: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE mem_nodes SET access_count=99 WHERE uri=?`, "mem://user/patterns/a"); err != nil {
		t.Fatal(err)
	}

	threeDaysAgo := now.AddDate(0, 0, -3)
	events := []MemEvent{
		{NodeURI: "mem://user/patterns/a", Event: "deepened", CreatedAt: now.UnixMilli()},
		{NodeURI: "mem://user/patterns/a", Event: "deepened", CreatedAt: now.UnixMilli()},
		{NodeURI: "mem://user/patterns/a", Event: "deepened", CreatedAt: threeDaysAgo.UnixMilli()},
		{NodeURI: "mem://user/patterns/a", Event: "shown", Surface: "tray", CreatedAt: now.UnixMilli()},
		// Outside a 7-day window.
		{NodeURI: "mem://user/patterns/a", Event: "deepened", CreatedAt: now.AddDate(0, 0, -20).UnixMilli()},
	}
	for _, e := range events {
		if err := db.InsertEvent(e); err != nil {
			t.Fatal(err)
		}
	}

	series, err := db.BuildDailySeries(7)
	if err != nil {
		t.Fatalf("BuildDailySeries: %v", err)
	}
	byDate := map[string]DailyPoint{}
	total := 0
	for _, p := range series {
		byDate[p.Date] = p
		total += p.Retrievals
	}
	if got := byDate[now.Format("2006-01-02")].Retrievals; got != 2 {
		t.Errorf("today retrievals = %d, want 2", got)
	}
	if got := byDate[threeDaysAgo.Format("2006-01-02")].Retrievals; got != 1 {
		t.Errorf("3-days-ago retrievals = %d, want 1", got)
	}
	if total != 3 {
		t.Errorf("window retrievals = %d, want 3 (shown and out-of-window excluded)", total)
	}
}

func TestRollupAndDailySeries(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()

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

	if err := db.RollupDailySnapshot(); err != nil {
		t.Fatalf("RollupDailySnapshot: %v", err)
	}

	// Today's snapshot must exist with the live active total and bucket counts.
	var active, neverRetrieved int
	today := now.Format("2006-01-02")
	if err := db.QueryRow(`SELECT active_total, never_retrieved FROM metrics_daily WHERE date=?`, today).
		Scan(&active, &neverRetrieved); err != nil {
		t.Fatalf("read today snapshot: %v", err)
	}
	if active != 3 {
		t.Errorf("snapshot active_total = %d, want 3", active)
	}
	if neverRetrieved != 3 {
		t.Errorf("snapshot never_retrieved = %d, want 3", neverRetrieved)
	}

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
	if !todayPt.HasSnapshot || todayPt.ActiveTotal != 3 {
		t.Errorf("today point = %+v, want snapshot with active_total 3", *todayPt)
	}
}
