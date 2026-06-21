package store

import (
	"testing"
	"time"
)

// TestComputeMetrics exercises the freshness bands, needs-attention lists, and
// retraction accounting against a hand-built memory base with controlled ages.
func TestComputeMetrics(t *testing.T) {
	db := testDB(t)
	now := time.Now().UnixMilli()
	day := int64(24 * 60 * 60 * 1000)

	// backdate forces created_at + last_access to ageDays in the past so decay is
	// computed from a known reference time. setAccess overrides access_count.
	mk := func(uri, category string) {
		if err := db.CreateNode(&MemNode{URI: uri, NodeType: "leaf", Category: category, L0Abstract: "x"}); err != nil {
			t.Fatalf("create %s: %v", uri, err)
		}
	}
	backdate := func(uri string, ageDays int64) {
		ts := now - ageDays*day
		if _, err := db.Exec(`UPDATE mem_nodes SET created_at=?, last_access=? WHERE uri=?`, ts, ts, uri); err != nil {
			t.Fatalf("backdate %s: %v", uri, err)
		}
	}
	setAccess := func(uri string, n int) {
		if _, err := db.Exec(`UPDATE mem_nodes SET access_count=? WHERE uri=?`, n, uri); err != nil {
			t.Fatalf("setAccess %s: %v", uri, err)
		}
	}

	// Fresh, never retrieved (age 0 → not "old").
	mk("mem://user/patterns/fresh", "patterns")

	// Stale, old, never retrieved → stale band + never_retrieved_old.
	mk("mem://user/patterns/stale-orphan", "patterns")
	backdate("mem://user/patterns/stale-orphan", 200)

	// Load-bearing but decaying: retrieved a lot, ~0.40 effective → stale + stale_high_retrieval.
	mk("mem://user/cases/load-bearing", "cases")
	backdate("mem://user/cases/load-bearing", 120)
	setAccess("mem://user/cases/load-bearing", 42)

	// Near the decay cliff: 99 days → 0.5^1.1 ≈ 0.466, in [0.4,0.5). Retrieved once.
	mk("mem://user/cases/cliff", "cases")
	backdate("mem://user/cases/cliff", 99)
	setAccess("mem://user/cases/cliff", 3)

	// Moments are decay-exempt: old but stays fresh (stored relevance 1.0).
	mk("mem://user/moments/exempt", "moments")
	backdate("mem://user/moments/exempt", 200)

	// Retracted with a successor → counted, recent, NOT orphaned.
	mk("mem://user/events/superseded", "events")
	if _, err := db.Exec(`UPDATE mem_nodes SET tombstoned_at=?, superseded_by=? WHERE uri=?`,
		now-5*day, "mem://user/events/newer", "mem://user/events/superseded"); err != nil {
		t.Fatal(err)
	}

	// Retracted without a successor → orphaned tombstone.
	mk("mem://user/events/orphan-tomb", "events")
	if _, err := db.Exec(`UPDATE mem_nodes SET tombstoned_at=? WHERE uri=?`,
		now-2*day, "mem://user/events/orphan-tomb"); err != nil {
		t.Fatal(err)
	}

	m, err := db.ComputeMetrics()
	if err != nil {
		t.Fatalf("ComputeMetrics: %v", err)
	}

	// Active = fresh, stale-orphan, load-bearing, cliff, moments = 5.
	if m.Summary.ActiveTotal != 5 {
		t.Errorf("active_total = %d, want 5", m.Summary.ActiveTotal)
	}
	if m.Summary.RetractedTotal != 2 {
		t.Errorf("retracted_total = %d, want 2", m.Summary.RetractedTotal)
	}
	if m.Summary.RecentRetractions != 2 {
		t.Errorf("recent_retractions = %d, want 2", m.Summary.RecentRetractions)
	}

	// Fresh band: fresh node + exempt moments = 2.
	if m.Summary.Fresh != 2 {
		t.Errorf("fresh = %d, want 2 (fresh + exempt moments)", m.Summary.Fresh)
	}
	// Stale band: stale-orphan + load-bearing = 2.
	if m.Summary.Stale != 2 {
		t.Errorf("stale = %d, want 2", m.Summary.Stale)
	}
	// Cliff node is fading (0.466 between stale and fresh).
	if m.Summary.Fading != 1 {
		t.Errorf("fading = %d, want 1", m.Summary.Fading)
	}

	// Never retrieved: fresh, stale-orphan, cliff has access, load-bearing has access,
	// moments never retrieved. → fresh, stale-orphan, moments = 3.
	if m.Summary.NeverRetrieved != 3 {
		t.Errorf("never_retrieved = %d, want 3", m.Summary.NeverRetrieved)
	}

	// Retraction rate = 2 / 7.
	if got := m.Summary.RetractionRate; got < 0.28 || got > 0.29 {
		t.Errorf("retraction_rate = %f, want ~0.2857", got)
	}

	// Needs-attention lists.
	if !hasURI(m.NeedsAttention.NeverRetrievedOld, "mem://user/patterns/stale-orphan") {
		t.Error("stale-orphan should be in never_retrieved_old")
	}
	if hasURI(m.NeedsAttention.NeverRetrievedOld, "mem://user/patterns/fresh") {
		t.Error("fresh (age 0) should NOT be in never_retrieved_old")
	}
	if !hasURI(m.NeedsAttention.StaleHighRetrieval, "mem://user/cases/load-bearing") {
		t.Error("load-bearing should be in stale_high_retrieval")
	}
	if !hasURI(m.NeedsAttention.NearDecayCliff, "mem://user/cases/cliff") {
		t.Error("cliff should be in near_decay_cliff")
	}
	if !hasURI(m.NeedsAttention.OrphanedTombstones, "mem://user/events/orphan-tomb") {
		t.Error("orphan-tomb should be in orphaned_tombstones")
	}
	if hasURI(m.NeedsAttention.OrphanedTombstones, "mem://user/events/superseded") {
		t.Error("superseded (has successor) should NOT be orphaned")
	}

	// Critical: most-retrieved first.
	if len(m.Critical) == 0 || m.Critical[0].URI != "mem://user/cases/load-bearing" {
		t.Errorf("critical[0] = %+v, want load-bearing on top", m.Critical)
	}

	// Histogram bins cover every active memory exactly once.
	histSum := 0
	for _, b := range m.Histogram {
		histSum += b.Count
	}
	if histSum != m.Summary.ActiveTotal {
		t.Errorf("histogram sum = %d, want active_total %d", histSum, m.Summary.ActiveTotal)
	}

	// Categories present and shares sum to ~1.0.
	var sum float64
	for _, c := range m.Categories {
		sum += c.Share
	}
	if sum < 0.99 || sum > 1.01 {
		t.Errorf("category shares sum = %f, want ~1.0", sum)
	}
}

// TestComputeMetrics_ReadOnly verifies that computing metrics never mutates the
// store — relevance and access_count must be identical before and after.
func TestComputeMetrics_ReadOnly(t *testing.T) {
	db := testDB(t)
	if err := db.CreateNode(&MemNode{URI: "mem://user/patterns/p", NodeType: "leaf", Category: "patterns", L0Abstract: "x"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	day := int64(24 * 60 * 60 * 1000)
	if _, err := db.Exec(`UPDATE mem_nodes SET created_at=?, last_access=?, relevance=1.0, access_count=7 WHERE uri=?`,
		now-150*day, now-150*day, "mem://user/patterns/p"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ComputeMetrics(); err != nil {
		t.Fatal(err)
	}

	n, err := db.GetNodeByURI("mem://user/patterns/p")
	if err != nil {
		t.Fatal(err)
	}
	if n.Relevance != 1.0 {
		t.Errorf("relevance mutated to %f; ComputeMetrics must be read-only", n.Relevance)
	}
	if n.AccessCount != 7 {
		t.Errorf("access_count mutated to %d; ComputeMetrics must be read-only", n.AccessCount)
	}
}

func hasURI(xs []MetricNode, uri string) bool {
	for _, x := range xs {
		if x.URI == uri {
			return true
		}
	}
	return false
}
