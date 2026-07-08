package store

import (
	"testing"
	"time"
)

// TestGCCandidatesExcludesProtected pins the safety-critical predicate: GC targets
// ONLY genuine dead weight (decayable category, floored relevance, long-unretrieved,
// live, unpinned) and never touches recently-used, still-relevant, retracted
// (receipt), pinned, or decay-exempt (contract/moments) memories.
func TestGCCandidatesExcludesProtected(t *testing.T) {
	db := testDB(t)
	oldMs := time.Now().Add(-365 * 24 * time.Hour).UnixMilli()
	recentMs := time.Now().UnixMilli()

	mk := func(uri, category string, relevance float64, lastAccess int64, retract, pin bool) {
		if err := db.CreateNode(&MemNode{
			URI: uri, NodeType: "leaf", Category: category,
			L0Abstract: "abstract", L1Overview: "body long enough to be a real memory",
		}); err != nil {
			t.Fatalf("create %s: %v", uri, err)
		}
		got, _ := db.GetNodeByURI(uri)
		if _, err := db.Exec(`UPDATE mem_nodes SET relevance = ?, last_access = ? WHERE id = ?`,
			relevance, lastAccess, got.ID); err != nil {
			t.Fatal(err)
		}
		if retract {
			if _, err := db.RetractNode(uri, "retracted", ""); err != nil {
				t.Fatal(err)
			}
		}
		if pin {
			if _, err := db.Exec(`UPDATE mem_nodes SET pinned_at = ? WHERE id = ?`, recentMs, got.ID); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The one true candidate: decayable, floored, long-unretrieved, live, unpinned.
	mk("mem://agent/patterns/dead", "patterns", 0.1, oldMs, false, false)
	// Everything below must be EXCLUDED:
	mk("mem://agent/patterns/fresh", "patterns", 0.1, recentMs, false, false)      // used recently
	mk("mem://agent/patterns/relevant", "patterns", 0.8, oldMs, false, false)      // not floored
	mk("mem://agent/cases/retracted", "cases", 0.1, oldMs, true, false)            // retracted receipt — never GC
	mk("mem://agent/patterns/pinned", "patterns", 0.1, oldMs, false, true)         // pinned
	mk("mem://user/preferences/contract", "preferences", 0.1, oldMs, false, false) // decay-exempt category

	floor := 0.1
	cutoff := time.Now().Add(-180 * 24 * time.Hour).UnixMilli()

	cands, err := db.GCCandidates(floor, cutoff, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 1 || cands[0].URI != "mem://agent/patterns/dead" {
		var uris []string
		for _, c := range cands {
			uris = append(uris, c.URI)
		}
		t.Fatalf("GC must target only the dead-weight node, got %v", uris)
	}

	if n, err := db.CountGCCandidates(floor, cutoff); err != nil || n != 1 {
		t.Errorf("CountGCCandidates = %d (err %v), want 1", n, err)
	}
}
