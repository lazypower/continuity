package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lazypower/continuity/internal/store"
)

// seedDeadWeight creates a floored, long-unretrieved, decayable leaf — a GC candidate.
func seedDeadWeight(t *testing.T, db *store.DB, uri string) {
	t.Helper()
	if err := db.CreateNode(&store.MemNode{
		URI: uri, NodeType: "leaf", Category: "patterns",
		L0Abstract: "cold pattern", L1Overview: "a body that is comfortably long enough",
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := db.GetNodeByURI(uri)
	oldMs := time.Now().Add(-365 * 24 * time.Hour).UnixMilli()
	if _, err := db.Exec(`UPDATE mem_nodes SET relevance = 0.1, last_access = ? WHERE id = ?`, oldMs, got.ID); err != nil {
		t.Fatal(err)
	}
}

// TestGCSweepModes pins the three-state toggle: off and shadow never delete;
// on reclaims genuine dead weight. Uses a file-backed DB so SnapshotNow produces
// a real restore point (GCOn refuses to reclaim without one).
func TestGCSweepModes(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "gc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	eng := New(db, nil)

	dead := "mem://agent/patterns/dead"
	seedDeadWeight(t, db, dead)
	alive := func() bool { n, _ := db.GetNodeByURI(dead); return n != nil }

	eng.SetGCMode(GCOff)
	eng.runGCSweep()
	if !alive() {
		t.Fatal("GCOff must not delete")
	}

	eng.SetGCMode(GCShadow)
	eng.runGCSweep()
	if !alive() {
		t.Fatal("GCShadow must not delete — it only logs what it would reclaim")
	}

	eng.SetGCMode(GCOn)
	eng.runGCSweep()
	if alive() {
		t.Error("GCOn must reclaim the dead-weight node")
	}
}

// TestGCOnRefusesWithoutRestorePoint pins the fail-closed contract: with no
// snapshot possible (:memory: returns an empty path), GCOn must NOT delete.
func TestGCOnRefusesWithoutRestorePoint(t *testing.T) {
	db := testDB(t) // :memory: — SnapshotNow returns ("", nil), no restore point
	eng := New(db, nil)

	dead := "mem://agent/patterns/dead"
	seedDeadWeight(t, db, dead)

	eng.SetGCMode(GCOn)
	eng.runGCSweep()

	if n, _ := db.GetNodeByURI(dead); n == nil {
		t.Error("GCOn must REFUSE to delete when no restore point can be taken")
	}
}

func TestGCReclaimableCountEmpty(t *testing.T) {
	eng := New(testDB(t), nil)
	if n, err := eng.GCReclaimableCount(); err != nil || n != 0 {
		t.Fatalf("empty corpus reclaimable = %d (err %v), want 0", n, err)
	}
}
