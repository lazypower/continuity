package cli

import (
	"context"
	"testing"

	"github.com/lazypower/continuity/internal/store"
)

type repairStubEmbedder struct {
	model string
	dims  int
}

func (s repairStubEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, s.dims)
	for i, r := range text {
		v[i%s.dims] += float64(r)
	}
	return v, nil
}
func (s repairStubEmbedder) Model() string   { return s.model }
func (s repairStubEmbedder) Dimensions() int { return s.dims }

func repairTestDB(t *testing.T) (*store.DB, int64) {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	n := &store.MemNode{URI: "mem://agent/patterns/a", NodeType: "leaf", Category: "patterns", L0Abstract: "alpha"}
	if err := db.CreateNode(n); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	got, _ := db.GetNodeByURI("mem://agent/patterns/a")
	if err := db.SaveVector(got.ID, make([]float64, 512), "old-model"); err != nil {
		t.Fatalf("SaveVector: %v", err)
	}
	return db, got.ID
}

func TestDoctorRepairDryRunMakesNoChanges(t *testing.T) {
	db, id := repairTestDB(t)
	emb := repairStubEmbedder{model: "new-model", dims: 64}

	if err := runDoctorRepair(db, emb, false, serverIdentity{}); err != nil { // dry-run, no server
		t.Fatal(err)
	}
	v, _ := db.GetVector(id)
	if v == nil || v.Model != "old-model" {
		t.Fatalf("dry-run must not change vectors, got %+v", v)
	}
	if _, ok, _ := db.VectorIdentity(); ok {
		t.Fatal("dry-run must not bind a vector identity")
	}
}

// TestDoctorRepairRefusesUnderLiveServer pins Codex round-4: --apply must refuse
// when a reachable, unlocked server reports a different (or unknown/empty, e.g.
// an old binary mid-upgrade) identity than the repair target.
func TestDoctorRepairRefusesUnderLiveServer(t *testing.T) {
	db, id := repairTestDB(t)
	emb := repairStubEmbedder{model: "new-model", dims: 64}

	// Different identity:
	if err := runDoctorRepair(db, emb, true, serverIdentity{Reachable: true, ActiveEmbedder: "other:768"}); err == nil {
		t.Fatal("apply must refuse under a live server with a different identity")
	}
	// Unknown/empty identity (old pre-vector-identity server):
	if err := runDoctorRepair(db, emb, true, serverIdentity{Reachable: true, ActiveEmbedder: ""}); err == nil {
		t.Fatal("apply must refuse under a reachable server reporting an unknown identity")
	}
	// Nothing should have been written.
	if v, _ := db.GetVector(id); v == nil || v.Model != "old-model" {
		t.Fatalf("refused repair must not write; got %+v", v)
	}
	// A LOCKED server is safe (not writing):
	if err := runDoctorRepair(db, emb, true, serverIdentity{Reachable: true, Locked: true}); err != nil {
		t.Fatalf("apply under a locked server should proceed: %v", err)
	}
}

func TestDoctorRepairApplyReembedsAndRebinds(t *testing.T) {
	db, id := repairTestDB(t)
	emb := repairStubEmbedder{model: "new-model", dims: 64}

	if err := runDoctorRepair(db, emb, true, serverIdentity{}); err != nil { // apply, no live server
		t.Fatal(err)
	}
	v, _ := db.GetVector(id)
	if v == nil || v.Model != "new-model" || v.Dimensions != 64 {
		t.Fatalf("apply must re-embed to the active embedder, got %+v", v)
	}
	if gotID, ok, _ := db.VectorIdentity(); !ok || gotID != "new-model:64" {
		t.Fatalf("apply must rebind identity, got %q ok=%v", gotID, ok)
	}
}
