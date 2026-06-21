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

	if err := runDoctorRepair(db, emb, false); err != nil { // dry-run
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

func TestDoctorRepairApplyReembedsAndRebinds(t *testing.T) {
	db, id := repairTestDB(t)
	emb := repairStubEmbedder{model: "new-model", dims: 64}

	if err := runDoctorRepair(db, emb, true); err != nil { // apply
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
