package store

import "testing"

func TestMetaGetSetUpsert(t *testing.T) {
	db := testDB(t)

	if _, ok, err := db.GetMeta("nope"); err != nil || ok {
		t.Fatalf("absent key: ok=%v err=%v", ok, err)
	}
	if err := db.SetMeta("k", "v1"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := db.GetMeta("k"); err != nil || !ok || v != "v1" {
		t.Fatalf("get after set: %q ok=%v err=%v", v, ok, err)
	}
	if err := db.SetMeta("k", "v2"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := db.GetMeta("k"); v != "v2" {
		t.Fatalf("upsert failed: %q", v)
	}
}

func TestVectorIdentityRoundtrip(t *testing.T) {
	db := testDB(t)

	if _, ok, _ := db.VectorIdentity(); ok {
		t.Fatal("fresh DB should have no declared vector identity")
	}
	if err := db.SetVectorIdentity("tfidf:512"); err != nil {
		t.Fatal(err)
	}
	if id, ok, _ := db.VectorIdentity(); !ok || id != "tfidf:512" {
		t.Fatalf("identity roundtrip: %q ok=%v", id, ok)
	}
}

func TestVectorModelCounts(t *testing.T) {
	db := testDB(t)
	n := &MemNode{URI: "mem://agent/patterns/a", NodeType: "leaf", Category: "patterns", L0Abstract: "x"}
	if err := db.CreateNode(n); err != nil {
		t.Fatal(err)
	}
	got, _ := db.GetNodeByURI("mem://agent/patterns/a")
	if err := db.SaveVector(got.ID, make([]float64, 768), "ollama:nomic-embed-text"); err != nil {
		t.Fatal(err)
	}

	counts, err := db.VectorModelCounts()
	if err != nil {
		t.Fatal(err)
	}
	if counts["ollama:nomic-embed-text:768"] != 1 {
		t.Fatalf("counts = %+v", counts)
	}
}
