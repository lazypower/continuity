package store

import (
	"math"
	"testing"
)

func TestEncodeDecodeEmbedding(t *testing.T) {
	original := []float64{1.0, -0.5, 0.333, math.Pi, 0.0}
	blob := encodeEmbedding(original)
	decoded := decodeEmbedding(blob)

	if len(decoded) != len(original) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(original))
	}
	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: got %f, want %f", i, decoded[i], original[i])
		}
	}
}

func TestSaveAndGetVector(t *testing.T) {
	db := testDB(t)

	// Create a node to attach the vector to
	node := &MemNode{
		URI:      "mem://user/profile/coding-style",
		NodeType: "leaf",
		Category: "profile",
	}
	if err := db.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	embedding := []float64{0.1, 0.2, 0.3, 0.4, 0.5}
	if err := db.SaveVector(node.ID, embedding, "test-model"); err != nil {
		t.Fatalf("SaveVector: %v", err)
	}

	v, err := db.GetVector(node.ID)
	if err != nil {
		t.Fatalf("GetVector: %v", err)
	}
	if v == nil {
		t.Fatal("expected vector, got nil")
	}
	if v.Model != "test-model" {
		t.Errorf("model = %q, want %q", v.Model, "test-model")
	}
	if v.Dimensions != 5 {
		t.Errorf("dimensions = %d, want 5", v.Dimensions)
	}
	if len(v.Embedding) != 5 {
		t.Fatalf("embedding length = %d, want 5", len(v.Embedding))
	}
	for i := range embedding {
		if v.Embedding[i] != embedding[i] {
			t.Errorf("embedding[%d] = %f, want %f", i, v.Embedding[i], embedding[i])
		}
	}
}

func TestSaveVectorReplace(t *testing.T) {
	db := testDB(t)

	node := &MemNode{
		URI:      "mem://user/profile/coding-style",
		NodeType: "leaf",
		Category: "profile",
	}
	db.CreateNode(node)

	db.SaveVector(node.ID, []float64{0.1, 0.2}, "model-a")
	db.SaveVector(node.ID, []float64{0.3, 0.4, 0.5}, "model-b")

	v, _ := db.GetVector(node.ID)
	if v.Model != "model-b" {
		t.Errorf("model = %q, want %q", v.Model, "model-b")
	}
	if v.Dimensions != 3 {
		t.Errorf("dimensions = %d, want 3", v.Dimensions)
	}
}

func TestGetVectorNotFound(t *testing.T) {
	db := testDB(t)

	v, err := db.GetVector(999)
	if err != nil {
		t.Fatalf("GetVector: %v", err)
	}
	if v != nil {
		t.Error("expected nil for nonexistent vector")
	}
}

func TestAllVectors(t *testing.T) {
	db := testDB(t)

	n1 := &MemNode{URI: "mem://user/profile/a", NodeType: "leaf", Category: "profile"}
	n2 := &MemNode{URI: "mem://user/profile/b", NodeType: "leaf", Category: "profile"}
	db.CreateNode(n1)
	db.CreateNode(n2)

	db.SaveVector(n1.ID, []float64{0.1, 0.2}, "test")
	db.SaveVector(n2.ID, []float64{0.3, 0.4}, "test")

	all, err := db.AllVectors()
	if err != nil {
		t.Fatalf("AllVectors: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 vectors, got %d", len(all))
	}
}

func TestDeleteVector(t *testing.T) {
	db := testDB(t)

	node := &MemNode{URI: "mem://user/profile/a", NodeType: "leaf", Category: "profile"}
	db.CreateNode(node)
	db.SaveVector(node.ID, []float64{0.1, 0.2}, "test")

	if err := db.DeleteVector(node.ID); err != nil {
		t.Fatalf("DeleteVector: %v", err)
	}

	v, _ := db.GetVector(node.ID)
	if v != nil {
		t.Error("expected nil after delete")
	}
}
