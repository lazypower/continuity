package engine

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// writeTestSafetensors builds a minimal valid .safetensors file with a single
// F32 tensor, for testing the header parser without needing the real 29MB
// model file.
func writeTestSafetensors(t *testing.T, path, tensorName string, shape []int, data []float32) {
	t.Helper()

	raw := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(v))
	}

	header := map[string]safetensorsTensorInfo{
		tensorName: {
			Dtype:       "F32",
			Shape:       shape,
			DataOffsets: [2]int{0, len(raw)},
		},
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(headerBytes)))
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatalf("write header length: %v", err)
	}
	if _, err := f.Write(headerBytes); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := f.Write(raw); err != nil {
		t.Fatalf("write data: %v", err)
	}
}

func TestLoadSafetensorsF32_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.safetensors")
	want := []float32{1.5, -2.25, 0, 3.125, 100.0, -0.5}
	writeTestSafetensors(t, path, "embeddings", []int{2, 3}, want)

	got, shape, err := loadSafetensorsMatrix(path, "embeddings")
	if err != nil {
		t.Fatalf("loadSafetensorsMatrix: %v", err)
	}
	if len(shape) != 2 || shape[0] != 2 || shape[1] != 3 {
		t.Fatalf("shape = %v, want [2 3]", shape)
	}
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != float64(want[i]) {
			t.Errorf("got[%d] = %f, want %f", i, got[i], want[i])
		}
	}
}

// writeTestSafetensorsI8 builds a minimal valid .safetensors file with a
// single I8 tensor, for testing the int8 widening path without needing the
// real ~32MB quantized model file.
func writeTestSafetensorsI8(t *testing.T, path, tensorName string, shape []int, data []int8) {
	t.Helper()

	raw := make([]byte, len(data))
	for i, v := range data {
		raw[i] = byte(v)
	}

	header := map[string]safetensorsTensorInfo{
		tensorName: {
			Dtype:       "I8",
			Shape:       shape,
			DataOffsets: [2]int{0, len(raw)},
		},
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(headerBytes)))
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatalf("write header length: %v", err)
	}
	if _, err := f.Write(headerBytes); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := f.Write(raw); err != nil {
		t.Fatalf("write data: %v", err)
	}
}

// TestLoadSafetensorsI8_RoundTrip pins the widening behavior the int8 model
// path depends on: each stored byte is reinterpreted as a signed int8 and
// widened to float64 AS-IS, with NO dequantization scale applied (see the
// loadSafetensorsMatrix doc comment for why that is correct rather than a
// shortcut — model2vec's single per-tensor scale cancels under the
// embedder's L2 normalization).
func TestLoadSafetensorsI8_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.safetensors")
	want := []int8{127, -128, 0, 1, -1, 64}
	writeTestSafetensorsI8(t, path, "embeddings", []int{2, 3}, want)

	got, shape, err := loadSafetensorsMatrix(path, "embeddings")
	if err != nil {
		t.Fatalf("loadSafetensorsMatrix: %v", err)
	}
	if len(shape) != 2 || shape[0] != 2 || shape[1] != 3 {
		t.Fatalf("shape = %v, want [2 3]", shape)
	}
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != float64(want[i]) {
			t.Errorf("got[%d] = %f, want %f", i, got[i], float64(want[i]))
		}
	}
}

func TestLoadSafetensorsMatrix_MissingTensor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.safetensors")
	writeTestSafetensors(t, path, "embeddings", []int{1, 2}, []float32{1, 2})

	_, _, err := loadSafetensorsMatrix(path, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing tensor name, got nil")
	}
}

func TestLoadSafetensorsMatrix_UnsupportedDtype(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.safetensors")

	header := map[string]safetensorsTensorInfo{
		"embeddings": {Dtype: "F16", Shape: []int{2, 2}, DataOffsets: [2]int{0, 8}},
	}
	headerBytes, _ := json.Marshal(header)
	f, _ := os.Create(path)
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(headerBytes)))
	f.Write(lenBuf[:])
	f.Write(headerBytes)
	f.Write(make([]byte, 8))
	f.Close()

	_, _, err := loadSafetensorsMatrix(path, "embeddings")
	if err == nil {
		t.Fatal("expected error for unsupported (F16) dtype, got nil")
	}
}

func TestLoadSafetensorsF32_TruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.safetensors")

	header := map[string]safetensorsTensorInfo{
		"embeddings": {Dtype: "F32", Shape: []int{10, 10}, DataOffsets: [2]int{0, 400}},
	}
	headerBytes, _ := json.Marshal(header)
	f, _ := os.Create(path)
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(headerBytes)))
	f.Write(lenBuf[:])
	f.Write(headerBytes)
	f.Write(make([]byte, 4)) // far short of the declared 400 bytes
	f.Close()

	_, _, err := loadSafetensorsMatrix(path, "embeddings")
	if err == nil {
		t.Fatal("expected error for truncated tensor data, got nil")
	}
}

// TestLoadModel2VecEmbedder_RealModel loads the actual downloaded
// potion-retrieval-32M artifacts (skipped if absent — see model2vecTestModelDir)
// and asserts the loader's core invariants: dimension is 512 and vocab size is
// 63091 (both DERIVED from the safetensors matrix shape, not hardcoded in the
// loader), and the "no runtime reweighting" assertion passes (no
// weights.npy/token_mapping.json/vocabulary_quantization.json sidecar files
// exist for this model).
func TestLoadModel2VecEmbedder_RealModel(t *testing.T) {
	emb := loadTestEmbedder(t)

	if emb.Dimensions() != 512 {
		t.Errorf("Dimensions() = %d, want 512", emb.Dimensions())
	}
	if emb.vocabSize != 63091 {
		t.Errorf("vocabSize = %d, want 63091", emb.vocabSize)
	}
	if len(emb.matrix) != emb.vocabSize*emb.dims {
		t.Errorf("matrix has %d elements, want %d", len(emb.matrix), emb.vocabSize*emb.dims)
	}
}

// TestAssertNoRuntimeReweighting_FailsClosedOnSidecarFiles pins that the
// loader refuses to proceed if a future model ships one of the
// reweighting/remapping/quantization sidecar files this implementation does
// not support, rather than silently loading and producing wrong vectors.
func TestAssertNoRuntimeReweighting_FailsClosedOnSidecarFiles(t *testing.T) {
	for _, forbidden := range []string{"weights.npy", "token_mapping.json", "vocabulary_quantization.json"} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, forbidden), []byte("{}"), 0o644); err != nil {
			t.Fatalf("write %s: %v", forbidden, err)
		}
		if err := assertNoRuntimeReweighting(dir); err == nil {
			t.Errorf("expected assertNoRuntimeReweighting to fail closed when %s is present", forbidden)
		}
	}
}

func TestAssertNoRuntimeReweighting_PassesWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	if err := assertNoRuntimeReweighting(dir); err != nil {
		t.Errorf("expected no error for a dir with no sidecar files, got %v", err)
	}
}
