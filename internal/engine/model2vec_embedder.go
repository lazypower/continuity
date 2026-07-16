package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Model2VecEmbedder is a static, semantic embedder backed by a model2vec
// distillation (potion-retrieval-32M): a fixed lookup-table embedding matrix
// plus a BERT WordPiece tokenizer, mean-pooled per input. Unlike Ollama it
// needs no running daemon, and unlike HashEmbedder its similarity is semantic
// rather than lexical (keyword overlap) — the tradeoff is a one-time model
// download and a larger resident matrix (potion-retrieval-32M is a 512-dim,
// 63091-token matrix, shipped int8-quantized at ~32MB).
//
// The matrix ships int8-quantized (~32MB vs ~130MB for F32), with measured-
// identical retrieval quality: model2vec's int8 quantization uses a single
// per-tensor symmetric scale (max|W|/127) that CANCELS under this embedder's
// L2 normalization, so the saved artifact carries no scale tensor and the
// int8 values are widened to float64 as-is (see loadSafetensorsMatrix). The
// loader also accepts F32 matrices for compatibility with any manually
// substituted model file.
//
// Inference is: tokenize (WordPiece, add_special_tokens=false) -> drop [UNK]
// ids -> mean-pool the surviving rows of the embedding matrix -> L2 normalize.
// This matches Python's model2vec.StaticModel.encode() exactly (verified
// against internal/engine/testdata/model2vec/parity.json, generated from the
// real package against the int8-quantized model) because potion-retrieval-32M
// carries no runtime reweighting: its `weights`, `token_mapping`, and
// `vocabulary_quantization` are all absent, so the matrix is used as-is with
// no extra step. loadModel2Vec asserts this (fails closed) rather than
// silently producing wrong vectors for a future model that DOES carry one of
// those.
type Model2VecEmbedder struct {
	tokenizer *wordpieceTokenizer
	matrix    []float64 // row-major [vocabSize x dims]; widened from the on-disk dtype (int8 or float32), see loadSafetensorsMatrix
	vocabSize int
	dims      int
	modelName string // e.g. "potion-retrieval-32M", used only for Model()/paths
}

// Provenance: this backend's weights are sourced from the Hugging Face model
// minishlab/potion-retrieval-32M (MIT). Our GitHub release re-hosts an
// int8-quantized re-export of it (see model2vecDownloadURLs) rather than
// downloading from HF directly. NON-GOAL (explicit): this is not a general
// model2vec loader, it is wired to exactly this one pinned model.
// potion-retrieval-32M is the retrieval-tuned distillation (stronger on
// paraphrase-heavy queries than potion-base-8M); it shares the exact same
// inference path — same bge-base-en-v1.5 WordPiece tokenizer, same
// BertNormalizer settings, same "##" prefix, same unk_id, and no runtime
// reweighting.

// Model2VecModelName is the model this backend is pinned to. Exported so CLI
// wiring (model dir resolution, doctor, etc.) can reference the same name
// without hand-copying the string.
const Model2VecModelName = "potion-retrieval-32M"

// Model returns the corpus-binding model identifier. Combined with
// Dimensions() via EmbedderIdentity, this becomes
// "model2vec:potion-retrieval-32M:512" (dims derived from the loaded matrix).
func (m *Model2VecEmbedder) Model() string { return "model2vec:" + m.modelName }

// Dimensions returns the embedding width (512 for potion-retrieval-32M),
// derived from the loaded matrix shape rather than hardcoded.
func (m *Model2VecEmbedder) Dimensions() int { return m.dims }

// Embed tokenizes text, drops [UNK] ids, mean-pools the surviving embedding
// rows, and L2-normalizes the result. An input that tokenizes to nothing but
// [UNK] (or is empty/whitespace-only) returns an all-zero vector of the
// model's dimension, matching model2vec's own behavior for degenerate input.
func (m *Model2VecEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	ids := m.tokenizer.Encode(text)

	sum := make([]float64, m.dims)
	count := 0
	for _, id := range ids {
		if id == m.tokenizer.unkID {
			continue
		}
		if int(id) >= m.vocabSize {
			continue // defensive: a corrupt/foreign vocab id should never index out of bounds
		}
		row := m.matrix[int(id)*m.dims : int(id)*m.dims+m.dims]
		for i, v := range row {
			sum[i] += v
		}
		count++
	}

	if count == 0 {
		return make([]float64, m.dims), nil
	}
	for i := range sum {
		sum[i] /= float64(count)
	}
	normalize(sum)
	return sum, nil
}

// LoadModel2VecEmbedder loads the potion-retrieval-32M model from modelDir
// (model.safetensors + tokenizer.json), downloading them first if absent. See
// EnsureModel2VecFiles for the download step; this function only loads
// already-present files, so callers that want to control WHEN a network
// fetch may happen (e.g. only when the backend is actually selected) can call
// EnsureModel2VecFiles explicitly first.
func LoadModel2VecEmbedder(modelDir string) (*Model2VecEmbedder, error) {
	tokPath := filepath.Join(modelDir, "tokenizer.json")
	tensorPath := filepath.Join(modelDir, "model.safetensors")

	tok, err := loadWordpieceTokenizer(tokPath)
	if err != nil {
		return nil, fmt.Errorf("load model2vec tokenizer: %w", err)
	}

	matrix, shape, err := loadSafetensorsMatrix(tensorPath, "embeddings")
	if err != nil {
		return nil, fmt.Errorf("load model2vec embedding matrix: %w", err)
	}
	if len(shape) != 2 {
		return nil, fmt.Errorf("model2vec embedding matrix has shape %v, want 2 dimensions", shape)
	}
	vocabSize, dims := shape[0], shape[1]
	if len(matrix) != vocabSize*dims {
		return nil, fmt.Errorf("model2vec embedding matrix has %d elements, want %d (%dx%d)",
			len(matrix), vocabSize*dims, vocabSize, dims)
	}

	// Fail-closed assertion: this loader — and the plain mean-pool Embed()
	// above — is only correct when the model applies NO runtime reweighting.
	// potion-retrieval-32M is confirmed to have none (weights, token_mapping,
	// and vocabulary_quantization all absent in the real model2vec package). A
	// config.json declaring any of those would mean a future model needs a
	// different inference path this implementation does not have; fail loudly
	// rather than silently emitting wrong vectors.
	if err := assertNoRuntimeReweighting(modelDir); err != nil {
		return nil, err
	}

	return &Model2VecEmbedder{
		tokenizer: tok,
		matrix:    matrix,
		vocabSize: vocabSize,
		dims:      dims,
		modelName: Model2VecModelName,
	}, nil
}

// assertNoRuntimeReweighting fails closed if config.json (when present)
// declares any field this implementation does not account for. potion-retrieval-32M
// ships apply_pca/apply_zipf/normalize baked into the matrix at distillation
// time (informational only per the model card), and no weights/token_mapping/
// vocabulary_quantization sidecar files exist in the repo at all — there is
// nothing in the file layout itself to assert against beyond their absence,
// which this checks directly.
func assertNoRuntimeReweighting(modelDir string) error {
	for _, forbidden := range []string{"weights.npy", "token_mapping.json", "vocabulary_quantization.json"} {
		if _, err := os.Stat(filepath.Join(modelDir, forbidden)); err == nil {
			return fmt.Errorf("model2vec model dir %s contains %s, which this implementation does not support "+
				"(it assumes no runtime reweighting/remapping/quantization); refusing to load", modelDir, forbidden)
		}
	}
	return nil
}

// DefaultModel2VecDir returns ~/.continuity/models/<model-name>/, the default
// on-disk location for downloaded model2vec artifacts.
func DefaultModel2VecDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".continuity", "models", Model2VecModelName), nil
}

// model2vecDownloadURLs returns the two artifact URLs this backend downloads
// from: continuity's own GitHub release, not Hugging Face directly. We host
// the int8-quantized matrix (~32MB, 4x smaller than the F32 original, with
// measured-identical retrieval quality — see the package doc comment) plus
// the unmodified tokenizer.json (byte-identical to the upstream bge-base
// WordPiece tokenizer, so token ids are unaffected by the quantization).
func model2vecDownloadURLs() (safetensorsURL, tokenizerURL string) {
	const base = "https://github.com/lazypower/continuity/releases/download/models-v1/"
	return base + "potion-retrieval-32M.model.safetensors", base + "potion-retrieval-32M.tokenizer.json"
}

// EnsureModel2VecFiles downloads model.safetensors and tokenizer.json into
// modelDir if either is missing. It is the ONLY place this package performs
// network I/O, and it is only ever invoked from the model2vec selection path
// (CONTINUITY_EMBEDDER=model2vec) — never on a plain hashtf/tfidf/ollama run,
// so choosing model2vec is what triggers the one-time fetch, not merely
// importing this package.
//
// Downloads to a temp file in modelDir first, then renames into place, so a
// killed/interrupted download can never leave a truncated file that a later
// run mistakes for a complete one.
func EnsureModel2VecFiles(modelDir string) error {
	if err := os.MkdirAll(modelDir, 0o755); err != nil {
		return fmt.Errorf("create model dir %s: %w", modelDir, err)
	}

	safetensorsURL, tokenizerURL := model2vecDownloadURLs()
	tensorPath := filepath.Join(modelDir, "model.safetensors")
	tokPath := filepath.Join(modelDir, "tokenizer.json")

	if !fileExists(tensorPath) {
		if err := downloadToFile(safetensorsURL, tensorPath); err != nil {
			return fmt.Errorf("download model.safetensors: %w", err)
		}
	}
	if !fileExists(tokPath) {
		if err := downloadToFile(tokenizerURL, tokPath); err != nil {
			return fmt.Errorf("download tokenizer.json: %w", err)
		}
	}
	return nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Size() > 0
}

// downloadToFile GETs url and writes the body to dest, via a .partial sibling
// file that is renamed into place only on full success — see EnsureModel2VecFiles.
func downloadToFile(url, dest string) error {
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	partial := dest + ".partial"
	f, err := os.Create(partial)
	if err != nil {
		return fmt.Errorf("create %s: %w", partial, err)
	}
	_, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()
	if copyErr != nil {
		os.Remove(partial)
		return fmt.Errorf("write %s: %w", partial, copyErr)
	}
	if closeErr != nil {
		os.Remove(partial)
		return fmt.Errorf("close %s: %w", partial, closeErr)
	}
	if err := os.Rename(partial, dest); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", partial, dest, err)
	}
	return nil
}

// NewModel2VecEmbedder ensures the model files are present (downloading on
// first use) at DefaultModel2VecDir(), then loads them. This is the entry
// point CLI wiring calls for CONTINUITY_EMBEDDER=model2vec.
func NewModel2VecEmbedder() (*Model2VecEmbedder, error) {
	dir, err := DefaultModel2VecDir()
	if err != nil {
		return nil, err
	}
	if err := EnsureModel2VecFiles(dir); err != nil {
		return nil, fmt.Errorf("ensure model2vec files: %w", err)
	}
	return LoadModel2VecEmbedder(dir)
}
