package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

// Embedder generates vector embeddings for text.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float64, error)
	Model() string
	Dimensions() int
}

// OllamaEmbedder uses Ollama's embedding API.
type OllamaEmbedder struct {
	url    string
	model  string
	dims   int
	client *http.Client
}

// NewOllamaEmbedder creates an embedder using Ollama's API.
func NewOllamaEmbedder(url, model string, dims int) *OllamaEmbedder {
	return &OllamaEmbedder{
		url:    url,
		model:  model,
		dims:   dims,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (o *OllamaEmbedder) Model() string   { return "ollama:" + o.model }
func (o *OllamaEmbedder) Dimensions() int { return o.dims }

// Embed sends text to Ollama's embed endpoint and returns the embedding vector.
func (o *OllamaEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	reqBody := map[string]any{
		"model": o.model,
		"input": text,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal embed request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", o.url+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama embed api: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read embed response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama embed status %d: %s", resp.StatusCode, respBody)
	}

	var result struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode embed response: %w", err)
	}
	if len(result.Embeddings) == 0 {
		return nil, fmt.Errorf("ollama returned no embeddings")
	}

	o.dims = len(result.Embeddings[0])
	return result.Embeddings[0], nil
}

// ProbeOllama checks if Ollama is reachable and the embedding model is available.
func ProbeOllama(url, model string) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	reqBody, _ := json.Marshal(map[string]any{
		"model": model,
		"input": "test",
	})
	resp, err := client.Post(url+"/api/embed", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// HashEmbedder is a fixed-dimension feature-hashed lexical embedder used as the
// Ollama-free fallback.
//
// Each term is hashed to a fixed bucket (hash(term) mod dims), so the coordinate
// system is constant forever — independent of corpus size, age, vocabulary, or
// process restarts. That is exactly what the retraction-resurrection gate, the
// dedup pass, and search all assume: two vectors are only comparable if they
// share a coordinate system. The predecessor (corpus-derived TF-IDF) rebuilt its
// vocabulary from the live corpus on every construction, so its axes drifted as
// memories were added or retracted; a fresh embedder and the stored vectors then
// lived in different spaces and cosine collapsed to noise (or 0 on a dimension
// mismatch), silently defeating the PII-resurrection guard.
//
// This is a STABLE LEXICAL safety net, not a semantic embedder: similarity is
// keyword overlap, not meaning. The trade is deliberate — we lower the ambition
// (no semantic recall, no IDF term-discrimination) to buy total stability. The
// README's "Embedding backends" section recommends Ollama for setups that need
// semantic recall. Properties this guarantees that corpus-TF-IDF did not:
//   - no OOV: every term hashes somewhere, so rare/new terms always contribute
//     (corpus-TF-IDF dropped any term outside its top-N vocabulary);
//   - works on a fresh/empty DB from the first write (no startup corpus needed);
//   - deterministic across restarts and across machines.
//
// Collisions (distinct terms sharing a bucket) are the cost; they are tuned away
// by dims. Signed hashing (a per-term ±1) keeps collisions unbiased in
// expectation rather than always additive.
type HashEmbedder struct {
	dims int
}

// defaultHashDims is the fixed feature-hash dimension. 2048 keeps collisions
// negligible for personal-scale corpora while keeping vectors cheap (they are
// sparse — only buckets for terms actually present are non-zero).
const defaultHashDims = 2048

// NewHashEmbedder builds a feature-hashed lexical embedder. dims <= 0 selects
// the default (defaultHashDims). It takes no corpus: the embedding of a given
// text is a pure function of the text and dims, which is the whole point.
//
// The error return is always nil today — construction is infallible because
// there is no corpus to read. It exists so the fallback slots into the
// (Embedder, error) construction factory (resolveActiveEmbedder) uniformly with
// the other backends, and leaves room for future config validation without a
// signature change.
func NewHashEmbedder(dims int) (*HashEmbedder, error) {
	if dims <= 0 {
		dims = defaultHashDims
	}
	return &HashEmbedder{dims: dims}, nil
}

func (h *HashEmbedder) Model() string   { return "hashtf" }
func (h *HashEmbedder) Dimensions() int { return h.dims }

// Embed generates a normalized hashed-TF vector for the given text. Sublinear
// term frequency (1 + log(count)) damps repetition; signed feature hashing keeps
// collisions unbiased; L2 normalization makes cosine length-independent.
func (h *HashEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	vec := make([]float64, h.dims)
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return vec, nil
	}

	tf := make(map[string]int)
	for _, tok := range tokens {
		if _, stop := lexicalStopwords[tok]; stop {
			continue // see lexicalStopwords: static stand-in for IDF down-weighting
		}
		tf[tok]++
	}

	for term, count := range tf {
		bucket, sign := hashFeature(term, h.dims)
		weight := 1.0 + math.Log(float64(count)) // sublinear TF
		vec[bucket] += sign * weight
	}

	normalize(vec)
	return vec, nil
}

// lexicalStopwords is a fixed, corpus-independent set of high-frequency English
// function words dropped before hashing. The legacy TF-IDF embedder leaned on
// IDF to down-weight ubiquitous words so that distinctive content terms drove
// similarity; dropping IDF (for coordinate stability) removed that, which both
// muddied paraphrase recall (the retraction gate's whole job) and let unrelated
// texts share spurious "the/in/by" overlap. A static stopword list restores the
// discrimination deterministically — no corpus, so no drift. It is intentionally
// conservative: only unambiguous function words, never content words.
var lexicalStopwords = func() map[string]struct{} {
	words := []string{
		"the", "a", "an", "and", "or", "but", "if", "then", "so", "as", "of",
		"to", "in", "on", "at", "by", "for", "with", "from", "into", "onto",
		"over", "under", "about", "after", "before", "up", "out", "down", "off",
		"is", "are", "was", "were", "be", "been", "being", "am",
		"do", "does", "did", "has", "have", "had", "having",
		"will", "would", "can", "could", "should", "shall", "may", "might", "must",
		"this", "that", "these", "those", "it", "its", "they", "them", "their",
		"he", "she", "his", "her", "him", "we", "us", "our", "you", "your",
		"i", "me", "my", "not", "no", "nor", "yes", "all", "any", "some", "each",
		"there", "here", "what", "which", "who", "whom", "whose", "when", "where",
		"why", "how", "than", "too", "very", "just", "also", "only", "more", "most",
	}
	m := make(map[string]struct{}, len(words))
	for _, w := range words {
		m[w] = struct{}{}
	}
	return m
}()

// lexicalMatchThreshold is the cosine bar for the hashed lexical fallback in the
// dedup / retraction-resurrection gates. It is lower than defaultSimilarityThreshold
// (used for semantic embedders) because keyword-overlap cosine for a genuine
// paraphrase is inherently lower than semantic cosine — a semantic-calibrated
// 0.65 would silently miss paraphrased retracted PII. Stopword removal keeps
// unrelated-text cosine well below this, preserving separation.
const lexicalMatchThreshold = 0.5

// MatchThreshold returns the cosine threshold for treating two embeddings as the
// "same" memory in the dedup and retraction-resurrection gates. The hashed
// lexical fallback gets a lower, separately-calibrated bar; semantic and unknown
// embedders use the default.
func MatchThreshold(emb Embedder) float64 {
	if emb != nil && emb.Model() == "hashtf" {
		return lexicalMatchThreshold
	}
	return defaultSimilarityThreshold
}

// hashFeature maps a term to its bucket in [0, dims) and a deterministic ±1 sign
// (the signed-hashing trick). Bucket and sign come from independent regions of a
// 64-bit FNV-1a hash: the low bits choose the bucket, the high bit the sign.
func hashFeature(term string, dims int) (bucket int, sign float64) {
	hsh := fnv.New64a()
	_, _ = hsh.Write([]byte(term))
	sum := hsh.Sum64()
	bucket = int(sum % uint64(dims))
	if sum&(1<<63) == 0 {
		return bucket, 1.0
	}
	return bucket, -1.0
}

// tokenize splits text into lowercase alphanumeric tokens, treating every other
// rune — including '-' and '_' — as a separator.
//
// Splitting on '-'/'_' (rather than keeping them inside tokens) is load-bearing
// for the retraction-resurrection gate: the same PII reformatted differently
// must still collide. Otherwise "phone 555-123-4567" tokenizes to one opaque
// token while "phone 555 123 4567" tokenizes to three, the two share almost no
// buckets, and the gate misses the exact same number. As a bonus, compound terms
// like "wal-mode" now match their spaced form "wal mode".
func tokenize(text string) []string {
	text = strings.ToLower(text)
	var tokens []string
	var current strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			current.WriteRune(r)
		} else {
			if current.Len() > 1 { // skip single-char tokens
				tokens = append(tokens, current.String())
			}
			current.Reset()
		}
	}
	if current.Len() > 1 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// normalize performs in-place L2 normalization.
func normalize(vec []float64) {
	var sum float64
	for _, v := range vec {
		sum += v * v
	}
	if sum == 0 {
		return
	}
	norm := math.Sqrt(sum)
	for i := range vec {
		vec[i] /= norm
	}
}

// CosineSimilarity computes the cosine similarity between two vectors.
// Assumes vectors are already L2-normalized for embeddings from Ollama;
// works correctly on unnormalized vectors too.
func CosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return dot / denom
}
