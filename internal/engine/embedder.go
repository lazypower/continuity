package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/store"
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

func (o *OllamaEmbedder) Model() string  { return "ollama:" + o.model }
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

// TFIDFEmbedder generates TF-IDF bag-of-words embeddings as a fallback.
//
// Best-effort by construction: the corpus IS the model. Every retraction or
// new write that introduces vocabulary not yet in the IDF table shifts the
// vector space. We minimize the most load-bearing variant of that drift —
// retraction-induced drift — by including retracted nodes in the corpus
// (see NewTFIDFEmbedder). Ollama users have a static pre-trained model and
// don't have this problem; the README's "Embedding backends" section
// recommends Ollama for any setup that needs strong dedup-against-retracted
// recall.
type TFIDFEmbedder struct {
	vocab []string           // ordered vocabulary (top terms by doc frequency)
	idf   map[string]float64 // inverse document frequency per term
	dims  int
}

// NewTFIDFEmbedder builds a TF-IDF embedder from existing L0 abstracts.
//
// The corpus deliberately INCLUDES retracted leaves (issue #22). Excluding
// them would cause the IDF vocabulary to drift between process restarts that
// straddle a retraction: previously-stored vectors were embedded against a
// corpus that contained the now-retracted node's terms, while fresh
// embeddings would be computed against a corpus that no longer does. Cosine
// similarity between the two becomes incoherent, silently degrading
// findRetractedMatches recall and defeating the PII-re-introduction guard.
// Including retracted nodes keeps the vector space stable across retractions.
func NewTFIDFEmbedder(db *store.DB, maxTerms int) (*TFIDFEmbedder, error) {
	if maxTerms <= 0 {
		maxTerms = 512
	}

	leaves, err := db.ListLeavesIncludingRetracted()
	if err != nil {
		return nil, fmt.Errorf("list leaves for tfidf: %w", err)
	}

	// Collect documents (L0 abstracts)
	var docs []string
	for _, n := range leaves {
		if n.L0Abstract != "" {
			docs = append(docs, n.L0Abstract)
		}
	}

	// Build document frequency
	df := make(map[string]int)
	for _, doc := range docs {
		seen := make(map[string]bool)
		for _, term := range tokenize(doc) {
			if !seen[term] {
				df[term]++
				seen[term] = true
			}
		}
	}

	// Sort terms by document frequency (descending), take top maxTerms.
	//
	// Tie-break alphabetically (issue #22): Go map iteration is randomized
	// and sort.Slice isn't stable, so without an explicit tiebreaker the same
	// corpus produces vocabularies in different orders across constructions.
	// That puts identical terms at different vector positions, making cosine
	// similarity between vectors from two NewTFIDFEmbedder() calls effectively
	// random — even when the corpus didn't change. The asymmetric loss falls
	// hardest on dedup-against-retracted, which relies on the vector space
	// being stable across process restarts.
	type termFreq struct {
		term string
		freq int
	}
	var terms []termFreq
	for t, f := range df {
		terms = append(terms, termFreq{t, f})
	}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].freq != terms[j].freq {
			return terms[i].freq > terms[j].freq
		}
		return terms[i].term < terms[j].term
	})

	dims := maxTerms
	if len(terms) < dims {
		dims = len(terms)
	}
	if dims == 0 {
		dims = 1 // minimum dimension to avoid zero-length vectors
	}

	vocab := make([]string, dims)
	idf := make(map[string]float64)
	numDocs := float64(len(docs))
	if numDocs == 0 {
		numDocs = 1
	}

	for i := 0; i < dims && i < len(terms); i++ {
		vocab[i] = terms[i].term
		// IDF = log(N / df) + 1 (smoothed)
		idf[vocab[i]] = math.Log(numDocs/float64(terms[i].freq)) + 1.0
	}

	return &TFIDFEmbedder{
		vocab: vocab,
		idf:   idf,
		dims:  dims,
	}, nil
}

func (t *TFIDFEmbedder) Model() string  { return "tfidf" }
func (t *TFIDFEmbedder) Dimensions() int { return t.dims }

// Embed generates a normalized TF-IDF vector for the given text.
func (t *TFIDFEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return make([]float64, t.dims), nil
	}

	// Count term frequencies
	tf := make(map[string]int)
	for _, tok := range tokens {
		tf[tok]++
	}

	// Build TF-IDF vector
	vec := make([]float64, t.dims)
	maxTF := 0
	for _, c := range tf {
		if c > maxTF {
			maxTF = c
		}
	}

	for i, term := range t.vocab {
		count := tf[term]
		if count == 0 {
			continue
		}
		// Augmented TF to prevent bias towards longer documents
		augTF := 0.5 + 0.5*float64(count)/float64(maxTF)
		idf := t.idf[term]
		if idf == 0 {
			idf = 1.0
		}
		vec[i] = augTF * idf
	}

	// L2 normalize
	normalize(vec)
	return vec, nil
}

// tokenize splits text into lowercase tokens, stripping punctuation.
func tokenize(text string) []string {
	text = strings.ToLower(text)
	var tokens []string
	var current strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
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
