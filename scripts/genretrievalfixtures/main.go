// Command genretrievalfixtures mints the retrieval golden fixture from a REAL
// Ollama nomic-embed-text. It embeds the curated corpus + query texts defined in
// internal/goldretrieval (the single source of truth shared with the test) and
// writes the recorded vectors to the committed golden.
//
// The committed fixture makes the PR-gate retrieval test hermetic (no Ollama).
// Regenerating on a schedule against current Ollama is the drift canary: a rank
// flip in the test after a regen is a real embedder regression that hit users.
//
// Usage:
//
//	go run ./scripts/genretrievalfixtures [-ollama URL] [-model M] [-dims N] [-out PATH]
//
// Requires Ollama running with the model pulled (`ollama pull nomic-embed-text`).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/goldretrieval"
)

func main() {
	url := flag.String("ollama", "http://localhost:11434", "Ollama base URL")
	model := flag.String("model", "nomic-embed-text", "embedding model")
	dims := flag.Int("dims", 768, "embedding dimension")
	out := flag.String("out", filepath.Join("internal", "engine", "testdata", "retrieval", "nomic.json"), "output fixture path")
	flag.Parse()

	if err := run(*url, *model, *dims, *out); err != nil {
		fmt.Fprintf(os.Stderr, "genretrievalfixtures: %v\n", err)
		os.Exit(1)
	}
}

func run(url, model string, dims int, out string) error {
	if !engine.ProbeOllama(url, model) {
		return fmt.Errorf("Ollama model %q not reachable at %s — run `ollama pull %s` and ensure `ollama serve` is up", model, url, model)
	}
	emb := engine.NewOllamaEmbedder(url, model, dims)
	ctx := context.Background()

	fx := goldretrieval.Fixture{
		Model:       emb.Model(),
		Dims:        dims,
		Fingerprint: goldretrieval.CorpusFingerprint(),
		CorpusVecs:  map[string][]float64{},
		QueryVecs:   map[string][]float64{},
	}

	for _, e := range goldretrieval.Corpus() {
		v, err := emb.Embed(ctx, e.L0)
		if err != nil {
			return fmt.Errorf("embed corpus %s: %w", e.URI, err)
		}
		fx.CorpusVecs[e.URI] = v
	}
	for _, a := range goldretrieval.Assertions() {
		if _, done := fx.QueryVecs[a.Query]; done {
			continue
		}
		v, err := emb.Embed(ctx, a.Query)
		if err != nil {
			return fmt.Errorf("embed query %q: %w", a.Query, err)
		}
		fx.QueryVecs[a.Query] = v
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("create fixture dir: %w", err)
	}
	if err := fx.Save(out); err != nil {
		return fmt.Errorf("write fixture: %w", err)
	}
	fmt.Printf("wrote %s (%s, %d-d): %d corpus vectors, %d query vectors\n",
		out, fx.Model, fx.Dims, len(fx.CorpusVecs), len(fx.QueryVecs))
	return nil
}
