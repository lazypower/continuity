// Package goldretrieval is the single source of truth for the retrieval golden
// fixtures: the curated corpus + query assertions, plus the on-disk fixture
// format and a replay embedder.
//
// The flow mirrors the migration goldens (see scripts/genfixtures +
// migration_fixture_test.go), with "a real Ollama nomic" standing in for "a real
// released binary":
//
//  1. scripts/genretrievalfixtures embeds Corpus() and the Assertions() queries
//     with a REAL Ollama nomic-embed-text and writes the vectors to
//     testdata/retrieval/nomic.json (the committed golden).
//  2. The hermetic PR test (engine.retrieval_golden_test) loads that JSON and
//     replays the recorded vectors through the REAL Find() — no Ollama needed —
//     asserting ranked-order PROPERTIES with score margins (not exact vectors,
//     which would be brittle across model versions).
//  3. A scheduled job regenerates the fixture against current Ollama and runs the
//     same test: a rank flip is a real embedder regression that hit users too.
//
// The corpus is a small CURATED set (not real user memories): stable, PII-free,
// and designed to exercise the ranking properties we care about — including the
// exact "devbox" scenario whose mis-ranking kicked off the vector-identity work.
package goldretrieval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// Entry is one curated corpus memory.
type Entry struct {
	URI      string
	Category string
	L0       string
}

// Corpus returns the curated fixture corpus. Keep it small and topically
// separated; each L0 is also used as a self-retrieval query.
func Corpus() []Entry {
	return []Entry{
		{"mem://user/preferences/devbox-go", "preferences", "Always use devbox run for go commands in labdns"},
		{"mem://user/entities/go-sandbox-runtime", "entities", "Go sandbox runtime: sandboxed execution for agents with sandbox and loop primitives"},
		{"mem://agent/patterns/branch-pr-model", "patterns", "Main is protected on most repos; always use a branch and pull request"},
		{"mem://user/preferences/data-safety", "preferences", "Data safety is paramount; snapshot before any destructive operation"},
		{"mem://agent/cases/content-truncation", "cases", "App shows truncated messages because the 70B model's context window is exhausted"},
		{"mem://user/profile/communication", "profile", "Sparse praise; gives feedback as collaborative discovery rather than directives"},
		{"mem://agent/patterns/continuity-release", "patterns", "Release: merge to main, wait for CI green, then push the version tag separately"},
		{"mem://user/preferences/git-dual-remotes", "preferences", "Two git remotes: origin is the homelab server, github is GitHub"},
	}
}

// Assertion is a ranked-order property a query must satisfy. Top must rank #1.
// If Above is set, Top must outrank it by at least MinMargin; otherwise Top must
// beat the second-place result by MinMargin.
type Assertion struct {
	Query     string
	Top       string  // URI that must rank first
	Above     string  // optional URI that Top must beat by a margin
	MinMargin float64 // minimum score gap
}

// Assertions returns the hand-written topical queries. Self-retrieval assertions
// (query == each entry's own L0 ⇒ that entry ranks #1) are generated from
// Corpus() in the test, so they need not be listed here.
func Assertions() []Assertion {
	return []Assertion{
		// The bug that started it all: "devbox" must surface the devbox preference,
		// and must beat the lexically-adjacent "go-sandbox-runtime" by a real margin
		// (TF-IDF buried it; nomic must not).
		{Query: "devbox", Top: "mem://user/preferences/devbox-go", Above: "mem://user/entities/go-sandbox-runtime", MinMargin: 0.05},
		{Query: "how do I open a branch and pull request", Top: "mem://agent/patterns/branch-pr-model", MinMargin: 0.04},
		{Query: "snapshot before destructive operations", Top: "mem://user/preferences/data-safety", MinMargin: 0.04},
		{Query: "what are the two git remotes", Top: "mem://user/preferences/git-dual-remotes", MinMargin: 0.04},
	}
}

// QueryTexts returns every distinct text that must be embedded into the fixture:
// the hand-written queries plus each corpus L0 (used as self-retrieval queries).
func QueryTexts() []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, e := range Corpus() {
		add(e.L0)
	}
	for _, a := range Assertions() {
		add(a.Query)
	}
	return out
}

// Fixture is the committed golden: recorded vectors for the corpus and queries.
type Fixture struct {
	Model      string               `json:"model"`
	Dims       int                  `json:"dims"`
	CorpusVecs map[string][]float64 `json:"corpus_vectors"` // uri -> vector
	QueryVecs  map[string][]float64 `json:"query_vectors"`  // query text -> vector
}

// Load reads a fixture from disk.
func Load(path string) (*Fixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f Fixture
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("decode fixture %s: %w", path, err)
	}
	return &f, nil
}

// Save writes a fixture to disk as indented JSON.
func (f *Fixture) Save(path string) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// ReplayEmbedder returns recorded vectors for the corpus L0s and the query
// texts, satisfying engine.Embedder structurally (no engine import) so the
// golden test drives the real Find() path hermetically.
type ReplayEmbedder struct {
	model string
	dims  int
	byTxt map[string][]float64
}

// ReplayEmbedder builds a replay embedder from the fixture: it maps each query
// text (and each corpus L0, for self-retrieval) to its recorded vector.
func (f *Fixture) ReplayEmbedder() *ReplayEmbedder {
	byTxt := make(map[string][]float64, len(f.QueryVecs)+len(f.CorpusVecs))
	for q, v := range f.QueryVecs {
		byTxt[q] = v
	}
	// Self-retrieval queries are the corpus L0 texts.
	for _, e := range Corpus() {
		if v, ok := f.CorpusVecs[e.URI]; ok {
			byTxt[e.L0] = v
		}
	}
	return &ReplayEmbedder{model: f.Model, dims: f.Dims, byTxt: byTxt}
}

// Embed returns the recorded vector for text, or an error if it was not in the
// fixture (which means the corpus/queries changed without regenerating).
func (r *ReplayEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	v, ok := r.byTxt[text]
	if !ok {
		return nil, fmt.Errorf("goldretrieval: no recorded vector for %q — regenerate with `make retrieval-fixtures`", text)
	}
	return v, nil
}

func (r *ReplayEmbedder) Model() string   { return r.model }
func (r *ReplayEmbedder) Dimensions() int { return r.dims }
