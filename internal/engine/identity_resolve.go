package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// EmbedderOptions carries the inputs ResolveEmbedderByIdentity needs to
// construct any of the three backends. Fields that a given backend doesn't
// need are ignored.
type EmbedderOptions struct {
	OllamaURL string // base URL for the Ollama daemon (e.g. "http://localhost:11434")
}

// ResolveEmbedderByIdentity constructs the embedder that matches a canonical
// vector identity string (the exact format canonicalIdentity produces:
// "<model>:<dims>", e.g. "hashtf:2048", "ollama:nomic-embed-text:768",
// "model2vec:potion-retrieval-32M:512"). It is the inverse of
// canonicalIdentity/EmbedderIdentity: construct-from-identity rather than
// identity-from-embedder.
//
// This is the load-bearing operation behind grandfathering existing corpora:
// when a corpus already declares an identity, serve must construct THAT
// embedder — never substitute a different one, even a "better" one — because
// silently switching vector spaces breaks search (cosine across different
// embedders is meaningless) until an explicit repair. See identity.go's
// ReconcileVectorIdentity for the lock this backstops.
//
// Returns an error if the identity string is malformed, names an unknown
// backend, or the backend can't currently be constructed (e.g. Ollama
// unreachable, or model2vec files absent and the download fails). Callers
// must treat any error as "could not honor the declared identity" and fall
// through to the existing lock behavior — never substitute a different
// embedder on failure.
func ResolveEmbedderByIdentity(identity string, opts EmbedderOptions) (Embedder, error) {
	backend, rest, ok := strings.Cut(identity, ":")
	if !ok {
		return nil, fmt.Errorf("malformed vector identity %q: expected \"<backend>:...\"", identity)
	}

	switch backend {
	case "hashtf":
		dims, err := strconv.Atoi(rest)
		if err != nil {
			return nil, fmt.Errorf("malformed hashtf identity %q: %w", identity, err)
		}
		return NewHashEmbedder(dims)

	case "ollama":
		model, dimsStr, ok := strings.Cut(rest, ":")
		if !ok {
			return nil, fmt.Errorf("malformed ollama identity %q: expected \"ollama:<model>:<dims>\"", identity)
		}
		dims, err := strconv.Atoi(dimsStr)
		if err != nil {
			return nil, fmt.Errorf("malformed ollama identity %q: %w", identity, err)
		}
		url := opts.OllamaURL
		if url == "" {
			url = "http://localhost:11434"
		}
		if !ProbeOllama(url, model) {
			return nil, fmt.Errorf("ollama unreachable at %s (or model %q not available); "+
				"cannot construct declared embedder identity %q", url, model, identity)
		}
		return NewOllamaEmbedder(url, model, dims), nil

	case "model2vec":
		model, dimsStr, ok := strings.Cut(rest, ":")
		if !ok {
			return nil, fmt.Errorf("malformed model2vec identity %q: expected \"model2vec:<model>:<dims>\"", identity)
		}
		if model != Model2VecModelName {
			return nil, fmt.Errorf("unknown model2vec model %q in identity %q (this build only supports %q)",
				model, identity, Model2VecModelName)
		}
		if _, err := strconv.Atoi(dimsStr); err != nil {
			return nil, fmt.Errorf("malformed model2vec identity %q: %w", identity, err)
		}
		emb, err := NewModel2VecEmbedder()
		if err != nil {
			return nil, fmt.Errorf("construct declared embedder identity %q: %w", identity, err)
		}
		return emb, nil

	default:
		return nil, fmt.Errorf("unknown embedder backend %q in vector identity %q", backend, identity)
	}
}
