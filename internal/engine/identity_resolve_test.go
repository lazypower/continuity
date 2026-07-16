package engine

import (
	"strings"
	"testing"
)

func TestResolveEmbedderByIdentity_Hashtf(t *testing.T) {
	emb, err := ResolveEmbedderByIdentity("hashtf:2048", EmbedderOptions{})
	if err != nil {
		t.Fatalf("ResolveEmbedderByIdentity(hashtf:2048): %v", err)
	}
	if got := EmbedderIdentity(emb); got != "hashtf:2048" {
		t.Errorf("identity round-trip: got %q, want %q", got, "hashtf:2048")
	}
}

func TestResolveEmbedderByIdentity_HashtfCustomDims(t *testing.T) {
	emb, err := ResolveEmbedderByIdentity("hashtf:1024", EmbedderOptions{})
	if err != nil {
		t.Fatalf("ResolveEmbedderByIdentity(hashtf:1024): %v", err)
	}
	if emb.Dimensions() != 1024 {
		t.Errorf("Dimensions() = %d, want 1024", emb.Dimensions())
	}
}

func TestResolveEmbedderByIdentity_OllamaUnreachable(t *testing.T) {
	// Port 1 is reserved and will refuse the connection immediately, so this
	// stays fast and hermetic (no real Ollama daemon required).
	_, err := ResolveEmbedderByIdentity("ollama:nomic-embed-text:768", EmbedderOptions{OllamaURL: "http://127.0.0.1:1"})
	if err == nil {
		t.Fatal("expected error when Ollama is unreachable, got nil")
	}
}

func TestResolveEmbedderByIdentity_Model2Vec(t *testing.T) {
	dir, err := DefaultModel2VecDir()
	if err != nil {
		t.Fatalf("resolve default model2vec dir: %v", err)
	}
	if !fileExists(dir + "/model.safetensors") {
		t.Skip("model2vec model files not present locally; skipping (see model2vec_embedder_test.go for the hermeticity rationale)")
	}

	emb, err := ResolveEmbedderByIdentity("model2vec:potion-retrieval-32M:512", EmbedderOptions{})
	if err != nil {
		t.Fatalf("ResolveEmbedderByIdentity(model2vec:potion-retrieval-32M:512): %v", err)
	}
	if got := EmbedderIdentity(emb); got != "model2vec:potion-retrieval-32M:512" {
		t.Errorf("identity round-trip: got %q, want %q", got, "model2vec:potion-retrieval-32M:512")
	}
}

func TestResolveEmbedderByIdentity_UnknownModel2VecModel(t *testing.T) {
	_, err := ResolveEmbedderByIdentity("model2vec:some-other-model:256", EmbedderOptions{})
	if err == nil {
		t.Fatal("expected error for an unsupported model2vec model name, got nil")
	}
}

func TestResolveEmbedderByIdentity_UnknownBackend(t *testing.T) {
	_, err := ResolveEmbedderByIdentity("openai:text-embedding-3:1536", EmbedderOptions{})
	if err == nil {
		t.Fatal("expected error for an unknown backend, got nil")
	}
	if !strings.Contains(err.Error(), "unknown embedder backend") {
		t.Errorf("error = %q, want it to mention 'unknown embedder backend'", err.Error())
	}
}

func TestResolveEmbedderByIdentity_Malformed(t *testing.T) {
	cases := []string{
		"",
		"hashtf",                         // no dims
		"hashtf:notanum",                 // bad dims
		"ollama:nomic-embed-text",        // no dims
		"model2vec:potion-retrieval-32M", // no dims
	}
	for _, id := range cases {
		if _, err := ResolveEmbedderByIdentity(id, EmbedderOptions{}); err == nil {
			t.Errorf("ResolveEmbedderByIdentity(%q): expected error, got nil", id)
		}
	}
}

// TestResolveEmbedderByIdentity_IsInverseOfCanonicalIdentity locks in that
// resolving-then-re-identifying is a no-op for every backend this function
// supports — the exact "inverse of canonicalIdentity" contract the task
// requires.
func TestResolveEmbedderByIdentity_IsInverseOfCanonicalIdentity(t *testing.T) {
	identities := []string{"hashtf:2048", "hashtf:512"}
	dir, err := DefaultModel2VecDir()
	if err == nil && fileExists(dir+"/model.safetensors") {
		identities = append(identities, "model2vec:potion-retrieval-32M:512")
	}
	for _, id := range identities {
		emb, err := ResolveEmbedderByIdentity(id, EmbedderOptions{})
		if err != nil {
			t.Errorf("ResolveEmbedderByIdentity(%q): %v", id, err)
			continue
		}
		if got := EmbedderIdentity(emb); got != id {
			t.Errorf("round-trip mismatch: ResolveEmbedderByIdentity(%q) -> EmbedderIdentity() = %q", id, got)
		}
	}
}
