package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
)

// model2vecFilesPresent reports whether the potion-retrieval-32M model files
// are present at the default location, so tests that would otherwise trigger
// a real network download can skip (never fail) when they're absent — the
// same hermeticity contract internal/engine's model2vec tests use.
func model2vecFilesPresent(t *testing.T) bool {
	t.Helper()
	dir, err := engine.DefaultModel2VecDir()
	if err != nil {
		return false
	}
	for _, f := range []string{"model.safetensors", "tokenizer.json"} {
		info, err := os.Stat(dir + "/" + f)
		if err != nil || info.Size() == 0 {
			return false
		}
	}
	return true
}

// TestSelectEmbedder_Precedence is the REQUIRED table-driven test covering
// every (override, declared-identity, availability) combination the task
// specifies. Its centerpiece is the no-downgrade guarantee: a corpus declared
// nomic gets Ollama, never model2vec, when nothing overrides it.
func TestSelectEmbedder_Precedence(t *testing.T) {
	haveModel2Vec := model2vecFilesPresent(t)

	// Use an address nothing listens on so "Ollama available" is always false
	// in this hermetic test process, and a bogus model name for the "declared
	// but no override" fresh-corpus-vs-declared distinction.
	unreachableOllama := "http://127.0.0.1:1"

	cases := []struct {
		name             string
		envOverride      string
		configBackend    string
		declaredIdentity string
		wantModelPrefix  string // prefix-match on emb.Model(), "" means expect nil
		wantNil          bool
	}{
		// --- Tier 1: explicit override always wins ---
		{
			name:            "env override hashtf wins over everything",
			envOverride:     "tfidf",
			configBackend:   "model2vec",
			wantModelPrefix: "hashtf",
		},
		{
			name:            "env override ollama wins even though unreachable (constructs anyway; reconcile handles reachability elsewhere)",
			envOverride:     "ollama",
			wantModelPrefix: "ollama:",
		},
		{
			name:          "env override none wins",
			envOverride:   "none",
			configBackend: "model2vec",
			wantNil:       true,
		},
		{
			name:             "env override mismatches declared identity — still constructs override (reconcile locks downstream)",
			envOverride:      "tfidf",
			declaredIdentity: "ollama:nomic-embed-text:768",
			wantModelPrefix:  "hashtf",
		},
		{
			name:            "config backend wins when no env override",
			envOverride:     "",
			configBackend:   "tfidf",
			wantModelPrefix: "hashtf",
		},
		{
			name:            "config backend hashtf spelling",
			configBackend:   "hashtf",
			wantModelPrefix: "hashtf",
		},

		// --- Tier 2: declared corpus identity is grandfathered ---
		{
			name:             "declared hashtf + no override -> hashtf (never model2vec)",
			declaredIdentity: "hashtf:2048",
			wantModelPrefix:  "hashtf",
		},
		{
			name:             "declared ollama + no override -> ollama attempted (unreachable here -> nil, NOT model2vec)",
			declaredIdentity: "ollama:nomic-embed-text:768",
			wantNil:          true, // Ollama unreachable in this test process; must NOT fall back to model2vec
		},

		// --- Tier 3: fresh corpus, no override -> model2vec (only if files present) ---
		// handled separately below since it depends on local model file presence
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			emb, logLine := selectEmbedder(embedderSelectionInput{
				EnvOverride:      tc.envOverride,
				ConfigBackend:    tc.configBackend,
				DeclaredIdentity: tc.declaredIdentity,
				OllamaURL:        unreachableOllama,
				EmbeddingModel:   "nomic-embed-text",
			})
			if tc.wantNil {
				if emb != nil {
					t.Errorf("expected nil embedder, got %s (log: %s)", emb.Model(), logLine)
				}
				return
			}
			if emb == nil {
				t.Fatalf("expected non-nil embedder, got nil (log: %s)", logLine)
			}
			if !strings.HasPrefix(emb.Model(), tc.wantModelPrefix) {
				t.Errorf("emb.Model() = %q, want prefix %q (log: %s)", emb.Model(), tc.wantModelPrefix, logLine)
			}
		})
	}

	// The no-downgrade guarantee is the #1 priority: a nomic-declared corpus
	// must NEVER receive model2vec, model2vec must never be silently
	// substituted. Assert this explicitly and separately from the table above.
	t.Run("declared nomic never yields model2vec even if model2vec files are present locally", func(t *testing.T) {
		emb, logLine := selectEmbedder(embedderSelectionInput{
			DeclaredIdentity: "ollama:nomic-embed-text:768",
			OllamaURL:        unreachableOllama,
			EmbeddingModel:   "nomic-embed-text",
		})
		if emb != nil && strings.HasPrefix(emb.Model(), "model2vec") {
			t.Fatalf("declared nomic identity silently downgraded to model2vec: %s (log: %s)", emb.Model(), logLine)
		}
	})

	if !haveModel2Vec {
		t.Skip("model2vec model files not present locally; skipping fresh-corpus default assertions (would require network)")
	}

	t.Run("fresh corpus, no override -> model2vec", func(t *testing.T) {
		emb, logLine := selectEmbedder(embedderSelectionInput{
			OllamaURL:      unreachableOllama,
			EmbeddingModel: "nomic-embed-text",
		})
		if emb == nil {
			t.Fatalf("expected model2vec embedder for a fresh corpus, got nil (log: %s)", logLine)
		}
		if !strings.HasPrefix(emb.Model(), "model2vec") {
			t.Errorf("fresh corpus should default to model2vec; got %q (log: %s)", emb.Model(), logLine)
		}
	})

	t.Run("declared model2vec + no override -> model2vec", func(t *testing.T) {
		emb, logLine := selectEmbedder(embedderSelectionInput{
			DeclaredIdentity: "model2vec:potion-retrieval-32M:512",
			OllamaURL:        unreachableOllama,
			EmbeddingModel:   "nomic-embed-text",
		})
		if emb == nil {
			t.Fatalf("expected model2vec embedder for a declared model2vec corpus, got nil (log: %s)", logLine)
		}
		if !strings.HasPrefix(emb.Model(), "model2vec") {
			t.Errorf("emb.Model() = %q, want model2vec prefix (log: %s)", emb.Model(), logLine)
		}
	})

	t.Run("config backend model2vec forced", func(t *testing.T) {
		emb, logLine := selectEmbedder(embedderSelectionInput{
			ConfigBackend:  "model2vec",
			OllamaURL:      unreachableOllama,
			EmbeddingModel: "nomic-embed-text",
		})
		if emb == nil {
			t.Fatalf("expected model2vec embedder, got nil (log: %s)", logLine)
		}
		if !strings.HasPrefix(emb.Model(), "model2vec") {
			t.Errorf("emb.Model() = %q, want model2vec prefix (log: %s)", emb.Model(), logLine)
		}
	})
}

func TestSelectEmbedder_EnvWinsOverConfig(t *testing.T) {
	emb, logLine := selectEmbedder(embedderSelectionInput{
		EnvOverride:   "tfidf",
		ConfigBackend: "ollama",
		OllamaURL:     "http://127.0.0.1:1",
	})
	if emb == nil || emb.Model() != "hashtf" {
		t.Fatalf("env override must win over config backend; got %v (log: %s)", emb, logLine)
	}
}

func TestSelectEmbedder_UnrecognizedEnvFallsBackButWarns(t *testing.T) {
	emb, logLine := selectEmbedder(embedderSelectionInput{
		EnvOverride:      "openai",
		DeclaredIdentity: "hashtf:2048",
	})
	if emb == nil || emb.Model() != "hashtf" {
		t.Fatalf("unrecognized env override should fall back to declared identity (hashtf); got %v", emb)
	}
	if !strings.Contains(logLine, "unrecognized CONTINUITY_EMBEDDER") {
		t.Errorf("expected a warning about the unrecognized value in the log line, got: %s", logLine)
	}
}

func TestSelectEmbedder_UnrecognizedConfigFallsBackButWarns(t *testing.T) {
	emb, logLine := selectEmbedder(embedderSelectionInput{
		ConfigBackend:    "openai",
		DeclaredIdentity: "hashtf:2048",
	})
	if emb == nil || emb.Model() != "hashtf" {
		t.Fatalf("unrecognized config backend should fall back to declared identity (hashtf); got %v", emb)
	}
	if !strings.Contains(logLine, "unrecognized config [embedder].backend") {
		t.Errorf("expected a warning about the unrecognized value in the log line, got: %s", logLine)
	}
}

func TestSelectEmbedder_DeclaredHashtfCustomDims(t *testing.T) {
	emb, logLine := selectEmbedder(embedderSelectionInput{
		DeclaredIdentity: "hashtf:1024",
	})
	if emb == nil {
		t.Fatalf("expected embedder, got nil (log: %s)", logLine)
	}
	if emb.Dimensions() != 1024 {
		t.Errorf("Dimensions() = %d, want 1024 (matching declared identity, not the package default)", emb.Dimensions())
	}
}
