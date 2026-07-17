package cli

import (
	"fmt"

	"github.com/lazypower/continuity/internal/engine"
)

// embedderSelectionInput is every input selectEmbedder needs, gathered up
// front so the precedence logic itself is a pure function — no env reads, no
// DB calls, no network probes buried inside branches — and therefore fully
// table-testable.
type embedderSelectionInput struct {
	// EnvOverride is CONTINUITY_EMBEDDER, lowercased/trimmed. "" or "auto" means
	// "no override" (falls through to ConfigBackend). One of "ollama", "tfidf",
	// "model2vec", "none" is a deliberate, explicit choice — the escape hatch —
	// and wins over everything else, including a mismatching declared identity
	// (which then locks via ReconcileVectorIdentity downstream; that safety net
	// is untouched by this function).
	EnvOverride string

	// ConfigBackend is config.toml's [embedder].backend, lowercased/trimmed.
	// "" or "auto" means "no override" (falls through to declared-identity /
	// fresh-corpus default). One of "ollama", "hashtf", "model2vec" is a
	// deliberate, explicit choice — the persistent form of EnvOverride, and it
	// wins over the declared identity the same way EnvOverride does. Only
	// `continuity embedder use` is expected to ever set this to a non-auto
	// value; a hand-edited config.toml is honored identically.
	ConfigBackend string

	// DeclaredIdentity is the corpus's already-bound vector identity
	// (DB.VectorIdentity()), or "" for a corpus with no declared identity yet
	// (fresh, or pre-vector-identity). When non-empty and neither override
	// fires, selectEmbedder constructs the MATCHING embedder — this is what
	// grandfathers existing users onto whatever they already have (nomic stays
	// nomic, hashtf stays hashtf) instead of silently defaulting to model2vec.
	DeclaredIdentity string

	OllamaURL      string // base URL for Ollama, used for both override and declared-identity construction
	EmbeddingModel string // Ollama embedding model name, e.g. "nomic-embed-text"
}

// normalizeBackend maps the small set of accepted spellings (env's legacy
// "tfidf" and config's "hashtf" both name the same backend; "" and "auto" both
// mean "no override") to one canonical token: "", "auto", "ollama", "hashtf",
// "model2vec", "none". Unknown input passes through unchanged so callers can
// still detect and warn on it.
func normalizeBackend(v string) string {
	switch v {
	case "", "auto":
		return "auto"
	case "tfidf", "hashtf":
		return "hashtf"
	case "ollama", "model2vec", "none":
		return v
	default:
		return v // unrecognized; caller decides how to handle
	}
}

// selectEmbedder is THE core rule (see the task-level doc comment on serve.go
// for the full precedence writeup). In order:
//
//  1. Explicit override — EnvOverride, else ConfigBackend, if either names a
//     concrete backend (not "auto"). Env wins over config. This is a
//     deliberate user choice: construct exactly that backend, even if it will
//     go on to mismatch the corpus's declared identity (ReconcileVectorIdentity
//     locks on that downstream — this function does not need to know about
//     the lock, only to never silently substitute a different embedder).
//
//  2. Declared corpus identity — if DeclaredIdentity is non-empty, construct
//     the embedder that MATCHES it via engine.ResolveEmbedderByIdentity. This
//     grandfathers existing corpora: a corpus declared
//     "ollama:nomic-embed-text:768" gets Ollama, NEVER model2vec, even though
//     model2vec is the new default for fresh corpora. If construction fails
//     (Ollama unreachable, model2vec download fails, etc.), this function
//     returns a nil embedder rather than substituting anything else — the
//     caller (runServe) then relies on the existing lock/EmbedMissing-skip
//     behavior (a nil embedder deletes vectors and leaves nodes Pending,
//     search fails closed via the "no active embedder" path). This is the
//     safe choice: never silently mismatch the corpus.
//
//  3. Fresh corpus (DeclaredIdentity == "", no override) — model2vec, the new
//     default. If the model files can't be ensured (offline first run), this
//     function falls back to hashtf for THIS session (documented tradeoff:
//     `continuity embedder use model2vec` upgrades later) rather than leaving
//     the corpus with no embedder at all.
//
// Returns the constructed embedder (nil only if truly nothing could be
// constructed) and a single human-readable line for the existing "  embedder:
// ..." startup log.
func selectEmbedder(in embedderSelectionInput) (engine.Embedder, string) {
	env := normalizeBackend(in.EnvOverride)
	cfgBackend := normalizeBackend(in.ConfigBackend)

	// --- Tier 1: explicit override (env wins over config) ---
	// An unrecognized value must never silently bypass the embedder or fall
	// through to a different tier without a trace — warn (in the returned log
	// line) and treat as "auto" (same contract resolveEmbedderChoice guaranteed
	// previously).
	if !isKnownBackend(env) {
		warning := fmt.Sprintf("warning: unrecognized CONTINUITY_EMBEDDER=%q; falling back to auto", in.EnvOverride)
		emb, line := selectEmbedder(embedderSelectionInput{
			ConfigBackend:    in.ConfigBackend,
			DeclaredIdentity: in.DeclaredIdentity,
			OllamaURL:        in.OllamaURL,
			EmbeddingModel:   in.EmbeddingModel,
		})
		return emb, warning + "\n" + line
	}
	if env != "auto" {
		return constructOverride(env, in, "CONTINUITY_EMBEDDER")
	}

	if !isKnownBackend(cfgBackend) {
		warning := fmt.Sprintf("warning: unrecognized config [embedder].backend=%q; falling back to auto", in.ConfigBackend)
		emb, line := selectEmbedder(embedderSelectionInput{
			DeclaredIdentity: in.DeclaredIdentity,
			OllamaURL:        in.OllamaURL,
			EmbeddingModel:   in.EmbeddingModel,
		})
		return emb, warning + "\n" + line
	}
	if cfgBackend != "auto" {
		return constructOverride(cfgBackend, in, "config [embedder].backend")
	}

	// --- Tier 2: declared corpus identity (grandfather existing users) ---
	if in.DeclaredIdentity != "" {
		emb, err := engine.ResolveEmbedderByIdentity(in.DeclaredIdentity, engine.EmbedderOptions{OllamaURL: in.OllamaURL})
		if err != nil {
			return nil, fmt.Sprintf(
				"  embedder: none — corpus is declared %q but it could not be constructed (%v); "+
					"search will fail closed until this is resolved (see `continuity doctor`)",
				in.DeclaredIdentity, err)
		}
		return emb, fmt.Sprintf("  embedder: %s (matches corpus identity %s)", describeEmbedder(emb), in.DeclaredIdentity)
	}

	// --- Tier 3: fresh corpus, no override — model2vec is the new default ---
	emb, err := engine.NewModel2VecEmbedder()
	if err == nil {
		return emb, fmt.Sprintf("  embedder: %s (semantic, static; new default for a fresh corpus)", describeEmbedder(emb))
	}

	// Fresh-install offline fallback: model2vec download failed. Use hashtf for
	// this session; do not leave the corpus with no embedder at all. This is a
	// documented tradeoff (lexical-only until `continuity embedder use
	// model2vec` succeeds later), not silently swallowed.
	hashEmb, hashErr := engine.NewHashEmbedder(0)
	if hashErr != nil {
		// NewHashEmbedder's error return is always nil today (see its doc
		// comment) — this branch exists only to satisfy the (Embedder, error)
		// shape and never actually fires.
		return nil, fmt.Sprintf("  embedder: none — model2vec unavailable (%v) and hashtf init failed (%v)", err, hashErr)
	}
	return hashEmb, fmt.Sprintf(
		"  embedder: hashtf (hashed lexical, fallback) — model2vec download failed (%v); "+
			"this fresh corpus will bind to the lexical floor for now. Run `continuity embedder use model2vec` "+
			"once network is available to upgrade.", err)
}

// constructOverride builds the embedder named by an explicit override
// (backend already normalized to one of "ollama"/"hashtf"/"model2vec"/"none").
// source is used only in log text ("CONTINUITY_EMBEDDER" or "config
// [embedder].backend") so operators can tell which knob fired.
func constructOverride(backend string, in embedderSelectionInput, source string) (engine.Embedder, string) {
	switch backend {
	case "ollama":
		emb := engine.NewOllamaEmbedder(in.OllamaURL, in.EmbeddingModel, 768)
		return emb, fmt.Sprintf("  embedder: ollama (%s, forced via %s)", in.EmbeddingModel, source)
	case "hashtf":
		emb, err := engine.NewHashEmbedder(0)
		if err != nil {
			return nil, fmt.Sprintf("  embedder: none — tfidf/hashtf init failed (%v)", err)
		}
		return emb, fmt.Sprintf("  embedder: tfidf (hashed lexical, forced via %s)", source)
	case "model2vec":
		emb, err := engine.NewModel2VecEmbedder()
		if err != nil {
			return nil, fmt.Sprintf("  embedder: none — model2vec init failed (%v), forced via %s", err, source)
		}
		return emb, fmt.Sprintf("  embedder: %s (semantic, static, forced via %s)", describeEmbedder(emb), source)
	case "none":
		return nil, fmt.Sprintf("  embedder: none (forced via %s; dedup-against-retracted gate inactive)", source)
	default:
		// Unreachable: callers only invoke constructOverride with a value that
		// isKnownBackend has already confirmed is one of the cases above (and
		// isn't "auto", which is handled before constructOverride is called).
		panic(fmt.Sprintf("constructOverride: unreachable backend %q", backend))
	}
}

// isKnownBackend reports whether v is a normalized backend token selectEmbedder
// knows how to handle: "auto" (no override) or one of the three concrete
// backends. Anything else is an unrecognized override value that must be
// warned about rather than silently mis-selecting.
func isKnownBackend(v string) bool {
	switch v {
	case "auto", "ollama", "hashtf", "model2vec", "none":
		return true
	default:
		return false
	}
}

// describeEmbedder returns the embedder's Model() string, used in log lines.
func describeEmbedder(emb engine.Embedder) string {
	if emb == nil {
		return "none"
	}
	return emb.Model()
}
