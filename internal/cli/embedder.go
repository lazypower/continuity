package cli

import (
	"fmt"

	"github.com/lazypower/continuity/internal/config"
	"github.com/lazypower/continuity/internal/engine"
	"github.com/spf13/cobra"
)

var embedderCmd = &cobra.Command{
	Use:   "embedder",
	Short: "Inspect and change the active embedding backend",
	Long: `embedder reports which embedding backend is active and lets you deliberately
migrate the corpus to a different one.

"auto" (the default) is identity-aware: it matches whatever backend the corpus
already declares (grandfathering existing installs), and only defaults to
model2vec for a brand-new, empty corpus. Changing config.toml's
[embedder].backend directly (or CONTINUITY_EMBEDDER) changes what the NEXT
serve boots with, but does not touch stored vectors — a mismatch against the
corpus's declared identity locks search until repaired.

The ONLY command that migrates an existing corpus's stored vectors is
"embedder use": it writes the config, re-embeds every node (snapshot-first),
and rebinds the corpus's declared identity — in one explicit step.`,
}

var embedderStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show the active embedder, the corpus's declared identity, and match/lock state",
	Long: `status is a focused subset of "continuity doctor": it answers the one question
"what embedder am I actually using, and does it match my corpus?" without the
full vector-health report. Run "continuity doctor" for the complete picture
(missing vectors, mixed dimensions, retrieval smoke test, etc).`,
	RunE: runEmbedderStatus,
}

var embedderUseCmd = &cobra.Command{
	Use:   "use <backend>",
	Short: "Migrate the corpus to a different embedding backend (model2vec | ollama | hashtf)",
	Long: `use is the ONLY command that migrates an existing corpus to a new embedding
backend. It:

  1. Validates <backend> and makes sure it can actually be constructed
     (Ollama must be reachable; model2vec's files are downloaded if absent).
  2. Writes [embedder].backend = "<backend>" to config.toml, so future
     "continuity serve" boots select it by default (still overridable by
     CONTINUITY_EMBEDDER).
  3. Snapshots the database, then re-embeds every node to the new backend and
     rebinds the corpus's declared vector identity — reusing the exact
     snapshot-first repair path "continuity doctor --repair-vectors --apply"
     uses.
  4. Tells you to run "continuity restart" to clear the identity lock on the
     running server.

This is the explicit, deliberate migration path. Nothing else in continuity
silently re-embeds or switches vector spaces for you.`,
	Args: cobra.ExactArgs(1),
	RunE: runEmbedderUse,
}

func init() {
	embedderCmd.AddCommand(embedderStatusCmd)
	embedderCmd.AddCommand(embedderUseCmd)
}

// runEmbedderStatus reuses doctor's identity-fetching helpers (resolveActiveEmbedder,
// db.VectorIdentity, fetchServerIdentity) rather than duplicating the logic —
// it is deliberately a narrower read of the same facts doctor's full report
// computes.
func runEmbedderStatus(cmd *cobra.Command, args []string) error {
	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	emb, err := resolveActiveEmbedder(db, cfg)
	if err != nil {
		return fmt.Errorf("resolve embedder: %w", err)
	}
	declared, _, err := db.VectorIdentity()
	if err != nil {
		return fmt.Errorf("read corpus vector identity: %w", err)
	}
	srv := fetchServerIdentity()

	activeID := "none"
	if emb != nil {
		activeID = engine.EmbedderIdentity(emb)
	}
	dash := func(s string) string {
		if s == "" {
			return "(none — fresh or pre-identity corpus)"
		}
		return s
	}

	fmt.Println("continuity embedder status")
	fmt.Println()
	fmt.Printf("  config [embedder].backend: %s\n", displayBackend(cfg.Embedder.Backend))
	fmt.Printf("  resolved active embedder:  %s\n", activeID)
	fmt.Printf("  corpus declared identity:  %s\n", dash(declared))

	switch {
	case emb == nil:
		fmt.Println("  match:                     n/a (no embedder active)")
	case declared == "":
		fmt.Println("  match:                     n/a (corpus has no declared identity yet — will bind on next serve boot)")
	case activeID == declared:
		fmt.Println("  match:                     yes")
	default:
		fmt.Println("  match:                     NO — mismatch. Search will fail closed (locked) until repaired.")
		fmt.Println("    Run `continuity embedder use <backend>` to migrate the corpus to the active embedder,")
		fmt.Println("    or `continuity doctor --repair-vectors --apply` for the same repair without changing config.")
	}

	if srv.Reachable {
		locked := "no"
		if srv.Locked {
			locked = "YES — search is currently failing closed"
		}
		fmt.Printf("  running server embedder:   %s\n", dash(srv.ActiveEmbedder))
		fmt.Printf("  running server locked:     %s\n", locked)
	} else {
		fmt.Println("  running server:            (not reachable)")
	}

	return nil
}

// displayBackend renders the config value for humans; "" (unset, pre-this-feature
// config.toml) reads the same as the documented default.
func displayBackend(backend string) string {
	if backend == "" {
		return "auto (default)"
	}
	return backend
}

// runEmbedderUse is the sole migration verb: validate, persist config, then
// snapshot-first re-embed by calling runDoctorRepair directly (never
// reimplementing its internals).
func runEmbedderUse(cmd *cobra.Command, args []string) error {
	backend := normalizeBackend(args[0])
	switch backend {
	case "ollama", "hashtf", "model2vec":
		// valid
	default:
		return fmt.Errorf("unknown backend %q: must be one of model2vec, ollama, hashtf", args[0])
	}

	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ollamaURL := cfg.LLM.OllamaURL
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}
	embeddingModel := cfg.LLM.EmbeddingModel
	if embeddingModel == "" {
		embeddingModel = "nomic-embed-text"
	}

	// Construct the target embedder FIRST, before writing anything — refuse
	// with a clear message if the backend isn't actually usable right now.
	var target engine.Embedder
	switch backend {
	case "ollama":
		if !engine.ProbeOllama(ollamaURL, embeddingModel) {
			return fmt.Errorf("ollama is not reachable at %s (model %q) — start Ollama and pull the model, then retry",
				ollamaURL, embeddingModel)
		}
		target = engine.NewOllamaEmbedder(ollamaURL, embeddingModel, 768)
	case "hashtf":
		target, err = engine.NewHashEmbedder(0)
		if err != nil {
			return fmt.Errorf("construct hashtf embedder: %w", err)
		}
	case "model2vec":
		target, err = engine.NewModel2VecEmbedder()
		if err != nil {
			return fmt.Errorf("model2vec files could not be ensured (offline?): %w", err)
		}
	}

	// Persist the choice so the next `serve` boot selects it without needing
	// this command to have run in the same process.
	path, err := config.Path()
	if err != nil {
		return fmt.Errorf("resolve config path: %w", err)
	}
	if err := config.SetEmbedderBackend(path, backend); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	fmt.Printf("Wrote [embedder].backend = %q to %s\n", backend, path)

	// Snapshot-first re-embed, reusing doctor's exact repair path — apply=true
	// because "embedder use" IS the explicit migration step; there is no
	// separate dry-run half for this command (status/doctor already cover
	// "what would happen").
	if err := runDoctorRepair(db, target, true, fetchServerIdentity()); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Run `continuity restart` to have the running server pick up the new embedder.")
	return nil
}
