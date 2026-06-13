package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lazypower/continuity/internal/config"
	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/server"
	"github.com/lazypower/continuity/internal/store"
	"github.com/spf13/cobra"
)

// Server-side environment variables, read at serve start. These exist to make
// hermetic subprocess tests possible (and pave the way for TFIDF CI coverage),
// not as the production configuration surface — Phase 1 config.toml loading
// remains the path for normal use.
const (
	envServeDB       = "CONTINUITY_DB"       // overrides Database.Path
	envServePort     = "CONTINUITY_PORT"     // overrides Server.Port (int)
	envServeBind     = "CONTINUITY_BIND"     // overrides Server.Bind
	envServeEmbedder = "CONTINUITY_EMBEDDER" // "tfidf" | "ollama" | "none" | "" (auto)
)

// tfidfBestEffortNotice is surfaced once at startup whenever TFIDF is the active
// embedder (forced or fallback). TFIDF is best-effort by construction — the
// corpus IS the model — so operators should know the tradeoff they're running
// with, plus a one-line pointer to the upgrade path. The README's "Embedding
// backends" section spells out the two shipped paths (Ollama / TFIDF). Issue #22.
const tfidfBestEffortNotice = "  ! tfidf: retraction-dedup recall is best-effort; install Ollama (nomic-embed-text) for stronger guarantees — see README \"Embedding backends\""

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the HTTP API server",
	RunE:  runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	cfg := config.Default()

	// Check for ANTHROPIC_API_KEY env override
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		cfg.LLM.Provider = "anthropic"
		cfg.LLM.AnthropicKey = key
	}

	if err := applyServeEnvOverrides(&cfg); err != nil {
		return err
	}

	// Resolve database path
	dbPath := cfg.Database.Path
	if dbPath == "" {
		var err error
		dbPath, err = store.DefaultDBPath()
		if err != nil {
			return fmt.Errorf("resolve db path: %w", err)
		}
	}

	db, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Tick the migration-snapshot retention counter. Each `continuity serve`
	// start counts as one boot against any retained safety snapshots; after
	// SnapshotRetentionBoots successful boots, snapshots auto-delete.
	// Deliberately NOT in store.Open so CLI subcommands that inspect or
	// prune snapshots don't advance the counter.
	if err := db.TickSnapshotRetention(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: snapshot retention tick failed: %v\n", err)
	}
	if snaps, _ := db.ListMigrationSnapshots(); len(snaps) > 0 {
		for _, s := range snaps {
			fmt.Fprintf(os.Stderr,
				"migration safety snapshot retained: %s (auto-deletes after %d more successful boots)\n",
				s.Path, store.SnapshotRetentionBoots-s.BootsSince,
			)
		}
	}

	// Create LLM client and engine
	var eng *engine.Engine
	llmClient, err := llm.NewClient(cfg.LLM)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: LLM not configured (%v), extraction disabled\n", err)
	} else {
		eng = engine.New(db, llmClient)
		eng.StartDecayTimer()
		defer eng.Stop()
		fmt.Fprintf(os.Stderr, "  llm: %s (%s)\n", cfg.LLM.Provider, cfg.LLM.Model)
	}

	// Detect and configure embedder
	{
		ollamaURL := cfg.LLM.OllamaURL
		if ollamaURL == "" {
			ollamaURL = "http://localhost:11434"
		}
		embeddingModel := cfg.LLM.EmbeddingModel
		if embeddingModel == "" {
			embeddingModel = "nomic-embed-text"
		}

		choice := resolveEmbedderChoice(ollamaURL, embeddingModel)
		switch choice {
		case "ollama":
			emb := engine.NewOllamaEmbedder(ollamaURL, embeddingModel, 768)
			if eng != nil {
				eng.SetEmbedder(emb)
			}
			fmt.Fprintf(os.Stderr, "  embedder: ollama (%s)\n", embeddingModel)
		case "tfidf":
			emb, tfidfErr := engine.NewTFIDFEmbedder(db, 512)
			if tfidfErr != nil {
				fmt.Fprintf(os.Stderr, "warning: tfidf embedder init failed: %v\n", tfidfErr)
			} else {
				if eng != nil {
					eng.SetEmbedder(emb)
				}
				fmt.Fprintf(os.Stderr, "  embedder: tfidf (forced)\n")
				fmt.Fprintln(os.Stderr, tfidfBestEffortNotice)
			}
		case "none":
			fmt.Fprintln(os.Stderr, "  embedder: none (forced; dedup-against-retracted gate inactive)")
		default:
			// auto: probe Ollama, fall back to TFIDF
			if engine.ProbeOllama(ollamaURL, embeddingModel) {
				emb := engine.NewOllamaEmbedder(ollamaURL, embeddingModel, 768)
				if eng != nil {
					eng.SetEmbedder(emb)
				}
				fmt.Fprintf(os.Stderr, "  embedder: ollama (%s)\n", embeddingModel)
			} else {
				emb, tfidfErr := engine.NewTFIDFEmbedder(db, 512)
				if tfidfErr != nil {
					fmt.Fprintf(os.Stderr, "warning: tfidf embedder init failed: %v\n", tfidfErr)
				} else {
					if eng != nil {
						eng.SetEmbedder(emb)
					}
					fmt.Fprintf(os.Stderr, "  embedder: tfidf (fallback)\n")
					fmt.Fprintln(os.Stderr, tfidfBestEffortNotice)
				}
			}
		}

		// Embed any nodes missing vectors
		if eng != nil && eng.Embedder != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				if n, err := eng.EmbedMissing(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "embed missing: %v\n", err)
				} else if n > 0 {
					fmt.Fprintf(os.Stderr, "  embedded %d missing nodes\n", n)
				}
			}()
		}
	}

	srv := server.New(db, eng, VersionString())
	addr := cfg.ListenAddr()

	httpServer := &http.Server{
		Addr:           addr,
		Handler:        srv,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	// Graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Fprintf(os.Stderr, "continuity serving on %s\n", addr)
		fmt.Fprintf(os.Stderr, "  db: %s\n", dbPath)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}()

	<-done
	fmt.Fprintln(os.Stderr, "\nshutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return httpServer.Shutdown(ctx)
}

// applyServeEnvOverrides mutates cfg with values from CONTINUITY_* env vars.
// Invalid values (e.g. a non-integer port) are returned as errors so the
// server fails fast rather than silently ignoring them.
func applyServeEnvOverrides(cfg *config.Config) error {
	if v := strings.TrimSpace(os.Getenv(envServeDB)); v != "" {
		cfg.Database.Path = v
	}
	if v := strings.TrimSpace(os.Getenv(envServeBind)); v != "" {
		cfg.Server.Bind = v
	}
	if v := strings.TrimSpace(os.Getenv(envServePort)); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port < 0 || port > 65535 {
			return fmt.Errorf("%s=%q: must be an integer in [0, 65535]", envServePort, v)
		}
		cfg.Server.Port = port
	}
	return nil
}

// resolveEmbedderChoice translates the CONTINUITY_EMBEDDER env var into one of
// {"ollama", "tfidf", "none", "auto"}. Unknown values fall back to "auto" with
// a warning so a typo never silently bypasses the embedder. The ollamaURL and
// embeddingModel arguments are unused today; they exist so future validation
// (e.g. require Ollama reachable when forced) can land without a signature
// change.
func resolveEmbedderChoice(ollamaURL, embeddingModel string) string {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(envServeEmbedder)))
	switch v {
	case "", "auto":
		return "auto"
	case "ollama", "tfidf", "none":
		return v
	default:
		fmt.Fprintf(os.Stderr, "warning: unrecognized %s=%q; falling back to auto\n", envServeEmbedder, v)
		return "auto"
	}
}
