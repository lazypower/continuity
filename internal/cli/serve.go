package cli

import (
	"context"
	"fmt"
	"net"
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

// tfidfLexicalNotice is surfaced once at startup whenever the hashed lexical
// fallback is the active embedder (forced or fallback). The fallback is a
// fixed-dimension feature-hashed embedder: stable and reliable for the
// retraction/dedup gates, but LEXICAL (keyword overlap), not semantic — so
// operators should know to install Ollama if they need semantic recall. The
// README's "Embedding backends" section spells out the two shipped paths.
const tfidfLexicalNotice = "  ! tfidf: hashed lexical fallback (keyword overlap, not semantic); install Ollama (nomic-embed-text) for semantic recall — see README \"Embedding backends\""

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
		if bin := llm.ProviderBinaryUnresolved(cfg.LLM); bin != "" {
			fmt.Fprintf(os.Stderr,
				"warning: LLM provider binary %q is not on this process's PATH — extraction will fail.\n"+
					"  If running as a service, re-run `continuity install-service` to bake in a usable PATH.\n",
				bin)
		}
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
			emb, tfidfErr := engine.NewHashEmbedder(0)
			if tfidfErr != nil {
				fmt.Fprintf(os.Stderr, "warning: tfidf embedder init failed: %v\n", tfidfErr)
			} else {
				if eng != nil {
					eng.SetEmbedder(emb)
				}
				fmt.Fprintf(os.Stderr, "  embedder: tfidf (hashed lexical, forced)\n")
				fmt.Fprintln(os.Stderr, tfidfLexicalNotice)
			}
		case "none":
			fmt.Fprintln(os.Stderr, "  embedder: none (forced; dedup-against-retracted gate inactive)")
		default:
			// auto: probe Ollama, fall back to the hashed lexical embedder
			if engine.ProbeOllama(ollamaURL, embeddingModel) {
				emb := engine.NewOllamaEmbedder(ollamaURL, embeddingModel, 768)
				if eng != nil {
					eng.SetEmbedder(emb)
				}
				fmt.Fprintf(os.Stderr, "  embedder: ollama (%s)\n", embeddingModel)
			} else {
				emb, tfidfErr := engine.NewHashEmbedder(0)
				if tfidfErr != nil {
					fmt.Fprintf(os.Stderr, "warning: tfidf embedder init failed: %v\n", tfidfErr)
				} else {
					if eng != nil {
						eng.SetEmbedder(emb)
					}
					fmt.Fprintf(os.Stderr, "  embedder: tfidf (hashed lexical, fallback)\n")
					fmt.Fprintln(os.Stderr, tfidfLexicalNotice)
				}
			}
		}

		// Reconcile the active embedder against the corpus's declared vector
		// identity BEFORE embedding anything. On mismatch we lock (search fails
		// closed) and do NOT re-embed — that migration must be explicit. Only on
		// a match do we fill truly-missing vectors.
		if eng != nil && eng.Embedder != nil {
			st, err := eng.ReconcileVectorIdentity(context.Background())
			switch {
			case err != nil:
				fmt.Fprintf(os.Stderr, "warning: vector identity reconcile failed: %v\n", err)
			case !st.Match:
				fmt.Fprintf(os.Stderr, "\n⚠ %s\n\n", st.Reason)
			default:
				fmt.Fprintf(os.Stderr, "  vectors: %s\n", st.Action)
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

	// Bind the listener explicitly BEFORE advancing the snapshot retention
	// counter. net.Listen surfaces a bind failure (e.g. the port is already
	// in use) synchronously — a failed start must NOT count as a boot.
	// Otherwise SnapshotRetentionBoots failed `serve` attempts in a row would
	// auto-delete the migration safety snapshot without the migrated schema
	// ever having served a single request: the exact case the snapshot guards.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	// The listener is bound: this is a genuine "the new schema boots and
	// serves" signal. Tick retention now, then surface what's still retained.
	// Deliberately not in store.Open, so CLI subcommands that inspect or prune
	// snapshots don't advance the counter — only a real serve boot does.
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

	// Daily metrics rollup: snapshot health buckets + cumulative access on a
	// timer so the Memory Health trend lines accrue. Read-only against memories;
	// it only writes the metrics_daily ledger. Stops on shutdown.
	rollupStop := make(chan struct{})
	go func() {
		if err := db.RollupDailySnapshot(); err != nil {
			fmt.Fprintf(os.Stderr, "metrics rollup (startup): %v\n", err)
		}
		t := time.NewTicker(1 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-rollupStop:
				return
			case <-t.C:
				if err := db.RollupDailySnapshot(); err != nil {
					fmt.Fprintf(os.Stderr, "metrics rollup: %v\n", err)
				}
			}
		}
	}()

	// Graceful shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Fprintf(os.Stderr, "continuity serving on %s\n", addr)
		fmt.Fprintf(os.Stderr, "  db: %s\n", dbPath)
		if err := httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}()

	<-done
	close(rollupStop)
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
