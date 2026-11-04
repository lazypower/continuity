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
// and as an escape-hatch override for one-off testing — config.toml
// ([embedder].backend etc.) is the persistent configuration surface for normal
// use; see selectEmbedder for the full precedence between the two.
const (
	envServeDB       = "CONTINUITY_DB"       // overrides Database.Path
	envServePort     = "CONTINUITY_PORT"     // overrides Server.Port (int)
	envServeBind     = "CONTINUITY_BIND"     // overrides Server.Bind
	envServeEmbedder = "CONTINUITY_EMBEDDER" // "tfidf" | "ollama" | "model2vec" | "none" | "" (auto); wins over config [embedder].backend
	envServeGC       = "CONTINUITY_GC"       // "off" (default) | "shadow" | "on"

	// envServeExtractionAuto re-enables the deprecated automatic session-end
	// extraction (default off). Accepts any strconv.ParseBool value.
	envServeExtractionAuto = "CONTINUITY_EXTRACTION_AUTO"

	// envServeRelationalAuto is the kill switch for automatic relational
	// profiling at session end (default on; #78). Accepts any strconv.ParseBool
	// value. Setting it false restores the pre-#78 behavior: non-force /extract
	// requests are skipped entirely while autoExtract is off.
	envServeRelationalAuto = "CONTINUITY_RELATIONAL_AUTO"
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
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

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
		gcMode := engine.ParseGCMode(os.Getenv(envServeGC))
		eng.SetGCMode(gcMode)
		defer eng.Stop()
		fmt.Fprintf(os.Stderr, "  llm: %s (%s)\n", cfg.LLM.Provider, cfg.LLM.Model)
		if gcMode != engine.GCOff {
			fmt.Fprintf(os.Stderr, "  gc: %s\n", gcMode)
		}
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

		declaredIdentity, _, identErr := db.VectorIdentity()
		if identErr != nil {
			fmt.Fprintf(os.Stderr, "warning: read corpus vector identity failed: %v\n", identErr)
			declaredIdentity = ""
		}

		emb, logLine := selectEmbedder(embedderSelectionInput{
			EnvOverride:      strings.ToLower(strings.TrimSpace(os.Getenv(envServeEmbedder))),
			ConfigBackend:    strings.ToLower(strings.TrimSpace(cfg.Embedder.Backend)),
			DeclaredIdentity: declaredIdentity,
			OllamaURL:        ollamaURL,
			EmbeddingModel:   embeddingModel,
		})
		if emb != nil && eng != nil {
			eng.SetEmbedder(emb)
		}
		fmt.Fprintln(os.Stderr, logLine)
		if emb != nil && emb.Model() == "hashtf" {
			fmt.Fprintln(os.Stderr, tfidfLexicalNotice)
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
	srv.SetAutoExtraction(cfg.Extraction.Auto)
	srv.SetRelationalAuto(cfg.Extraction.RelationalAuto)
	if !cfg.Extraction.RelationalAuto {
		fmt.Fprintf(os.Stderr,
			"  ! extraction.relational_auto DISABLED — the relational profile will not "+
				"update from session ends. Unset %s to return to the default.\n",
			envServeRelationalAuto)
	}
	if cfg.Extraction.Auto {
		fmt.Fprintf(os.Stderr,
			"  ! extraction.auto ENABLED — automatic session extraction is on; it is off by "+
				"default (unmeasured usefulness, non-provenance-distinguishable writes). "+
				"Unset %s to return to the default.\n", envServeExtractionAuto)
	}
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

	// Start the extraction worker only after a successful bind — a failed bind
	// returns from runServe under deferred db.Close()/eng.Stop(), so a worker
	// started earlier could run against a closing DB. The shutdown path below is
	// the single place that drains it.
	srv.StartExtractionWorker()

	// Start decay + GC only after a successful bind — same boundary as the worker.
	// GC hard-deletes, so a failed-start process must never run a compost sweep.
	if eng != nil {
		eng.StartDecayTimer()
	}

	// Observation retention is deliberately NOT inside the eng != nil guard.
	// The decay/GC timer needs an Engine because it acts on memories; retention
	// is pure storage hygiene, and an install with no LLM configured records
	// observations at exactly the same rate. Gating it on the Engine would leave
	// those installs growing without bound — the original issue #72 failure.
	retentionStop := make(chan struct{})
	engine.StartRetentionTimer(db, retentionStop)

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
	close(retentionStop)
	fmt.Fprintln(os.Stderr, "\nshutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownErr := httpServer.Shutdown(ctx)
	// Drain the extraction worker after HTTP stops accepting: a job still running
	// gets a bounded window to finish; if it doesn't, its queue row persists and
	// replays on the next boot (H1).
	srv.StopExtractionWorker(10 * time.Second)
	return shutdownErr
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
	if v := strings.TrimSpace(os.Getenv(envServeExtractionAuto)); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s=%q: must be a boolean (true/false/1/0)", envServeExtractionAuto, v)
		}
		cfg.Extraction.Auto = enabled
	}
	if v := strings.TrimSpace(os.Getenv(envServeRelationalAuto)); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s=%q: must be a boolean (true/false/1/0)", envServeRelationalAuto, v)
		}
		cfg.Extraction.RelationalAuto = enabled
	}
	return nil
}
