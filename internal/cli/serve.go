package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lazypower/continuity/internal/config"
	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/server"
	"github.com/lazypower/continuity/internal/store"
	"github.com/spf13/cobra"
)

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
		Addr:    addr,
		Handler: srv,
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
