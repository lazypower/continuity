package engine

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// Engine orchestrates memory extraction, relational profiling, and decay.
type Engine struct {
	DB       *store.DB
	LLM      llm.Client
	Embedder Embedder
	stopCh   chan struct{}
}

// New creates a new Engine.
func New(db *store.DB, client llm.Client) *Engine {
	return &Engine{
		DB:     db,
		LLM:    client,
		stopCh: make(chan struct{}),
	}
}

// SetEmbedder configures the embedding provider.
func (e *Engine) SetEmbedder(emb Embedder) {
	e.Embedder = emb
}

// EmbedNode generates and stores an embedding for a single node.
func (e *Engine) EmbedNode(ctx context.Context, node *store.MemNode) error {
	if e.Embedder == nil {
		return nil
	}
	text := node.L0Abstract
	if text == "" {
		return nil
	}

	vec, err := e.Embedder.Embed(ctx, text)
	if err != nil {
		return fmt.Errorf("embed node %s: %w", node.URI, err)
	}
	return e.DB.SaveVector(node.ID, vec, e.Embedder.Model())
}

// EmbedMissing embeds all leaf nodes that don't have a vector or whose model differs.
func (e *Engine) EmbedMissing(ctx context.Context) (int, error) {
	if e.Embedder == nil {
		return 0, nil
	}

	leaves, err := e.DB.ListLeaves()
	if err != nil {
		return 0, fmt.Errorf("list leaves: %w", err)
	}

	embedded := 0
	for i := range leaves {
		if leaves[i].L0Abstract == "" {
			continue
		}

		// Check if vector exists with current model
		existing, err := e.DB.GetVector(leaves[i].ID)
		if err != nil {
			log.Printf("embed missing: get vector for %s: %v", leaves[i].URI, err)
			continue
		}
		if existing != nil && existing.Model == e.Embedder.Model() {
			continue
		}

		if err := e.EmbedNode(ctx, &leaves[i]); err != nil {
			log.Printf("embed missing: %v", err)
			continue
		}
		embedded++
	}

	return embedded, nil
}

// StartDecayTimer runs smart decay on startup and then daily.
func (e *Engine) StartDecayTimer() {
	// Run once at startup
	if updated, err := e.DB.DecayAllNodes(); err != nil {
		log.Printf("decay error: %v", err)
	} else if updated > 0 {
		log.Printf("decay: updated %d nodes", updated)
	}

	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if updated, err := e.DB.DecayAllNodes(); err != nil {
					log.Printf("decay error: %v", err)
				} else if updated > 0 {
					log.Printf("decay: updated %d nodes", updated)
				}
			case <-e.stopCh:
				return
			}
		}
	}()
}

// Stop shuts down the engine's background goroutines.
func (e *Engine) Stop() {
	close(e.stopCh)
}

// ExtractSession runs the full extraction pipeline for a completed session.
// This is designed to be called asynchronously (in a goroutine).
func (e *Engine) ExtractSession(sessionID, transcriptPath string) error {
	if transcriptPath == "" {
		return fmt.Errorf("no transcript path provided")
	}

	if err := extractMemories(e.DB, e.LLM, e.Embedder, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("memory extraction: %w", err)
	}

	if err := extractRelational(e.DB, e.LLM, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("relational extraction: %w", err)
	}

	return nil
}
