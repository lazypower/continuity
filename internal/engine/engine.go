package engine

import (
	"fmt"
	"log"
	"time"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// Engine orchestrates memory extraction, relational profiling, and decay.
type Engine struct {
	DB     *store.DB
	LLM    llm.Client
	stopCh chan struct{}
}

// New creates a new Engine.
func New(db *store.DB, client llm.Client) *Engine {
	return &Engine{
		DB:     db,
		LLM:    client,
		stopCh: make(chan struct{}),
	}
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

	if err := extractMemories(e.DB, e.LLM, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("memory extraction: %w", err)
	}

	if err := extractRelational(e.DB, e.LLM, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("relational extraction: %w", err)
	}

	return nil
}
