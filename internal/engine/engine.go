package engine

import (
	"context"
	"fmt"
	"log"
	"strings"
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

// Dedup finds semantically duplicate leaf nodes and merges them.
// For each category, it clusters nodes by cosine similarity above threshold,
// keeps the most recently updated node per cluster, and deletes the rest.
// Returns the number of nodes removed.
func (e *Engine) Dedup(ctx context.Context, threshold float64) (int, error) {
	if e.Embedder == nil {
		return 0, fmt.Errorf("no embedder configured")
	}

	leaves, err := e.DB.ListLeaves()
	if err != nil {
		return 0, fmt.Errorf("list leaves: %w", err)
	}

	// Embed any leaves missing vectors first
	for i := range leaves {
		if leaves[i].L0Abstract == "" {
			continue
		}
		existing, _ := e.DB.GetVector(leaves[i].ID)
		if existing != nil {
			continue
		}
		vec, err := e.Embedder.Embed(ctx, leaves[i].L0Abstract)
		if err != nil {
			log.Printf("dedup: embed %s: %v", leaves[i].URI, err)
			continue
		}
		e.DB.SaveVector(leaves[i].ID, vec, e.Embedder.Model())
	}

	// Load all vectors and build lookup
	vectors, err := e.DB.AllVectors()
	if err != nil {
		return 0, fmt.Errorf("load vectors: %w", err)
	}

	vecMap := make(map[int64][]float64, len(vectors))
	for _, v := range vectors {
		vecMap[v.NodeID] = v.Embedding
	}

	// Group leaves by category
	byCategory := make(map[string][]store.MemNode)
	for _, n := range leaves {
		byCategory[n.Category] = append(byCategory[n.Category], n)
	}

	removed := 0
	for cat, nodes := range byCategory {
		// Track which nodes are already claimed by a cluster
		claimed := make(map[int64]bool)

		for i := 0; i < len(nodes); i++ {
			if claimed[nodes[i].ID] {
				continue
			}
			vecI, ok := vecMap[nodes[i].ID]
			if !ok {
				continue
			}

			// Start a cluster with this node as the initial keeper
			cluster := []int{i}
			for j := i + 1; j < len(nodes); j++ {
				if claimed[nodes[j].ID] {
					continue
				}
				vecJ, ok := vecMap[nodes[j].ID]
				if !ok {
					continue
				}

				sim := CosineSimilarity(vecI, vecJ)
				if sim >= threshold {
					cluster = append(cluster, j)
				}
			}

			if len(cluster) <= 1 {
				continue
			}

			// Find the most recently updated node in the cluster
			bestIdx := cluster[0]
			for _, idx := range cluster[1:] {
				if nodes[idx].UpdatedAt > nodes[bestIdx].UpdatedAt {
					bestIdx = idx
				}
			}

			// Delete all others
			for _, idx := range cluster {
				claimed[nodes[idx].ID] = true
				if idx == bestIdx {
					continue
				}
				log.Printf("dedup: removing %s (duplicate of %s in %s)", nodes[idx].URI, nodes[bestIdx].URI, cat)
				if err := e.DB.DeleteNode(nodes[idx].ID); err != nil {
					log.Printf("dedup: delete %s: %v", nodes[idx].URI, err)
					continue
				}
				removed++
			}
		}
	}

	// Clean up orphaned directory nodes
	if orphans, err := e.DB.DeleteOrphanDirs(); err != nil {
		log.Printf("dedup: cleanup orphan dirs: %v", err)
	} else if orphans > 0 {
		log.Printf("dedup: removed %d orphaned directory nodes", orphans)
	}

	return removed, nil
}

// ExtractSignal processes a user-flagged signal prompt and creates a memory immediately.
// This is designed to be called asynchronously (in a goroutine).
func (e *Engine) ExtractSignal(ctx context.Context, sessionID, prompt string) error {
	if e.LLM == nil {
		return fmt.Errorf("LLM not configured")
	}

	resp, err := e.LLM.Complete(ctx, llm.SignalExtractionPrompt(prompt))
	if err != nil {
		return fmt.Errorf("signal extraction LLM: %w", err)
	}

	candidates, err := parseExtractionResponse(resp.Content)
	if err != nil {
		return fmt.Errorf("parse signal response: %w", err)
	}

	for _, c := range candidates {
		vc, err := validateCandidate(c)
		if err != nil {
			log.Printf("signal: rejecting candidate %q: %v", c.URIHint, err)
			continue
		}
		c = vc

		owner := ownerForCategory(c.Category)
		uri := fmt.Sprintf("mem://%s/%s/%s", owner, c.Category, c.URIHint)

		if c.MergeTarget != "" && strings.HasPrefix(c.MergeTarget, "mem://") {
			uri = c.MergeTarget
		}

		node := &store.MemNode{
			URI:           uri,
			NodeType:      "leaf",
			Category:      c.Category,
			L0Abstract:    c.L0,
			L1Overview:    c.L1,
			L2Content:     c.L2,
			SourceSession: sessionID,
		}

		if err := e.DB.UpsertNode(node); err != nil {
			log.Printf("signal: failed to upsert %s: %v", uri, err)
			continue
		}
		log.Printf("signal: stored %s [%s]", uri, c.Category)

		// Embed if available
		if e.Embedder != nil && node.L0Abstract != "" {
			stored, err := e.DB.GetNodeByURI(node.URI)
			if err == nil && stored != nil {
				if vec, err := e.Embedder.Embed(ctx, stored.L0Abstract); err == nil {
					e.DB.SaveVector(stored.ID, vec, e.Embedder.Model())
				}
			}
		}
	}

	return nil
}

// ExtractSession runs the full extraction pipeline for a completed session.
// This is designed to be called asynchronously (in a goroutine).
// Idempotent: skips sessions that have already been extracted.
func (e *Engine) ExtractSession(sessionID, transcriptPath string) error {
	if transcriptPath == "" {
		return fmt.Errorf("no transcript path provided")
	}

	// Idempotency guard: skip if already extracted
	sess, err := e.DB.GetSession(sessionID)
	if err != nil {
		return fmt.Errorf("check session: %w", err)
	}
	if sess != nil && sess.ExtractedAt != nil {
		log.Printf("extraction: skipping %s — already extracted", sessionID)
		return nil
	}

	if err := extractMemories(e.DB, e.LLM, e.Embedder, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("memory extraction: %w", err)
	}

	if err := extractRelational(e.DB, e.LLM, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("relational extraction: %w", err)
	}

	// Mark as extracted so we don't re-process
	if err := e.DB.MarkExtracted(sessionID); err != nil {
		log.Printf("extraction: failed to mark %s as extracted: %v", sessionID, err)
	}

	return nil
}
