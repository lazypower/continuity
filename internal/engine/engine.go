package engine

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
	"github.com/lazypower/continuity/internal/transcript"
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

// RememberInput holds structured memory content for direct storage (no LLM needed).
type RememberInput struct {
	Category  string
	Name      string
	Summary   string // L0 abstract
	Body      string // L1 overview
	Detail    string // L2 content (optional)
	SessionID string // optional provenance
}

// Remember stores a structured memory directly — no LLM round-trip needed.
// Returns the resulting URI, whether the node was newly created, and any error.
//
// The caller-supplied slug (input.Name) is always honored. The semantic-similarity
// dedup heuristic that runs on the LLM extraction path is intentionally skipped
// here: a direct write through this API is explicit user/agent intent, and
// silently redirecting it onto a near-duplicate's URI causes silent data loss
// (see issue #11). For immutable-category slug collisions, the underlying
// UpsertNode appends a timestamp suffix; we report the actual stored URI.
func (e *Engine) Remember(ctx context.Context, input RememberInput) (string, bool, error) {
	c := memoryCandidate{
		Category: input.Category,
		URIHint:  input.Name,
		L0:       input.Summary,
		L1:       input.Body,
		L2:       input.Detail,
	}

	vc, err := validateCandidate(c)
	if err != nil {
		return "", false, fmt.Errorf("validate: %w", err)
	}
	c = vc

	owner := ownerForCategory(c.Category)
	requestedURI := fmt.Sprintf("mem://%s/%s/%s", owner, c.Category, c.URIHint)

	existing, err := e.DB.GetNodeByURI(requestedURI)
	if err != nil {
		return "", false, fmt.Errorf("check existing: %w", err)
	}

	node := &store.MemNode{
		URI:           requestedURI,
		NodeType:      "leaf",
		Category:      c.Category,
		L0Abstract:    c.L0,
		L1Overview:    c.L1,
		L2Content:     c.L2,
		SourceSession: input.SessionID,
	}

	if err := e.DB.UpsertNode(node); err != nil {
		return "", false, fmt.Errorf("upsert: %w", err)
	}

	// UpsertNode mutates node.URI when an immutable-category slug collision
	// triggers the timestamp-suffix path. created reflects whether a new row
	// was inserted (fresh slug, OR collision-with-suffix), as opposed to an
	// in-place merge of a mergeable category.
	created := existing == nil || node.URI != requestedURI
	storedURI := node.URI
	log.Printf("remember: stored %s [%s] (created=%v)", storedURI, c.Category, created)

	// Embed if available
	if e.Embedder != nil && node.L0Abstract != "" {
		stored, err := e.DB.GetNodeByURI(storedURI)
		if err == nil && stored != nil {
			if err := e.EmbedNode(ctx, stored); err != nil {
				log.Printf("remember: embed %s: %v", storedURI, err)
			}
		}
	}

	// Moments pool cap: evict most redundant when pool exceeds 10
	if c.Category == "moments" && e.Embedder != nil {
		if evicted, err := e.evictRedundantMoment(ctx); err != nil {
			log.Printf("remember: moment eviction failed: %v", err)
		} else if evicted != "" {
			log.Printf("remember: evicted redundant moment %s", evicted)
		}
	}

	return storedURI, created, nil
}

const maxMoments = 10

// evictRedundantMoment checks the moments pool size and removes the most
// semantically redundant moment if the pool exceeds maxMoments. Redundancy
// is measured by average cosine similarity to all other moments — the moment
// most "covered" by the rest gets evicted.
// Returns the URI of the evicted moment, or empty string if no eviction needed.
func (e *Engine) evictRedundantMoment(ctx context.Context) (string, error) {
	moments, err := e.DB.FindByCategory("moments")
	if err != nil {
		return "", fmt.Errorf("find moments: %w", err)
	}
	if len(moments) <= maxMoments {
		return "", nil
	}

	// Ensure all moments are embedded
	for i := range moments {
		if moments[i].L0Abstract == "" {
			continue
		}
		existing, _ := e.DB.GetVector(moments[i].ID)
		if existing != nil {
			continue
		}
		if err := e.EmbedNode(ctx, &moments[i]); err != nil {
			log.Printf("evict: embed %s: %v", moments[i].URI, err)
		}
	}

	// Load vectors for all moments
	type momentVec struct {
		node store.MemNode
		vec  []float64
	}
	var pool []momentVec
	for _, m := range moments {
		v, err := e.DB.GetVector(m.ID)
		if err != nil || v == nil {
			continue
		}
		pool = append(pool, momentVec{m, v.Embedding})
	}

	if len(pool) <= maxMoments {
		return "", nil // not enough embedded moments to evict
	}

	// Compute average similarity for each moment against all others
	var mostRedundantIdx int
	highestAvgSim := -1.0

	for i := range pool {
		var totalSim float64
		for j := range pool {
			if i == j {
				continue
			}
			totalSim += CosineSimilarity(pool[i].vec, pool[j].vec)
		}
		avgSim := totalSim / float64(len(pool)-1)
		if avgSim > highestAvgSim {
			highestAvgSim = avgSim
			mostRedundantIdx = i
		}
	}

	// Evict the most redundant
	evictURI := pool[mostRedundantIdx].node.URI
	if err := e.DB.DeleteNode(pool[mostRedundantIdx].node.ID); err != nil {
		return "", fmt.Errorf("delete redundant moment: %w", err)
	}

	// Clean up orphaned directory nodes
	e.DB.DeleteOrphanDirs()

	return evictURI, nil
}

// mergeableCategory returns whether the given category supports in-place merging.
func mergeableCategory(category string) bool {
	switch category {
	case "profile", "preferences", "patterns":
		return true
	default:
		return false
	}
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

// extractTone runs tone extraction for a session and stores the result.
func extractTone(db *store.DB, client llm.Client, sessionID, transcriptPath string) error {
	entries, err := transcript.ParseFile(transcriptPath)
	if err != nil {
		return fmt.Errorf("parse transcript: %w", err)
	}

	condensed := transcript.Condense(entries)
	if len(condensed) < 100 {
		return nil // too short for meaningful tone
	}

	prompt := llm.TonePrompt(condensed)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resp, err := client.Complete(ctx, prompt)
	if err != nil {
		return fmt.Errorf("llm tone extraction: %w", err)
	}

	tone := strings.TrimSpace(resp.Content)
	// Strip quotes if LLM wraps it
	tone = strings.Trim(tone, "\"'`")
	tone = strings.TrimSpace(tone)

	if tone == "" || len(tone) > 200 {
		log.Printf("tone: rejecting for %s — empty or too long (%d chars)", sessionID, len(tone))
		return nil
	}

	if err := db.SetSessionTone(sessionID, tone); err != nil {
		return fmt.Errorf("store tone: %w", err)
	}
	log.Printf("tone: %s → %q", sessionID, tone)
	return nil
}

// ExtractSession runs the full extraction pipeline for a completed session.
// This is designed to be called asynchronously (in a goroutine).
// Idempotent: skips sessions that have already been extracted.
// Content-gated: sessions with insufficient content (fewer than 3 user
// messages or <100 chars condensed) return nil WITHOUT marking the session
// as extracted, so subsequent Stop/SessionEnd hooks get another chance once
// the conversation grows.
func (e *Engine) ExtractSession(sessionID, transcriptPath string) error {
	return e.extractSession(sessionID, transcriptPath, false)
}

// ExtractSessionForce runs extraction while bypassing the idempotency guard.
// The content gate still applies — forcing extraction on a genuinely empty
// session is a no-op. Used by `continuity extract --force` for reprocessing
// sessions that were incorrectly marked as extracted.
func (e *Engine) ExtractSessionForce(sessionID, transcriptPath string) error {
	return e.extractSession(sessionID, transcriptPath, true)
}

func (e *Engine) extractSession(sessionID, transcriptPath string, force bool) error {
	if transcriptPath == "" {
		return fmt.Errorf("no transcript path provided")
	}

	// Idempotency guard: skip if already extracted (unless forced)
	if !force {
		sess, err := e.DB.GetSession(sessionID)
		if err != nil {
			return fmt.Errorf("check session: %w", err)
		}
		if sess != nil && sess.ExtractedAt != nil {
			log.Printf("extraction: skipping %s — already extracted", sessionID)
			return nil
		}
	}

	// Pre-flight content gate — return without marking if there's not enough
	// to extract yet. Parsing the transcript here is cheap; the downstream
	// extractors re-parse but that's a separate concern.
	ok, reason, err := hasEnoughContent(transcriptPath)
	if err != nil {
		return fmt.Errorf("content gate: %w", err)
	}
	if !ok {
		log.Printf("extraction: skipping %s — %s (not marking)", sessionID, reason)
		return nil
	}

	if err := extractMemories(e.DB, e.LLM, e.Embedder, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("memory extraction: %w", err)
	}

	if err := extractRelational(e.DB, e.LLM, sessionID, transcriptPath); err != nil {
		return fmt.Errorf("relational extraction: %w", err)
	}

	if err := extractTone(e.DB, e.LLM, sessionID, transcriptPath); err != nil {
		log.Printf("tone extraction failed (non-fatal): %v", err)
	}

	// Mark as extracted so we don't re-process
	if err := e.DB.MarkExtracted(sessionID); err != nil {
		log.Printf("extraction: failed to mark %s as extracted: %v", sessionID, err)
	}

	return nil
}

// hasEnoughContent returns true when the transcript meets the extractors'
// minimum thresholds (>=3 user messages AND >=100 chars condensed). This is
// the single source of truth for the content gate — mirrored client-side in
// the Stop hook to avoid unnecessary HTTP round-trips.
func hasEnoughContent(transcriptPath string) (bool, string, error) {
	entries, err := transcript.ParseFile(transcriptPath)
	if err != nil {
		return false, "", fmt.Errorf("parse transcript: %w", err)
	}
	if transcript.CountUserMessages(entries) < 3 {
		return false, "fewer than 3 user messages", nil
	}
	if len(transcript.Condense(entries)) < 100 {
		return false, "condensed transcript too short", nil
	}
	return true, "", nil
}
