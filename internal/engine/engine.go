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

	// Vector-identity lock. Set by ReconcileVectorIdentity when the active
	// embedder's identity differs from the corpus's declared identity. While
	// locked, search must fail closed rather than compare query vectors against
	// a corpus embedded in a different vector space, and EmbedMissing must not
	// run (no silent re-embed). Cleared only by an explicit repair.
	identityMismatch bool
	identityReason   string
}

// VectorIdentityLocked reports whether the active embedder is incompatible with
// the corpus's declared vector identity. When true, reason explains the
// mismatch and points the operator at repair; callers (search) must fail closed.
func (e *Engine) VectorIdentityLocked() (bool, string) {
	return e.identityMismatch, e.identityReason
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

// EmbedNode brings a node's stored vector in sync with its current content, or
// removes a stale one. When the active embedder can't produce a vector
// compatible with the corpus — none configured, or the vector identity is locked
// — it DELETES any existing vector and leaves the node Pending. This is critical
// on a content UPDATE: skipping the embed while leaving the old vector in place
// would make search serve a vector describing the previous content once the
// embedder returns (EmbedMissing only fills MISSING vectors). DeleteVector is a
// no-op when none exists, so a fresh node simply stays Pending.
func (e *Engine) EmbedNode(ctx context.Context, node *store.MemNode) error {
	if e.Embedder == nil || e.identityMismatch {
		return e.DB.DeleteVector(node.ID)
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

// EmbedMissing embeds leaf nodes that have NO vector yet, using the active
// embedder. It deliberately does NOT re-embed nodes whose stored model differs
// from the active embedder: re-embedding an existing corpus into a new vector
// space is a corpus migration and must be explicit (snapshot-first repair), not
// a silent side effect of startup. Callers run this only when the active
// embedder matches the corpus's declared vector identity (see
// ReconcileVectorIdentity); while the identity is locked, it must not run.
func (e *Engine) EmbedMissing(ctx context.Context) (int, error) {
	if e.Embedder == nil {
		return 0, nil
	}
	if e.identityMismatch {
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

		// Fill only truly-missing vectors. A vector that exists under a
		// different model is STALE, not missing — leave it for explicit repair
		// rather than silently re-embedding it into the active vector space.
		existing, err := e.DB.GetVector(leaves[i].ID)
		if err != nil {
			log.Printf("embed missing: get vector for %s: %v", leaves[i].URI, err)
			continue
		}
		if existing != nil {
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

	// Cluster only within the active identity — never delete a memory based on a
	// cross-space cosine score against a stale foreign-identity vector (which can
	// linger even when active==declared, e.g. after an interrupted repair).
	activeID := EmbedderIdentity(e.Embedder)
	vecMap := make(map[int64][]float64, len(vectors))
	for _, v := range vectors {
		if canonicalIdentity(v.Model, v.Dimensions) != activeID {
			continue
		}
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

	// AcknowledgeRetracted, when true, bypasses the dedup-against-retracted gate.
	// Set this only after the agent has fetched the matched memory's reason via
	// `continuity show <uri> --include-retracted` and decided the candidate is
	// genuinely different. Override events are intentionally not recorded — see
	// issue #12 / RFC "Override behavior."
	AcknowledgeRetracted bool
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

	// Direct URI collision with a retracted memory: refuse the write rather than
	// silently overwriting the tombstone. The agent must choose a different slug
	// or, if the retraction was wrong, restore via SQL (the friction-bearing path).
	if existing != nil && existing.IsRetracted() {
		return "", false, validationErrorf("uri %s is retracted; choose a different slug", requestedURI)
	}

	// While the vector identity is locked, the retracted-memory safety gate
	// cannot run (the active embedder is incompatible with the corpus). Fail
	// CLOSED: refuse an unacknowledged write rather than risk re-introducing
	// retracted (e.g. PII) content the gate would have caught. An explicit
	// --acknowledge-retracted still overrides, matching the unlocked flow.
	if !input.AcknowledgeRetracted && e.identityMismatch {
		return "", false, validationErrorf("vector identity is locked — the retracted-memory check cannot run; resolve with `continuity doctor` (and --repair-vectors), or re-run with --acknowledge-retracted to write without the check")
	}

	// Dedup against retracted memories. Retracted memories must still participate
	// in similarity matching, or retraction-because-PII is silently broken: the
	// next session writes similar content, hits no match, re-introduces the leak.
	// Reasons are NOT exposed inline — agent fetches them deliberately via
	// `continuity show <uri> --include-retracted`. See issue #12 / RFC.
	if !input.AcknowledgeRetracted && e.Embedder != nil && c.L0 != "" {
		matches, err := e.findRetractedMatches(ctx, c.L0, c.Category, MatchThreshold(e.Embedder))
		if err != nil {
			// Fail CLOSED: if the gate can't complete we cannot prove the write is
			// safe, so refuse it rather than risk re-introducing retracted content.
			// --acknowledge-retracted still overrides (this block is gated on it).
			return "", false, fmt.Errorf("retracted-memory check failed (failing closed; re-run with --acknowledge-retracted to bypass): %w", err)
		}
		if len(matches) > 0 {
			uris := make([]string, len(matches))
			for i, m := range matches {
				uris[i] = m.URI
			}
			log.Printf("dedup-retracted: candidate matches %d retracted node(s) hash=%s", len(matches), hashMatchedURIs(matches))
			return "", false, &RetractedMatchError{MatchedURIs: uris}
		}
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

	// Reconcile the stored vector with the new content UNCONDITIONALLY: EmbedNode
	// embeds when possible, or clears a stale vector when no compatible embedder
	// is available (locked OR none) — so an update never leaves search serving a
	// vector for the previous content.
	if stored, err := e.DB.GetNodeByURI(storedURI); err == nil && stored != nil {
		if err := e.EmbedNode(ctx, stored); err != nil {
			log.Printf("remember: embed %s: %v", storedURI, err)
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
	// While the vector identity is locked, do not run vector-based eviction:
	// comparing moments across vector spaces could delete a real moment on
	// cross-space noise rather than a genuinely redundant one.
	if e.identityMismatch {
		return "", nil
	}

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
	activeID := EmbedderIdentity(e.Embedder)
	var pool []momentVec
	for _, m := range moments {
		v, err := e.DB.GetVector(m.ID)
		if err != nil || v == nil {
			continue
		}
		if canonicalIdentity(v.Model, v.Dimensions) != activeID {
			continue // never compare across vector spaces
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

// ExtractSignal processes a user-flagged signal prompt and creates a memory immediately.
// This is designed to be called asynchronously (in a goroutine).
func (e *Engine) ExtractSignal(ctx context.Context, sessionID, prompt string) error {
	if e.LLM == nil {
		return fmt.Errorf("LLM not configured")
	}

	// Fail closed while the vector identity is locked: the retraction gate can't
	// run, so a signal write could silently re-introduce retracted content. Defer
	// rather than write unchecked; the operator repairs via `continuity doctor`.
	if e.identityMismatch {
		log.Printf("signal: deferring — vector identity locked; run `continuity doctor --repair-vectors`")
		return nil
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

		// An LLM-supplied merge_target is intentionally NOT honored (see the matching
		// note in extractMemories): trusting an LLM-chosen URI was a recurring gate
		// bypass. The candidate always lands in its declared category, so the gate
		// keys on c.Category.

		// Retraction-resurrection gate (per-candidate, fail-closed): a signal
		// candidate matching a retracted memory must not be written. Skip only the
		// offending candidate; on a gate error skip it too rather than write unchecked.
		// (Locked identity is handled above; embedder is nil here only in `none` mode.)
		if emb := e.embedderIfUnlocked(); emb != nil && c.L0 != "" {
			matches, err := e.findRetractedMatches(ctx, c.L0, c.Category, MatchThreshold(emb))
			if err != nil {
				log.Printf("signal: retracted-check failed for %s — skipping candidate (fail-closed): %v", uri, err)
				continue
			}
			if len(matches) > 0 {
				log.Printf("signal: skipping %s — matches %d retracted node(s) hash=%s", uri, len(matches), hashMatchedURIs(matches))
				continue
			}
		}

		// Exact retracted-URI guard (mirrors Remember): the constructed uri_hint can
		// still collide with a retracted canonical node the vector gate can't catch
		// (no same-identity vector). UpsertNode also enforces this atomically.
		if existing, err := e.DB.GetNodeByURI(uri); err == nil && existing != nil && existing.IsRetracted() {
			log.Printf("signal: skipping %s — target URI is retracted (would resurrect)", uri)
			continue
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

		// Keep the stored vector in sync; when locked/none, DELETE any stale vector
		// so a content update can't leave search serving the previous content.
		if stored, err := e.DB.GetNodeByURI(node.URI); err == nil && stored != nil {
			if emb := e.embedderIfUnlocked(); emb != nil && stored.L0Abstract != "" {
				if vec, err := emb.Embed(ctx, stored.L0Abstract); err == nil {
					e.DB.SaveVector(stored.ID, vec, emb.Model())
				}
			} else {
				e.DB.DeleteVector(stored.ID)
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

	// Fail closed while the vector identity is locked: the active embedder is
	// incompatible with the corpus, so the retraction-resurrection gate cannot
	// run, and extraction would write new memory nodes that could silently
	// re-introduce retracted (e.g. PII) content the gate would have caught. Defer
	// the whole session WITHOUT marking it extracted, so the next Stop/SessionEnd
	// re-extracts once the operator repairs (`continuity doctor --repair-vectors`).
	if e.identityMismatch {
		log.Printf("extraction: deferring %s — vector identity locked; run `continuity doctor --repair-vectors` (not marking extracted)", sessionID)
		return nil
	}

	// embedderIfUnlocked: with the identity NOT locked, this is the active embedder
	// (or nil only in `none` mode, where the operator opted out of the gate).
	if err := extractMemories(e.DB, e.LLM, e.embedderIfUnlocked(), sessionID, transcriptPath); err != nil {
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
