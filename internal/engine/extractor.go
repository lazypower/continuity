package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
	"github.com/lazypower/continuity/internal/transcript"
)

// defaultSimilarityThreshold is the cosine similarity threshold for deduplication.
// Candidates with similarity above this merge into existing nodes.
const defaultSimilarityThreshold = 0.65

// memoryCandidate is the JSON structure returned by the extraction LLM.
type memoryCandidate struct {
	Category    string `json:"category"`
	URIHint     string `json:"uri_hint"`
	L0          string `json:"l0"`
	L1          string `json:"l1"`
	L2          string `json:"l2"`
	MergeTarget string `json:"merge_target"`
}

// ownerForCategory returns the URI owner for a given category.
// "feedback" and "reference" intentionally take the default "user" branch:
// feedback captures guidance the user has given (issue #24), and reference
// captures pointers to systems the user works in (Linear, dashboards, rituals).
// An agent-side feedback tree is deferred to a later issue.
func ownerForCategory(category string) string {
	switch category {
	case "patterns", "cases":
		return "agent"
	default:
		return "user"
	}
}

// validCategories defines the allowed memory categories.
var validCategories = map[string]bool{
	"profile": true, "preferences": true, "entities": true,
	"events": true, "patterns": true, "cases": true,
	"moments": true, "feedback": true, "reference": true,
}

// findSimilarNode searches existing nodes for one semantically similar to the given
// L0 abstract within the same category. Returns the best match above threshold, or
// nil if none found. Unlike Find(), this has no side effects (no TouchNode).
func findSimilarNode(ctx context.Context, db *store.DB, embedder Embedder,
	l0 string, category string, threshold float64) (*store.MemNode, float64, error) {

	candidateVec, err := embedder.Embed(ctx, l0)
	if err != nil {
		return nil, 0, fmt.Errorf("embed candidate: %w", err)
	}
	activeID := EmbedderIdentity(embedder)

	vectors, err := db.AllVectors()
	if err != nil {
		return nil, 0, fmt.Errorf("load vectors: %w", err)
	}
	if len(vectors) == 0 {
		return nil, 0, nil
	}

	nodeIDs := make([]int64, len(vectors))
	for i, v := range vectors {
		nodeIDs[i] = v.NodeID
	}

	nodes, err := db.GetNodesByIDs(nodeIDs)
	if err != nil {
		return nil, 0, fmt.Errorf("get nodes: %w", err)
	}
	nodeMap := make(map[int64]store.MemNode, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}

	var bestNode *store.MemNode
	bestSim := 0.0

	for _, v := range vectors {
		if canonicalIdentity(v.Model, v.Dimensions) != activeID {
			continue // never compare across vector spaces
		}
		node, ok := nodeMap[v.NodeID]
		if !ok || node.NodeType != "leaf" || node.Category != category {
			continue
		}
		// Retracted nodes must not influence similarity selection. Returning
		// one as a merge target lets a later UpsertNode silently overwrite the
		// retracted row's content — resurrection through the back door. The
		// dedup-against-retracted gate (engine.findRetractedMatches) is a
		// separate path that intentionally finds these; this one must not.
		if node.IsRetracted() {
			continue
		}

		sim := CosineSimilarity(candidateVec, v.Embedding)
		if sim > bestSim && sim >= threshold {
			bestSim = sim
			n := node // avoid capturing loop variable
			bestNode = &n
		}
	}

	return bestNode, bestSim, nil
}

// extractMemories parses a transcript, condenses it, calls the LLM for extraction,
// and persists the resulting memory candidates. If embedder is non-nil, newly
// extracted nodes are embedded immediately.
func extractMemories(db *store.DB, client llm.Client, embedder Embedder, sessionID, transcriptPath string) error {
	entries, err := transcript.ParseFile(transcriptPath)
	if err != nil {
		return fmt.Errorf("parse transcript: %w", err)
	}

	// Guard: skip if < 3 user messages
	if transcript.CountUserMessages(entries) < 3 {
		log.Printf("extraction: skipping %s — fewer than 3 user messages", sessionID)
		return nil
	}

	condensed := transcript.Condense(entries)

	// Guard: skip if < 100 chars condensed
	if len(condensed) < 100 {
		log.Printf("extraction: skipping %s — condensed too short (%d chars)", sessionID, len(condensed))
		return nil
	}

	prompt := llm.ExtractionPrompt(condensed)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	resp, err := client.Complete(ctx, prompt)
	if err != nil {
		return fmt.Errorf("llm extraction: %w", err)
	}

	// Guard: skip if < 20 chars response
	if len(resp.Content) < 20 {
		log.Printf("extraction: skipping %s — LLM response too short (%d chars)", sessionID, len(resp.Content))
		return nil
	}

	// Parse JSON response — extract array from response
	candidates, err := parseExtractionResponse(resp.Content)
	if err != nil {
		return fmt.Errorf("parse extraction response: %w", err)
	}

	// Hard cap: even if the LLM returns more, only keep the first 3
	if len(candidates) > 3 {
		log.Printf("extraction: capping %d candidates to 3 for %s", len(candidates), sessionID)
		candidates = candidates[:3]
	}

	// Persist each candidate
	for _, c := range candidates {
		vc, err := validateCandidate(c)
		if err != nil {
			log.Printf("extraction: rejecting candidate %q: %v", c.URIHint, err)
			continue
		}
		c = vc

		owner := ownerForCategory(c.Category)
		uri := fmt.Sprintf("mem://%s/%s/%s", owner, c.Category, c.URIHint)
		gateCat := c.Category // category the content will actually land in

		// Honor merge_target ONLY if it RESOLVES to an existing node — its sole
		// purpose is to merge into one. A raw LLM string is never trusted as a URI:
		// a malformed/variant form (casing, ?query, #frag, trailing /) or a
		// hallucinated target won't resolve, so we ignore it and fall back to the
		// canonical constructed uri. This is what makes the gate robust — the write
		// target is always a canonical URI, never an attacker-shaped string that
		// dodges category derivation or the exact-URI lookup. If it resolves to a
		// RETRACTED node, merging would resurrect it: skip the candidate. When
		// honored, gate on the target's REAL category (not a parsed string).
		merged := false
		if c.MergeTarget != "" && strings.HasPrefix(c.MergeTarget, "mem://") {
			if target, err := db.GetNodeByURI(c.MergeTarget); err == nil && target != nil {
				if target.IsRetracted() {
					log.Printf("extraction: skipping %s — merge_target %s is retracted (would resurrect)", uri, c.MergeTarget)
					continue
				}
				uri = target.URI
				gateCat = target.Category
				merged = true
			} else {
				log.Printf("extraction: ignoring merge_target %s — no such node; using %s", c.MergeTarget, uri)
			}
		}

		// Similarity gate: check if a semantically equivalent LIVE node already
		// exists in the declared category. Skipped when an explicit merge_target was
		// honored (an explicit instruction wins over the heuristic). The redirect
		// only ever targets a live node in c.Category, so gateCat is unchanged.
		if !merged && embedder != nil && c.Category != "" {
			match, sim, err := findSimilarNode(ctx, db, embedder, c.L0, c.Category, MatchThreshold(embedder))
			if err != nil {
				log.Printf("extraction: similarity check failed: %v", err)
				// Continue with normal upsert on error — don't block extraction
			} else if match != nil {
				log.Printf("extraction: merging %s → %s (similarity: %.3f)", uri, match.URI, sim)
				uri = match.URI // Redirect to existing node's URI
			}
		}

		// Retraction-resurrection gate (per-candidate, fail-closed): a candidate
		// that matches a retracted memory must NOT be written — otherwise retracted
		// (e.g. PII) content silently resurfaces as a fresh live node. findSimilarNode
		// deliberately skips retracted nodes (so it can't merge INTO one), which is
		// why this separate semantic gate is required to catch the create-a-new-node
		// path. It keys on gateCat — the category the content actually lands in (the
		// merge target's real category for an honored merge, else the declared one) —
		// so a cross-category merge can't be checked against the wrong vector space.
		// Skip only the offending candidate — one bad candidate must not drop the
		// rest of the batch; on a gate error we also skip (fail closed). The embedder
		// is nil only in `none` mode (gate opted out); the locked case is deferred
		// upstream in extractSession.
		if embedder != nil && c.L0 != "" {
			matches, err := findRetractedMatchesIn(ctx, db, embedder, c.L0, gateCat, MatchThreshold(embedder))
			if err != nil {
				log.Printf("extraction: retracted-check failed for %s — skipping candidate (fail-closed): %v", uri, err)
				continue
			}
			if len(matches) > 0 {
				log.Printf("extraction: skipping %s — matches %d retracted node(s) hash=%s", uri, len(matches), hashMatchedURIs(matches))
				continue
			}
		}

		// Exact retracted-URI guard (mirrors Remember): the constructed uri_hint can
		// still collide with a retracted canonical node that has no same-identity
		// vector. UpsertNode enforces this atomically too (ErrRetractedTarget), but
		// skipping here keeps a clean per-candidate log and avoids a wasted write.
		if existing, err := db.GetNodeByURI(uri); err == nil && existing != nil && existing.IsRetracted() {
			log.Printf("extraction: skipping %s — target URI is retracted (would resurrect)", uri)
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

		if err := db.UpsertNode(node); err != nil {
			log.Printf("extraction: failed to upsert %s: %v", uri, err)
			continue
		}
		log.Printf("extraction: stored %s [%s]", uri, c.Category)

		// Keep the stored vector in sync with the (possibly updated) content.
		// UpsertNode may have merged into an existing node — look it up for its ID.
		// When no usable embedder is available (locked / none), DELETE any existing
		// vector instead of skipping: otherwise a content update leaves a stale
		// vector that search would serve once the embedder returns (EmbedMissing
		// only fills MISSING vectors). DeleteVector is a no-op for a fresh node.
		if stored, err := db.GetNodeByURI(node.URI); err == nil && stored != nil {
			if embedder != nil && node.L0Abstract != "" {
				if vec, err := embedder.Embed(ctx, node.L0Abstract); err != nil {
					log.Printf("extraction: embed %s: %v", uri, err)
				} else if err := db.SaveVector(stored.ID, vec, embedder.Model()); err != nil {
					log.Printf("extraction: save vector %s: %v", uri, err)
				}
			} else if err := db.DeleteVector(stored.ID); err != nil {
				log.Printf("extraction: clear stale vector %s: %v", uri, err)
			}
		}
	}

	return nil
}

// parseExtractionResponse extracts a JSON array from the LLM response.
// The response might contain markdown code fences or other wrapper text.
func parseExtractionResponse(content string) ([]memoryCandidate, error) {
	content = strings.TrimSpace(content)

	// Strip markdown code fences if present
	if strings.HasPrefix(content, "```") {
		lines := strings.Split(content, "\n")
		// Remove first and last lines (```json and ```)
		if len(lines) > 2 {
			content = strings.Join(lines[1:len(lines)-1], "\n")
		}
	}

	content = strings.TrimSpace(content)

	// Find the JSON array
	start := strings.Index(content, "[")
	end := strings.LastIndex(content, "]")
	if start < 0 || end < 0 || end <= start {
		return nil, fmt.Errorf("no JSON array found in response")
	}

	jsonStr := content[start : end+1]

	var candidates []memoryCandidate
	if err := json.Unmarshal([]byte(jsonStr), &candidates); err != nil {
		return nil, fmt.Errorf("unmarshal candidates: %w", err)
	}

	return candidates, nil
}
