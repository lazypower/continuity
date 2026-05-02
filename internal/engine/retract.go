package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/lazypower/continuity/internal/store"
)

// RetractInput holds parameters for a memory retraction.
type RetractInput struct {
	URI          string
	Reason       string
	SupersededBy string // optional; supersession when non-empty
}

// Retract marks a memory as retracted. Required: URI, Reason. Optional: SupersededBy.
//
// Returns (newly bool, error). newly is true when this call performed the retraction;
// false when the memory was already retracted (idempotent — the act has already
// happened, original reason and timestamp preserved).
//
// Memory is not immutable; it is accountable. The retracted node is excluded from
// default reads (search/find/tree/show/context-injection) but remains in the database
// as a marker — no silent erasure. See issue #12 / RFC for the contract.
func (e *Engine) Retract(ctx context.Context, input RetractInput) (bool, error) {
	if !strings.HasPrefix(input.URI, "mem://") {
		return false, fmt.Errorf("invalid URI %q: must start with mem://", input.URI)
	}
	if input.SupersededBy != "" && !strings.HasPrefix(input.SupersededBy, "mem://") {
		return false, fmt.Errorf("invalid superseded_by URI %q: must start with mem://", input.SupersededBy)
	}

	newly, err := e.DB.RetractNode(input.URI, input.Reason, input.SupersededBy)
	if err != nil {
		return false, err
	}

	if newly {
		if input.SupersededBy != "" {
			log.Printf("retract: %s superseded by %s", input.URI, input.SupersededBy)
		} else {
			log.Printf("retract: %s tombstoned", input.URI)
		}
	} else {
		log.Printf("retract: %s already retracted (no-op)", input.URI)
	}
	return newly, nil
}

// hashURI returns a short, stable identifier for a URI suitable for operator-facing
// logs. URIs in retraction-related logs are correlatable to reasons by anyone with
// --include-retracted access; hashing keeps the deliberate-act principle intact for
// passive log feeds. Operators investigating match rates query explicitly.
func hashURI(uri string) string {
	sum := sha256.Sum256([]byte(uri))
	return hex.EncodeToString(sum[:])[:16]
}

// findRetractedMatches returns retracted leaf nodes in the given category whose
// embedding similarity to candidateText is above threshold. Returns all matches
// (not just the best), so multi-match aggregation is possible at the caller.
//
// The reason content of matched nodes is intentionally NOT consulted or returned —
// callers receive URIs only and must use --include-retracted to fetch reasons.
func (e *Engine) findRetractedMatches(ctx context.Context, candidateText, category string, threshold float64) ([]store.MemNode, error) {
	if e.Embedder == nil || candidateText == "" {
		return nil, nil
	}

	candidateVec, err := e.Embedder.Embed(ctx, candidateText)
	if err != nil {
		return nil, fmt.Errorf("embed candidate: %w", err)
	}

	vectors, err := e.DB.AllVectors()
	if err != nil {
		return nil, fmt.Errorf("load vectors: %w", err)
	}
	if len(vectors) == 0 {
		return nil, nil
	}

	nodeIDs := make([]int64, len(vectors))
	for i, v := range vectors {
		nodeIDs[i] = v.NodeID
	}
	nodes, err := e.DB.GetNodesByIDs(nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("get nodes: %w", err)
	}
	nodeMap := make(map[int64]store.MemNode, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}

	var matches []store.MemNode
	for _, v := range vectors {
		node, ok := nodeMap[v.NodeID]
		if !ok || node.NodeType != "leaf" || node.Category != category {
			continue
		}
		if !node.IsRetracted() {
			continue
		}
		sim := CosineSimilarity(candidateVec, v.Embedding)
		if sim >= threshold {
			matches = append(matches, node)
		}
	}
	return matches, nil
}

// RetractedMatchError signals that a candidate write matches one or more retracted
// memories. The caller (CLI/HTTP handler) surfaces the URIs to the agent without
// exposing the retraction reasons; the agent fetches reasons deliberately via
// `continuity show <uri> --include-retracted`.
//
// To proceed past the gate, set AcknowledgeRetracted on the input.
type RetractedMatchError struct {
	MatchedURIs []string
}

func (e *RetractedMatchError) Error() string {
	if len(e.MatchedURIs) == 1 {
		return fmt.Sprintf("candidate matches retracted memory %s; inspect with `continuity show %s --include-retracted` before proceeding", e.MatchedURIs[0], e.MatchedURIs[0])
	}
	return fmt.Sprintf("candidate matches %d retracted memories: %s; inspect each with `continuity show <uri> --include-retracted` before proceeding", len(e.MatchedURIs), strings.Join(e.MatchedURIs, ", "))
}

// IsRetractedMatch returns true and the matched URIs if err is a RetractedMatchError.
func IsRetractedMatch(err error) (bool, []string) {
	if rme, ok := err.(*RetractedMatchError); ok {
		return true, rme.MatchedURIs
	}
	return false, nil
}
