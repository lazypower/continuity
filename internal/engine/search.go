package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// SearchResult represents a single search result.
type SearchResult struct {
	Node       store.MemNode `json:"node"`
	Score      float64       `json:"score"`
	Similarity float64       `json:"similarity"`
}

// SearchOpts controls search behavior.
type SearchOpts struct {
	Limit    int    // max results (default 10)
	Category string // filter by category (empty = all)
}

func (o SearchOpts) limit() int {
	if o.Limit <= 0 {
		return 10
	}
	return o.Limit
}

// Find performs fast vector search without LLM assistance.
// Score = similarity * relevance.
func Find(ctx context.Context, db *store.DB, embedder Embedder, query string, opts SearchOpts) ([]SearchResult, error) {
	if embedder == nil {
		return nil, fmt.Errorf("no embedder configured")
	}

	// Embed the query
	queryVec, err := embedder.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	// Load all vectors
	vectors, err := db.AllVectors()
	if err != nil {
		return nil, fmt.Errorf("load vectors: %w", err)
	}

	if len(vectors) == 0 {
		return nil, nil
	}

	// Build node ID set for quick lookup
	nodeIDs := make([]int64, len(vectors))
	for i, v := range vectors {
		nodeIDs[i] = v.NodeID
	}

	// Fetch all nodes for these IDs
	nodes, err := db.GetNodesByIDs(nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("get nodes: %w", err)
	}
	nodeMap := make(map[int64]store.MemNode, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}

	// Score each vector
	var results []SearchResult
	for _, v := range vectors {
		node, ok := nodeMap[v.NodeID]
		if !ok {
			continue
		}
		// Filter by category if specified
		if opts.Category != "" && node.Category != opts.Category {
			continue
		}
		// Only score leaf nodes
		if node.NodeType != "leaf" {
			continue
		}

		similarity := CosineSimilarity(queryVec, v.Embedding)
		score := similarity * node.Relevance

		if score > 0 {
			results = append(results, SearchResult{
				Node:       node,
				Score:      score,
				Similarity: similarity,
			})
		}
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit results
	limit := opts.limit()
	if len(results) > limit {
		results = results[:limit]
	}

	// Touch accessed nodes (retrieval boost)
	for _, r := range results {
		db.TouchNode(r.Node.URI)
	}

	return results, nil
}

// subQuery represents a decomposed search intent.
type subQuery struct {
	Query string `json:"query"`
	Type  string `json:"type"` // MEMORY, RESOURCE, PATTERN
}

// Search performs LLM-assisted search with intent decomposition.
// Score = 0.5*similarity + 0.3*relevance + 0.2*parentScore.
func Search(ctx context.Context, db *store.DB, embedder Embedder, client llm.Client, query string, opts SearchOpts) ([]SearchResult, error) {
	if client == nil {
		// Fall back to Find() if no LLM available
		return Find(ctx, db, embedder, query, opts)
	}

	// Decompose query into sub-queries
	prompt := llm.SearchIntentPrompt(query)
	resp, err := client.Complete(ctx, prompt)
	if err != nil {
		log.Printf("search intent decomposition failed, falling back to find: %v", err)
		return Find(ctx, db, embedder, query, opts)
	}

	subQueries := parseSubQueries(resp.Content)
	if len(subQueries) == 0 {
		// If decomposition returns nothing useful, search the original query
		subQueries = []subQuery{{Query: query, Type: "MEMORY"}}
	}

	// Run Find() for each sub-query with expanded limit
	expandedOpts := SearchOpts{
		Limit:    opts.limit() * 3,
		Category: opts.Category,
	}

	// Collect all results across sub-queries, deduplicate by node ID (max score wins)
	seen := make(map[int64]SearchResult)
	for _, sq := range subQueries {
		results, err := Find(ctx, db, embedder, sq.Query, expandedOpts)
		if err != nil {
			log.Printf("sub-query find failed for %q: %v", sq.Query, err)
			continue
		}
		for _, r := range results {
			existing, exists := seen[r.Node.ID]
			if !exists || r.Score > existing.Score {
				seen[r.Node.ID] = r
			}
		}
	}

	// Build parent score map for tree-aware scoring
	parentScores := buildParentScores(db, seen)

	// Re-score with full formula: 0.5*similarity + 0.3*relevance + 0.2*parentScore
	var results []SearchResult
	for _, r := range seen {
		ps := parentScores[r.Node.ParentURI]
		r.Score = 0.5*r.Similarity + 0.3*r.Node.Relevance + 0.2*ps
		results = append(results, r)
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit results
	limit := opts.limit()
	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// buildParentScores computes average similarity of sibling nodes for tree-aware scoring.
func buildParentScores(db *store.DB, results map[int64]SearchResult) map[string]float64 {
	parentScores := make(map[string]float64)
	parentCounts := make(map[string]int)

	for _, r := range results {
		if r.Node.ParentURI != "" {
			parentScores[r.Node.ParentURI] += r.Similarity
			parentCounts[r.Node.ParentURI]++
		}
	}

	for uri := range parentScores {
		if parentCounts[uri] > 0 {
			parentScores[uri] /= float64(parentCounts[uri])
		}
	}

	return parentScores
}

// parseSubQueries extracts the JSON array of sub-queries from the LLM response.
func parseSubQueries(content string) []subQuery {
	content = strings.TrimSpace(content)

	// Strip markdown code fences
	if strings.HasPrefix(content, "```") {
		lines := strings.Split(content, "\n")
		if len(lines) > 2 {
			content = strings.Join(lines[1:len(lines)-1], "\n")
		}
	}
	content = strings.TrimSpace(content)

	// Find JSON array
	start := strings.Index(content, "[")
	end := strings.LastIndex(content, "]")
	if start < 0 || end < 0 || end <= start {
		return nil
	}

	var queries []subQuery
	if err := json.Unmarshal([]byte(content[start:end+1]), &queries); err != nil {
		return nil
	}

	// Cap at 3
	if len(queries) > 3 {
		queries = queries[:3]
	}
	return queries
}
