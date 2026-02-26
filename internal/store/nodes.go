package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// textNearIdentical returns true if two strings are >95% similar by character overlap.
// Uses a simple normalized edit-distance-like metric: shared bigram ratio.
// This is intentionally cheap — no embeddings needed at the store layer.
func textNearIdentical(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == b {
		return true
	}
	if a == "" || b == "" {
		return false
	}

	// Bigram overlap as a quick similarity proxy
	bigramsA := bigrams(a)
	bigramsB := bigrams(b)
	if len(bigramsA) == 0 || len(bigramsB) == 0 {
		return a == b
	}

	shared := 0
	for bg := range bigramsA {
		if bigramsB[bg] {
			shared++
		}
	}

	union := len(bigramsA) + len(bigramsB) - shared
	if union == 0 {
		return true
	}

	similarity := float64(shared) / float64(union) // Jaccard index
	return similarity > 0.95
}

func bigrams(s string) map[string]bool {
	if len(s) < 2 {
		return nil
	}
	m := make(map[string]bool, len(s)-1)
	for i := 0; i < len(s)-1; i++ {
		m[s[i:i+2]] = true
	}
	return m
}

// MemNode represents a node in the memory tree.
type MemNode struct {
	ID            int64
	URI           string
	ParentURI     string
	NodeType      string // "dir" or "leaf"
	Category      string // profile, preferences, entities, events, patterns, cases, session
	L0Abstract    string
	L1Overview    string
	L2Content     string
	Mergeable     bool
	MergedFrom    string // JSON array of source node IDs
	Relevance     float64
	LastAccess    *int64
	AccessCount   int
	SourceSession string
	CreatedAt     int64
	UpdatedAt     int64
}

// mergeableCategories defines which categories support in-place merging.
var mergeableCategories = map[string]bool{
	"profile":     true,
	"preferences": true,
	"patterns":    true,
}

// CreateNode inserts a new mem_node. Sets mergeable based on category.
// Automatically ensures parent directory nodes exist.
func (db *DB) CreateNode(node *MemNode) error {
	now := time.Now().UnixMilli()
	mergeable := 0
	if mergeableCategories[node.Category] {
		mergeable = 1
	}

	// Ensure parent directories exist
	if err := db.EnsureParentDirs(node.URI, node.Category); err != nil {
		return fmt.Errorf("ensure parents: %w", err)
	}

	// Derive parent_uri from the URI
	parentURI := parentURIOf(node.URI)

	result, err := db.Exec(`
		INSERT INTO mem_nodes (uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at)
		VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, node.URI, parentURI, node.NodeType, node.Category,
		node.L0Abstract, node.L1Overview, node.L2Content,
		mergeable, node.MergedFrom,
		1.0, now, 0, node.SourceSession, now, now)
	if err != nil {
		return fmt.Errorf("create node: %w", err)
	}

	id, _ := result.LastInsertId()
	node.ID = id
	node.Mergeable = mergeableCategories[node.Category]
	node.Relevance = 1.0
	node.CreatedAt = now
	node.UpdatedAt = now
	return nil
}

// GetNodeByURI returns a node by its URI, or nil if not found.
func (db *DB) GetNodeByURI(uri string) (*MemNode, error) {
	var n MemNode
	var mergeable int
	var lastAccess sql.NullInt64
	var parentURI, l0, l1, l2, mergedFrom, sourceSession sql.NullString
	err := db.QueryRow(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE uri = ?
	`, uri).Scan(&n.ID, &n.URI, &parentURI, &n.NodeType, &n.Category,
		&l0, &l1, &l2,
		&mergeable, &mergedFrom, &n.Relevance, &lastAccess, &n.AccessCount,
		&sourceSession, &n.CreatedAt, &n.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get node by uri: %w", err)
	}
	n.ParentURI = parentURI.String
	n.L0Abstract = l0.String
	n.L1Overview = l1.String
	n.L2Content = l2.String
	n.MergedFrom = mergedFrom.String
	n.SourceSession = sourceSession.String
	n.Mergeable = mergeable != 0
	if lastAccess.Valid {
		n.LastAccess = &lastAccess.Int64
	}
	return &n, nil
}

// UpdateNode updates a node's content tiers and updated_at.
func (db *DB) UpdateNode(node *MemNode) error {
	now := time.Now().UnixMilli()
	_, err := db.Exec(`
		UPDATE mem_nodes SET l0_abstract = ?, l1_overview = ?, l2_content = ?,
			merged_from = ?, source_session = ?, updated_at = ?
		WHERE id = ?
	`, node.L0Abstract, node.L1Overview, node.L2Content,
		node.MergedFrom, node.SourceSession, now, node.ID)
	if err != nil {
		return fmt.Errorf("update node: %w", err)
	}
	node.UpdatedAt = now
	return nil
}

// UpsertNode creates a new node or merges into an existing one.
// For mergeable categories, updates in place. For immutable, creates new.
func (db *DB) UpsertNode(node *MemNode) error {
	existing, err := db.GetNodeByURI(node.URI)
	if err != nil {
		return err
	}

	if existing == nil {
		return db.CreateNode(node)
	}

	if existing.Mergeable {
		// Skip if new content is near-identical to existing (avoid churn)
		if textNearIdentical(existing.L1Overview, node.L1Overview) &&
			textNearIdentical(existing.L0Abstract, node.L0Abstract) {
			return nil
		}
		existing.L0Abstract = node.L0Abstract
		existing.L1Overview = node.L1Overview
		existing.L2Content = node.L2Content
		existing.SourceSession = node.SourceSession
		return db.UpdateNode(existing)
	}

	// Immutable — create as new node with deduplicated URI
	node.URI = fmt.Sprintf("%s-%d", node.URI, time.Now().UnixMilli())
	return db.CreateNode(node)
}

// FindByCategory returns all leaf nodes for a given category, ordered by relevance DESC.
func (db *DB) FindByCategory(category string) ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE category = ? AND node_type = 'leaf'
		ORDER BY relevance DESC
	`, category)
	if err != nil {
		return nil, fmt.Errorf("find by category: %w", err)
	}
	defer rows.Close()

	return scanNodes(rows)
}

// ListLeaves returns all leaf nodes ordered by relevance DESC.
func (db *DB) ListLeaves() ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE node_type = 'leaf'
		ORDER BY relevance DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list leaves: %w", err)
	}
	defer rows.Close()

	return scanNodes(rows)
}

// TouchNode updates last_access and increments access_count (retrieval boost).
func (db *DB) TouchNode(uri string) error {
	now := time.Now().UnixMilli()
	_, err := db.Exec(`
		UPDATE mem_nodes SET last_access = ?, access_count = access_count + 1, relevance = 1.0
		WHERE uri = ?
	`, now, uri)
	if err != nil {
		return fmt.Errorf("touch node: %w", err)
	}
	return nil
}

// DecayAllNodes applies time-based decay to all non-exempt nodes.
// 90-day half-life, floor of 0.1. Profile nodes are exempt.
func (db *DB) DecayAllNodes() (int, error) {
	// Fetch all decayable nodes
	rows, err := db.Query(`
		SELECT id, uri, relevance, last_access, created_at
		FROM mem_nodes
		WHERE node_type = 'leaf' AND uri != 'mem://user/profile/communication'
	`)
	if err != nil {
		return 0, fmt.Errorf("query decayable nodes: %w", err)
	}
	defer rows.Close()

	type decayTarget struct {
		id         int64
		relevance  float64
		lastAccess *int64
		createdAt  int64
	}

	var targets []decayTarget
	for rows.Next() {
		var t decayTarget
		var lastAccess sql.NullInt64
		if err := rows.Scan(&t.id, new(string), &t.relevance, &lastAccess, &t.createdAt); err != nil {
			return 0, fmt.Errorf("scan decay target: %w", err)
		}
		if lastAccess.Valid {
			t.lastAccess = &lastAccess.Int64
		}
		targets = append(targets, t)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	now := time.Now().UnixMilli()
	halfLifeMs := float64(90 * 24 * 60 * 60 * 1000) // 90 days in ms
	updated := 0

	for _, t := range targets {
		refTime := t.createdAt
		if t.lastAccess != nil {
			refTime = *t.lastAccess
		}

		elapsed := float64(now - refTime)
		if elapsed <= 0 {
			continue
		}

		// decay = 0.5 ^ (elapsed / halfLife)
		decay := pow05(elapsed / halfLifeMs)
		newRelevance := decay
		if newRelevance < 0.1 {
			newRelevance = 0.1
		}
		if newRelevance >= t.relevance {
			continue // relevance can only decrease via decay
		}

		if _, err := db.Exec(`UPDATE mem_nodes SET relevance = ? WHERE id = ?`, newRelevance, t.id); err != nil {
			return updated, fmt.Errorf("update decay: %w", err)
		}
		updated++
	}

	return updated, nil
}

// pow05 computes 0.5^x using repeated squaring approach.
// This avoids importing math for a single function.
func pow05(x float64) float64 {
	// 0.5^x = exp(x * ln(0.5)) = exp(-x * ln(2))
	// Use the identity: 0.5^x = 1 / 2^x
	// Approximate using exp(-x * 0.693147...)
	ln2 := 0.6931471805599453
	return exp(-x * ln2)
}

// exp approximates e^x using the Taylor series, good enough for decay calculations.
func exp(x float64) float64 {
	if x > 700 {
		return 1e308
	}
	if x < -700 {
		return 0
	}

	// Reduce to |x| < 1 using e^x = (e^(x/n))^n
	n := 1
	for x > 1 || x < -1 {
		x /= 2
		n *= 2
	}

	// Taylor series: e^x ≈ 1 + x + x²/2! + x³/3! + ...
	result := 1.0
	term := 1.0
	for i := 1; i <= 20; i++ {
		term *= x / float64(i)
		result += term
	}

	// Square n times
	for n > 1 {
		result *= result
		n /= 2
	}

	return result
}

// EnsureParentDirs creates directory nodes for a given leaf URI.
// e.g., for "mem://user/profile/coding-style", ensures "mem://user" and "mem://user/profile" exist.
func (db *DB) EnsureParentDirs(uri, category string) error {
	segments := uriSegments(uri) // ["user", "profile", "coding-style"]
	if len(segments) <= 1 {
		return nil // top-level URI, no parents needed
	}

	// Build parent directories from root to leaf's parent
	for i := 1; i < len(segments); i++ {
		dirURI := "mem://" + joinParts(segments[:i])
		var parentURI *string
		if i > 1 {
			p := "mem://" + joinParts(segments[:i-1])
			parentURI = &p
		}

		existing, err := db.GetNodeByURI(dirURI)
		if err != nil {
			return err
		}
		if existing != nil {
			continue
		}

		now := time.Now().UnixMilli()
		_, err = db.Exec(`
			INSERT OR IGNORE INTO mem_nodes (uri, parent_uri, node_type, category, relevance, created_at, updated_at)
			VALUES (?, ?, 'dir', ?, 1.0, ?, ?)
		`, dirURI, parentURI, category, now, now)
		if err != nil {
			return fmt.Errorf("create parent dir %s: %w", dirURI, err)
		}
	}
	return nil
}

// uriSegments extracts the path segments from a mem:// URI.
// "mem://user/profile/coding-style" → ["user", "profile", "coding-style"]
func uriSegments(uri string) []string {
	// Strip the "mem://" prefix
	const prefix = "mem://"
	if len(uri) <= len(prefix) {
		return nil
	}
	path := uri[len(prefix):]
	// Split on "/"
	var segments []string
	for _, s := range splitSimple(path, '/') {
		if s != "" {
			segments = append(segments, s)
		}
	}
	return segments
}

// splitSimple splits a string on a single byte delimiter.
func splitSimple(s string, sep byte) []string {
	var result []string
	current := ""
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			result = append(result, current)
			current = ""
		} else {
			current += string(s[i])
		}
	}
	result = append(result, current)
	return result
}

// joinParts joins parts with "/".
func joinParts(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "/"
		}
		result += p
	}
	return result
}

// parentURI derives the parent URI from a mem:// URI.
// "mem://user/profile/coding-style" → "mem://user/profile"
// "mem://user" → ""
func parentURIOf(uri string) string {
	segments := uriSegments(uri)
	if len(segments) <= 1 {
		return ""
	}
	return "mem://" + joinParts(segments[:len(segments)-1])
}

// GetNodeByID returns a node by its database ID, or nil if not found.
func (db *DB) GetNodeByID(id int64) (*MemNode, error) {
	var n MemNode
	var mergeable int
	var lastAccess sql.NullInt64
	var parentURI, l0, l1, l2, mergedFrom, sourceSession sql.NullString
	err := db.QueryRow(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE id = ?
	`, id).Scan(&n.ID, &n.URI, &parentURI, &n.NodeType, &n.Category,
		&l0, &l1, &l2,
		&mergeable, &mergedFrom, &n.Relevance, &lastAccess, &n.AccessCount,
		&sourceSession, &n.CreatedAt, &n.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get node by id: %w", err)
	}
	n.ParentURI = parentURI.String
	n.L0Abstract = l0.String
	n.L1Overview = l1.String
	n.L2Content = l2.String
	n.MergedFrom = mergedFrom.String
	n.SourceSession = sourceSession.String
	n.Mergeable = mergeable != 0
	if lastAccess.Valid {
		n.LastAccess = &lastAccess.Int64
	}
	return &n, nil
}

// GetChildren returns all direct children of a given parent URI.
func (db *DB) GetChildren(parentURI string) ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE parent_uri = ?
		ORDER BY uri
	`, parentURI)
	if err != nil {
		return nil, fmt.Errorf("get children: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// ListRoots returns all top-level nodes (those with no parent).
func (db *DB) ListRoots() ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE parent_uri IS NULL
		ORDER BY uri
	`)
	if err != nil {
		return nil, fmt.Errorf("list roots: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// GetNodesByIDs returns nodes for the given list of IDs.
func (db *DB) GetNodesByIDs(ids []int64) ([]MemNode, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	// Build placeholder string
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	// Join placeholders with commas
	ph := ""
	for i, p := range placeholders {
		if i > 0 {
			ph += ","
		}
		ph += p
	}

	query := fmt.Sprintf(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at
		FROM mem_nodes WHERE id IN (%s)
	`, ph)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("get nodes by ids: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// DeleteNode removes a node and its associated vector by ID.
func (db *DB) DeleteNode(id int64) error {
	if err := db.DeleteVector(id); err != nil {
		return fmt.Errorf("delete vector for node %d: %w", id, err)
	}
	_, err := db.Exec("DELETE FROM mem_nodes WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete node %d: %w", id, err)
	}
	return nil
}

// DeleteOrphanDirs removes directory nodes that have no children.
func (db *DB) DeleteOrphanDirs() (int, error) {
	result, err := db.Exec(`
		DELETE FROM mem_nodes WHERE node_type = 'dir'
		AND id NOT IN (
			SELECT DISTINCT p.id FROM mem_nodes p
			JOIN mem_nodes c ON c.parent_uri = p.uri
		)
	`)
	if err != nil {
		return 0, fmt.Errorf("delete orphan dirs: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// CountChildren returns the number of direct children for a parent URI.
func (db *DB) CountChildren(parentURI string) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM mem_nodes WHERE parent_uri = ?", parentURI).Scan(&count)
	return count, err
}

func scanNodes(rows *sql.Rows) ([]MemNode, error) {
	var nodes []MemNode
	for rows.Next() {
		var n MemNode
		var mergeable int
		var lastAccess sql.NullInt64
		var parentURI, l0, l1, l2, mergedFrom, sourceSession sql.NullString
		if err := rows.Scan(&n.ID, &n.URI, &parentURI, &n.NodeType, &n.Category,
			&l0, &l1, &l2,
			&mergeable, &mergedFrom, &n.Relevance, &lastAccess, &n.AccessCount,
			&sourceSession, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		n.ParentURI = parentURI.String
		n.L0Abstract = l0.String
		n.L1Overview = l1.String
		n.L2Content = l2.String
		n.MergedFrom = mergedFrom.String
		n.SourceSession = sourceSession.String
		n.Mergeable = mergeable != 0
		if lastAccess.Valid {
			n.LastAccess = &lastAccess.Int64
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}
