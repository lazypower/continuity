package store

import (
	"database/sql"
	"fmt"
	"time"
)

// systemOwnedURIs are nodes that continuity synthesizes itself and that
// participate in invariants beyond user memory (session bootstrap, context
// injection contracts). They are non-retractable via the public verb —
// the operator must SQL-edit if they need to be cleared, accepting the
// friction as the safety mechanism.
//
// Rule: any URI synthesized by the system whose absence would silently break
// downstream contracts belongs here. Add new entries deliberately when
// synthesizing new system-owned nodes.
var systemOwnedURIs = map[string]bool{
	"mem://user/profile/communication": true, // synthesized relational profile; bootstraps session context
}

// RetractNode marks a memory as retracted. The node remains in the database
// (tombstone, not delete) but is excluded from default reads.
//
// Returns (newly bool, err error). newly is true when this call performed the
// retraction; false when the memory was already retracted (idempotent — the
// act has already happened, the original timestamp and reason are preserved).
//
// supersededBy may be empty (pure tombstone) or a URI that supersedes this
// memory (supersession). When non-empty, the successor URI must already exist.
//
// Refuses to retract directory nodes (node_type='dir') and system-owned URIs
// (see systemOwnedURIs) — both shapes have semantics that retraction would
// silently corrupt.
func (db *DB) RetractNode(uri, reason, supersededBy string) (newly bool, err error) {
	if uri == "" {
		return false, fmt.Errorf("uri required")
	}
	if reason == "" {
		return false, fmt.Errorf("reason required")
	}
	if systemOwnedURIs[uri] {
		return false, fmt.Errorf("system-owned: %s cannot be retracted via the public verb", uri)
	}

	target, err := db.GetNodeByURI(uri)
	if err != nil {
		return false, fmt.Errorf("look up target: %w", err)
	}
	if target == nil {
		return false, fmt.Errorf("memory not found: %s", uri)
	}
	if target.NodeType != "leaf" {
		return false, fmt.Errorf("cannot retract %s node: %s (only leaf memories are retractable)", target.NodeType, uri)
	}

	if target.IsRetracted() {
		return false, nil
	}

	if supersededBy != "" {
		if supersededBy == uri {
			return false, fmt.Errorf("self-supersession: %s cannot supersede itself", uri)
		}
		successor, err := db.GetNodeByURI(supersededBy)
		if err != nil {
			return false, fmt.Errorf("look up successor: %w", err)
		}
		if successor == nil {
			return false, fmt.Errorf("successor not found: %s", supersededBy)
		}
	}

	now := time.Now().UnixMilli()
	_, err = db.Exec(`
		UPDATE mem_nodes
		SET tombstoned_at = ?, tombstone_reason = ?, superseded_by = NULLIF(?, ''), updated_at = ?
		WHERE uri = ?
	`, now, reason, supersededBy, now, uri)
	if err != nil {
		return false, fmt.Errorf("retract node: %w", err)
	}
	return true, nil
}

// IsRetracted reports whether the memory at the given URI has been retracted.
// Returns false if the URI does not exist (not an error).
func (db *DB) IsRetracted(uri string) (bool, error) {
	var tombstonedAt sql.NullInt64
	err := db.QueryRow("SELECT tombstoned_at FROM mem_nodes WHERE uri = ?", uri).Scan(&tombstonedAt)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check retraction: %w", err)
	}
	return tombstonedAt.Valid, nil
}

// FindByCategoryIncludingRetracted returns all leaf nodes for a category,
// including retracted ones. For the --include-retracted inspection path.
func (db *DB) FindByCategoryIncludingRetracted(category string) ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at,
			tombstoned_at, tombstone_reason, superseded_by
		FROM mem_nodes WHERE category = ? AND node_type = 'leaf'
		ORDER BY relevance DESC
	`, category)
	if err != nil {
		return nil, fmt.Errorf("find by category (incl retracted): %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// ListLeavesIncludingRetracted returns all leaf nodes including retracted ones.
// For the --include-retracted inspection path.
func (db *DB) ListLeavesIncludingRetracted() ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at,
			tombstoned_at, tombstone_reason, superseded_by
		FROM mem_nodes WHERE node_type = 'leaf'
		ORDER BY relevance DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list leaves (incl retracted): %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// GetChildrenIncludingRetracted returns all direct children of a parent URI,
// including retracted ones. For the --include-retracted inspection path.
func (db *DB) GetChildrenIncludingRetracted(parentURI string) ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at,
			tombstoned_at, tombstone_reason, superseded_by
		FROM mem_nodes WHERE parent_uri = ?
		ORDER BY uri
	`, parentURI)
	if err != nil {
		return nil, fmt.Errorf("get children (incl retracted): %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}
