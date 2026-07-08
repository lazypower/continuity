package store

import "fmt"

// gcCandidateWhere is the single predicate that defines "genuine dead weight":
// a live (non-retracted), unpinned, decayable leaf that has decayed to the floor
// AND gone unretrieved since the cutoff. Retracted receipts (tombstoned) are
// never targeted — they're tiny and their vector is the load-bearing resurrection
// antibody. Pins and the decay-exempt categories (contract + moments) are never
// targeted either. Shared by GCCandidates and CountGCCandidates so the sweep and
// the health count can never diverge.
const gcCandidateWhere = `
	node_type = 'leaf'
	AND tombstoned_at IS NULL
	AND pinned_at IS NULL
	AND category NOT IN (` + decayExemptCategoriesSQL + `)
	AND relevance <= ?
	AND COALESCE(last_access, created_at) <= ?`

// GCCandidates returns up to limit dead-weight leaves (see gcCandidateWhere),
// oldest-first, for the GC sweep to reclaim. floorThreshold is the decay floor;
// unusedBeforeMs is (now - min-age).
func (db *DB) GCCandidates(floorThreshold float64, unusedBeforeMs int64, limit int) ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at,
			tombstoned_at, tombstone_reason, superseded_by, pinned_at
		FROM mem_nodes
		WHERE `+gcCandidateWhere+`
		ORDER BY COALESCE(last_access, created_at) ASC
		LIMIT ?
	`, floorThreshold, unusedBeforeMs, limit)
	if err != nil {
		return nil, fmt.Errorf("gc candidates: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// CountGCCandidates counts the reclaimable dead weight (same predicate, no limit)
// so a growing pile is visible in /api/health rather than only in sweep logs.
func (db *DB) CountGCCandidates(floorThreshold float64, unusedBeforeMs int64) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE `+gcCandidateWhere,
		floorThreshold, unusedBeforeMs).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count gc candidates: %w", err)
	}
	return n, nil
}
