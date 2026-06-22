package store

import (
	"fmt"
	"time"
)

// PinValidationError signals that a pin/unpin was rejected for a user/domain
// reason (memory not found, target is a directory, target is retracted) rather
// than an internal failure. Its Message describes the caller's own input — no
// SQL, no filesystem paths — so the boundary layer may surface it verbatim.
//
// store cannot import engine (engine imports store, not vice versa), so engine
// detects this type via errors.As and re-wraps it as an engine.ValidationError
// to reuse the existing HTTP-400 classification path. Mirrors RetractValidationError.
type PinValidationError struct {
	Message string
}

func (e *PinValidationError) Error() string {
	return e.Message
}

func pinValidationErrorf(format string, args ...any) error {
	return &PinValidationError{Message: fmt.Sprintf(format, args...)}
}

// PinNode marks a memory as an operator-declared pin by stamping pinned_at.
// A pin is the "declared half" of the operating contract: it forces the memory
// into the cold-boot injection window's dedicated Pinned section regardless of
// recency or relevance.
//
// Returns (newly bool, err error). newly is true when this call performed the
// pin; false when the memory was already pinned (idempotent — the original
// pinned_at timestamp is preserved so pin ordering is stable).
//
// Refuses to pin directory nodes (only leaf memories are pinnable) and retracted
// nodes (a retraction is an unlearning; pinning retracted content would be a
// contradiction, and the read paths exclude it anyway — fail closed at write).
func (db *DB) PinNode(uri string) (newly bool, err error) {
	if uri == "" {
		return false, pinValidationErrorf("uri required")
	}

	target, err := db.GetNodeByURI(uri)
	if err != nil {
		return false, fmt.Errorf("look up target: %w", err)
	}
	if target == nil {
		return false, pinValidationErrorf("memory not found: %s", uri)
	}
	if target.NodeType != "leaf" {
		return false, pinValidationErrorf("cannot pin %s node: %s (only leaf memories are pinnable)", target.NodeType, uri)
	}
	if target.IsRetracted() {
		return false, pinValidationErrorf("cannot pin retracted memory: %s (retraction is an unlearning; un-retract first if this was a mistake)", uri)
	}

	if target.IsPinned() {
		return false, nil
	}

	now := time.Now().UnixMilli()
	// Guard against a retraction landing between the read above and this write:
	// only stamp pinned_at on a still-live row. If 0 rows change, the node was
	// retracted in the race — report the refusal rather than pin a tombstone.
	res, err := db.Exec(`
		UPDATE mem_nodes SET pinned_at = ?, updated_at = ?
		WHERE uri = ? AND tombstoned_at IS NULL
	`, now, now, uri)
	if err != nil {
		return false, fmt.Errorf("pin node: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, pinValidationErrorf("cannot pin retracted memory: %s", uri)
	}
	return true, nil
}

// UnpinNode clears pinned_at on a memory, removing it from the Pinned section.
//
// Returns (newly bool, err error). newly is true when this call performed the
// unpin; false when the memory was not pinned (idempotent). A node that does not
// exist is reported as a validation error (the operator named a URI that isn't there).
func (db *DB) UnpinNode(uri string) (newly bool, err error) {
	if uri == "" {
		return false, pinValidationErrorf("uri required")
	}

	target, err := db.GetNodeByURI(uri)
	if err != nil {
		return false, fmt.Errorf("look up target: %w", err)
	}
	if target == nil {
		return false, pinValidationErrorf("memory not found: %s", uri)
	}
	if !target.IsPinned() {
		return false, nil
	}

	now := time.Now().UnixMilli()
	if _, err := db.Exec(`
		UPDATE mem_nodes SET pinned_at = NULL, updated_at = ?
		WHERE uri = ?
	`, now, uri); err != nil {
		return false, fmt.Errorf("unpin node: %w", err)
	}
	return true, nil
}

// ListPinned returns live (non-retracted) pinned leaf nodes, oldest pin first.
//
// Retraction exclusion is the load-bearing safety property: a memory that was
// pinned and later retracted MUST NOT inject. tombstoned_at IS NULL here is the
// single chokepoint that guarantees a pinned-then-retracted node goes silent,
// matching the contract honored by every other default read path.
func (db *DB) ListPinned() ([]MemNode, error) {
	rows, err := db.Query(`
		SELECT id, uri, parent_uri, node_type, category, l0_abstract, l1_overview, l2_content,
			mergeable, merged_from, relevance, last_access, access_count, source_session, created_at, updated_at,
			tombstoned_at, tombstone_reason, superseded_by, pinned_at
		FROM mem_nodes
		WHERE pinned_at IS NOT NULL AND tombstoned_at IS NULL AND node_type = 'leaf'
		ORDER BY pinned_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list pinned: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}
