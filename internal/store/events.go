package store

import (
	"fmt"
	"time"
)

// MemEvent is one row in the surfacing observation journal (ADR-001 §5).
// Append-only telemetry about exposure and use — never node state. The journal
// is the single authority on shown-vs-used; mem_nodes.access_count is frozen
// legacy. Valid event names are fixed by the ADR vocabulary and enforced by a
// schema CHECK: 'shown', 'deepened' (this slice); 'attributed', 're-taught',
// 'retrieval-miss' (staged, session-end extractor). Adding a name is a
// deliberate act that takes a migration.
type MemEvent struct {
	ID        int64
	NodeURI   string
	Event     string // 'shown' | 'deepened' (staged: 'attributed' | 're-taught' | 'retrieval-miss')
	Surface   string // for 'shown': 'tray' | 'moments' | 'search' | 'gate'; empty otherwise
	SessionID string
	CreatedAt int64
}

// InsertEvent appends one surfacing event. Read-path callers must not call
// this inline — ADR-001 §5: telemetry may lose an event, a surfacing never
// waits on one. The server wraps this in a buffered fire-and-forget recorder;
// only intentional-op paths (CLI verbs, tests) insert synchronously.
func (db *DB) InsertEvent(e MemEvent) error {
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().UnixMilli()
	}
	_, err := db.Exec(`
		INSERT INTO mem_events (node_uri, event, surface, session_id, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, e.NodeURI, e.Event, e.Surface, e.SessionID, e.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// EventsByURI returns a node's events, newest first. Inspection/test helper —
// not a ranking input (ranking derives from used-given-shown only after the
// §5 research window, and from aggregates, not raw rows).
func (db *DB) EventsByURI(uri string) ([]MemEvent, error) {
	rows, err := db.Query(`
		SELECT id, node_uri, event, surface, session_id, created_at
		FROM mem_events WHERE node_uri = ? ORDER BY created_at DESC, id DESC
	`, uri)
	if err != nil {
		return nil, fmt.Errorf("events by uri: %w", err)
	}
	defer rows.Close()

	var events []MemEvent
	for rows.Next() {
		var e MemEvent
		if err := rows.Scan(&e.ID, &e.NodeURI, &e.Event, &e.Surface, &e.SessionID, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// ShownURIsForSession returns the set of node URIs already journaled as
// `shown` to the given session, across every surface (tray, index, moments,
// search, gate). This is the durable half of the prompt gate's dedupe ledger
// (#80): a URI the session has already seen must not be injected again.
// Served by idx_events_session — the gate reads this on the synchronous
// prompt path, so it must not scan.
func (db *DB) ShownURIsForSession(sessionID string) (map[string]bool, error) {
	if sessionID == "" {
		return map[string]bool{}, nil
	}
	rows, err := db.Query(`
		SELECT DISTINCT node_uri FROM mem_events
		WHERE session_id = ? AND event = 'shown'
	`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("shown uris for session: %w", err)
	}
	defer rows.Close()

	uris := make(map[string]bool)
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("scan shown uri: %w", err)
		}
		uris[u] = true
	}
	return uris, rows.Err()
}

// CountEvents returns how many events match the given event name ("" = all).
// Inspection/test helper.
func (db *DB) CountEvents(event string) (int, error) {
	var n int
	var err error
	if event == "" {
		err = db.QueryRow(`SELECT COUNT(*) FROM mem_events`).Scan(&n)
	} else {
		err = db.QueryRow(`SELECT COUNT(*) FROM mem_events WHERE event = ?`, event).Scan(&n)
	}
	return n, err
}
