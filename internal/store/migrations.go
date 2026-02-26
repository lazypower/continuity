package store

import (
	"fmt"
)

type migration struct {
	Version     int
	Description string
	SQL         string
}

var migrations = []migration{
	{
		Version:     1,
		Description: "mem_nodes: virtual filesystem for memory tree",
		SQL: `
CREATE TABLE mem_nodes (
    id             INTEGER PRIMARY KEY,
    uri            TEXT NOT NULL UNIQUE,
    parent_uri     TEXT,
    node_type      TEXT NOT NULL CHECK (node_type IN ('dir', 'leaf')),
    category       TEXT NOT NULL CHECK (category IN ('profile', 'preferences', 'entities', 'events', 'patterns', 'cases', 'session')),

    -- Three-tier content
    l0_abstract    TEXT,
    l1_overview    TEXT,
    l2_content     TEXT,

    -- Merge control
    mergeable      INTEGER NOT NULL DEFAULT 0,
    merged_from    TEXT,

    -- Decay
    relevance      REAL NOT NULL DEFAULT 1.0,
    last_access    INTEGER,
    access_count   INTEGER NOT NULL DEFAULT 0,

    -- Metadata
    source_session TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,

    FOREIGN KEY (parent_uri) REFERENCES mem_nodes(uri)
);

CREATE INDEX idx_nodes_parent    ON mem_nodes(parent_uri);
CREATE INDEX idx_nodes_category  ON mem_nodes(category);
CREATE INDEX idx_nodes_relevance ON mem_nodes(relevance DESC);
`,
	},
	{
		Version:     2,
		Description: "sessions: session tracking",
		SQL: `
CREATE TABLE sessions (
    id             INTEGER PRIMARY KEY,
    session_id     TEXT NOT NULL UNIQUE,
    project        TEXT,
    started_at     INTEGER NOT NULL,
    ended_at       INTEGER,
    status         TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'completed', 'failed')),
    summary_node   INTEGER,
    message_count  INTEGER NOT NULL DEFAULT 0,
    tool_count     INTEGER NOT NULL DEFAULT 0,

    FOREIGN KEY (summary_node) REFERENCES mem_nodes(id)
);

CREATE INDEX idx_sessions_status     ON sessions(status);
CREATE INDEX idx_sessions_started_at ON sessions(started_at DESC);
CREATE INDEX idx_sessions_project    ON sessions(project);
`,
	},
	{
		Version:     3,
		Description: "observations: tool use tracking per session",
		SQL: `
CREATE TABLE observations (
    id             INTEGER PRIMARY KEY,
    session_id     TEXT NOT NULL,
    tool_name      TEXT,
    tool_input     TEXT,
    tool_response  TEXT,
    created_at     INTEGER NOT NULL
);

CREATE INDEX idx_obs_session ON observations(session_id);
CREATE INDEX idx_obs_created ON observations(created_at DESC);
`,
	},
	{
		Version:     4,
		Description: "mem_vectors: embedding vectors for semantic search",
		SQL: `
CREATE TABLE mem_vectors (
    node_id    INTEGER PRIMARY KEY,
    embedding  BLOB NOT NULL,
    model      TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (node_id) REFERENCES mem_nodes(id) ON DELETE CASCADE
);
`,
	},
}

func (db *DB) migrate() error {
	// Create schema_versions table if it doesn't exist
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_versions (
			version     INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at  INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_versions: %w", err)
	}

	for _, m := range migrations {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM schema_versions WHERE version = ?", m.Version).Scan(&count)
		if err != nil {
			return fmt.Errorf("check migration %d: %w", m.Version, err)
		}
		if count > 0 {
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.Version, err)
		}

		if _, err := tx.Exec(m.SQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d (%s): %w", m.Version, m.Description, err)
		}

		if _, err := tx.Exec(
			"INSERT INTO schema_versions (version, description) VALUES (?, ?)",
			m.Version, m.Description,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %d: %w", m.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}
	}

	return nil
}

// SchemaVersion returns the current schema version.
func (db *DB) SchemaVersion() (int, error) {
	var version int
	err := db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_versions").Scan(&version)
	return version, err
}
