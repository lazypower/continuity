package store

import (
	"fmt"
)

type migration struct {
	Version     int
	Description string
	SQL         string
	// Risky marks a migration that performs a destructive, non-reversible
	// rewrite of an existing table (CREATE _new + INSERT SELECT * + DROP +
	// RENAME). A committed-but-logically-wrong risky migration cannot be
	// undone by re-running migrate(), so the presence of a pending risky
	// migration is what triggers an upgrade restore point (see snapshot.go).
	Risky bool
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
	{
		Version:     5,
		Description: "sessions: add extracted_at for idempotent extraction",
		SQL:         `ALTER TABLE sessions ADD COLUMN extracted_at INTEGER;`,
	},
	{
		Version:     6,
		Description: "mem_nodes: add moments category",
		Risky:       true,
		SQL: `
PRAGMA foreign_keys=OFF;

CREATE TABLE mem_nodes_new (
    id             INTEGER PRIMARY KEY,
    uri            TEXT NOT NULL UNIQUE,
    parent_uri     TEXT,
    node_type      TEXT NOT NULL CHECK (node_type IN ('dir', 'leaf')),
    category       TEXT NOT NULL CHECK (category IN ('profile', 'preferences', 'entities', 'events', 'patterns', 'cases', 'moments', 'session')),

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

    FOREIGN KEY (parent_uri) REFERENCES mem_nodes_new(uri)
);

INSERT INTO mem_nodes_new SELECT * FROM mem_nodes;
DROP TABLE mem_nodes;
ALTER TABLE mem_nodes_new RENAME TO mem_nodes;

CREATE INDEX idx_nodes_parent    ON mem_nodes(parent_uri);
CREATE INDEX idx_nodes_category  ON mem_nodes(category);
CREATE INDEX idx_nodes_relevance ON mem_nodes(relevance DESC);

PRAGMA foreign_keys=ON;
`,
	},
	{
		Version:     7,
		Description: "sessions: add tone for session emotional arc",
		SQL:         `ALTER TABLE sessions ADD COLUMN tone TEXT;`,
	},
	{
		Version:     8,
		Description: "mem_nodes: retraction columns for memory accountability (issue #12)",
		SQL: `
ALTER TABLE mem_nodes ADD COLUMN tombstoned_at INTEGER;
ALTER TABLE mem_nodes ADD COLUMN tombstone_reason TEXT;
ALTER TABLE mem_nodes ADD COLUMN superseded_by TEXT;
`,
	},
	{
		Version:     9,
		Description: "mem_nodes: add feedback and reference categories (issue #24)",
		Risky:       true,
		SQL: `
PRAGMA foreign_keys=OFF;

CREATE TABLE mem_nodes_new (
    id             INTEGER PRIMARY KEY,
    uri            TEXT NOT NULL UNIQUE,
    parent_uri     TEXT,
    node_type      TEXT NOT NULL CHECK (node_type IN ('dir', 'leaf')),
    category       TEXT NOT NULL CHECK (category IN ('profile', 'preferences', 'entities', 'events', 'patterns', 'cases', 'moments', 'feedback', 'reference', 'session')),

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

    -- Retraction (added in v8)
    tombstoned_at    INTEGER,
    tombstone_reason TEXT,
    superseded_by    TEXT,

    FOREIGN KEY (parent_uri) REFERENCES mem_nodes_new(uri)
);

INSERT INTO mem_nodes_new SELECT * FROM mem_nodes;
DROP TABLE mem_nodes;
ALTER TABLE mem_nodes_new RENAME TO mem_nodes;

CREATE INDEX idx_nodes_parent    ON mem_nodes(parent_uri);
CREATE INDEX idx_nodes_category  ON mem_nodes(category);
CREATE INDEX idx_nodes_relevance ON mem_nodes(relevance DESC);

PRAGMA foreign_keys=ON;
`,
	},
}

// headVersion is the highest schema version this binary knows how to apply.
// Computed lazily; callable from tests for the forward-compat guard.
func headVersion() int {
	if len(migrations) == 0 {
		return 0
	}
	return migrations[len(migrations)-1].Version
}

// ErrSchemaTooNew signals that the database has been migrated by a newer
// continuity binary than the one currently running. Treated as a fast-fail
// at startup so the operator sees a clear remediation message instead of
// the binary silently ignoring invariants it does not understand.
//
// Typed error so callers (e.g. a future `continuity doctor` command or a
// recovery flow) can branch on it without parsing the message string.
type ErrSchemaTooNew struct {
	Found     int
	Supported int
}

func (e *ErrSchemaTooNew) Error() string {
	return fmt.Sprintf(
		"database schema version %d is newer than this binary supports "+
			"(max %d); upgrade continuity, or restore a backup of the database "+
			"from before that migration ran",
		e.Found, e.Supported,
	)
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

	// Forward-compat guard: refuse to operate against a DB that has been
	// stamped with a schema version this binary does not know. The newer
	// version may carry invariants we cannot uphold (CHECK constraints,
	// triggers, foreign-key relationships), and silently ignoring them
	// would risk corrupting the on-disk state. Fail fast with a clear
	// operator-facing message instead.
	var maxApplied int
	if err := db.QueryRow(
		`SELECT COALESCE(MAX(version), 0) FROM schema_versions`,
	).Scan(&maxApplied); err != nil {
		return fmt.Errorf("read schema_versions: %w", err)
	}
	if head := headVersion(); maxApplied > head {
		return &ErrSchemaTooNew{Found: maxApplied, Supported: head}
	}

	// Upgrade restore point: if this is an existing on-disk DB (current
	// version > 0) and the pending migration set contains at least one risky
	// migration, take ONE restore point of the pre-upgrade state before any
	// pending migration runs. Fails closed — if the snapshot is required but
	// cannot be created/validated, abort the migration with no schema change.
	// Skipped for fresh installs (maxApplied == 0), :memory:, and SQLite
	// URI/DSN paths (see ensureUpgradeRestorePoint).
	if err := db.ensureUpgradeRestorePoint(maxApplied); err != nil {
		return err
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
