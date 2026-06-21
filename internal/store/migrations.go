package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
)

type migration struct {
	Version     int
	Description string
	SQL         string

	// Risky marks a migration as one that rebuilds or transforms user data
	// in ways SQLite's transactional guarantees cannot recover from on
	// developer error. Triggers an automatic safety snapshot before the
	// migration runs (see snapshot.go). Set true for full-table rebuilds
	// (CREATE _new + INSERT SELECT * + DROP + RENAME). Leave false for
	// additive migrations (CREATE TABLE / ALTER TABLE ADD COLUMN) — those
	// are reversible enough that the snapshot cost is unjustified.
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
		Risky:       true, // full-table rebuild via INSERT SELECT *; column-order parity is by developer discipline
		// NOTE: This rebuild does DROP TABLE mem_nodes, which would cascade-
		// delete mem_vectors (FK ON DELETE CASCADE, v4) if foreign keys were
		// enforced. FK-off is enforced by the migration RUNNER on a pinned
		// connection (see migrate()), NOT by an in-SQL `PRAGMA foreign_keys=OFF`
		// — that pragma is a no-op inside a transaction and on the wrong pooled
		// connection. Do NOT re-add it here; it is an inert trap.
		SQL: `
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
		Risky:       true, // full-table rebuild via INSERT SELECT *; column-order parity is by developer discipline
		// NOTE: FK-off during this DROP-TABLE rebuild is enforced by the
		// migration RUNNER on a pinned connection (see migrate()), NOT by an
		// in-SQL `PRAGMA foreign_keys=OFF` — that pragma is inert inside a
		// transaction and would otherwise let DROP TABLE mem_nodes cascade-
		// delete mem_vectors. Do NOT re-add it here.
		SQL: `
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
`,
	},
	{
		Version:     10,
		Description: "metrics_daily: daily health snapshot for Memory Health trend lines",
		// Additive table; no user data touched. total_access is the cumulative
		// SUM(access_count) snapshotted daily — day-over-day diffs yield retrievals
		// per day without logging per-touch events or mutating hot paths.
		SQL: `
CREATE TABLE metrics_daily (
    date            TEXT PRIMARY KEY,            -- 'YYYY-MM-DD' (UTC)
    active_total    INTEGER NOT NULL DEFAULT 0,
    retracted_total INTEGER NOT NULL DEFAULT 0,
    fresh           INTEGER NOT NULL DEFAULT 0,
    fading          INTEGER NOT NULL DEFAULT 0,
    stale           INTEGER NOT NULL DEFAULT 0,
    never_retrieved INTEGER NOT NULL DEFAULT 0,
    total_access    INTEGER NOT NULL DEFAULT 0,  -- SUM(access_count); daily retrievals = diff vs prior day
    captures        INTEGER NOT NULL DEFAULT 0,  -- memories created that day
    category_counts TEXT,                        -- JSON {category: count}
    updated_at      INTEGER NOT NULL
);
`,
	},
	{
		Version:     11,
		Description: "mem_meta: key/value store for corpus-level facts (vector identity)",
		// Additive table; no user data touched. Holds the corpus's DECLARED
		// vector identity (model:dims) so the active embedder can be reconciled
		// against it at startup instead of being chosen by environment alone —
		// the root cause of the silent re-embed migration. See engine/identity.go.
		SQL: `
CREATE TABLE mem_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
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

// HeadSchemaVersion returns the highest schema version this binary knows how to
// apply. It is the exported accessor over headVersion(), used by the health
// endpoint to advertise the binary's schema ceiling so a client can detect
// when it is talking to a server built against an older (or newer) schema.
func HeadSchemaVersion() int {
	return headVersion()
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
	// operator-facing message instead. Runs before any other setup so a
	// too-new DB short-circuits before we touch snapshot bookkeeping.
	var maxApplied int
	if err := db.QueryRow(
		`SELECT COALESCE(MAX(version), 0) FROM schema_versions`,
	).Scan(&maxApplied); err != nil {
		return fmt.Errorf("read schema_versions: %w", err)
	}
	if head := headVersion(); maxApplied > head {
		return &ErrSchemaTooNew{Found: maxApplied, Supported: head}
	}

	// Sidecar table for migration-snapshot bookkeeping. Lives outside the
	// schema_versions migration system because we need it to exist before
	// any risky migration runs (including v6, the first risky one).
	if err := db.ensureSnapshotStateTable(); err != nil {
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

		// Capture the pre-migration schema version BEFORE taking the
		// snapshot; we need this to record what the snapshot represents.
		var preVersion int
		if err := db.QueryRow(
			`SELECT COALESCE(MAX(version), 0) FROM schema_versions`,
		).Scan(&preVersion); err != nil {
			return fmt.Errorf("read pre-version for migration %d: %w", m.Version, err)
		}

		// Snapshot BEFORE applying a risky migration. If snapshot creation
		// fails, the migration MUST NOT proceed — that's the safety net
		// contract. The operator can set CONTINUITY_NO_MIGRATION_SNAPSHOT
		// to bypass when they've made an explicit decision to.
		snapPath, err := db.snapshotBeforeRiskyMigration(m)
		if err != nil {
			return fmt.Errorf(
				"snapshot before migration %d: %w "+
					"(set %s=1 to skip snapshots, knowing you accept the risk)",
				m.Version, err, EnvNoMigrationSnapshot,
			)
		}

		if err := db.applyMigration(m); err != nil {
			if snapPath != "" {
				_ = os.Remove(snapPath) // migration failed/rolled back; snapshot is dead weight
			}
			return err
		}

		// Migration committed. Now enroll the snapshot in the tracking
		// table and prune any older one. A failure here leaves the
		// snapshot file on disk but unrecorded — `continuity snapshot
		// prune` can mop up later, and the migration's success is the
		// load-bearing outcome.
		if snapPath != "" {
			if err := db.recordSnapshotAndPruneOlder(snapPath, preVersion, m.Version); err != nil {
				fmt.Fprintf(os.Stderr,
					"warning: migration %d succeeded but snapshot %s could not be recorded: %v\n",
					m.Version, snapPath, err)
			}
		}
	}

	return nil
}

// applyMigration runs a single migration's SQL plus its schema_versions stamp
// inside one transaction, then commits.
//
// Risky (table-rebuild) migrations DROP TABLE mem_nodes, which would cascade-
// delete mem_vectors (FK ON DELETE CASCADE, v4) if foreign keys were enforced.
// SQLite's `PRAGMA foreign_keys` is a no-op inside a transaction, and the
// connection pool may hand the tx a different connection than a pooled
// db.Exec("PRAGMA ...") touched — so the only correct way to disable FK
// enforcement for the rebuild is to pin a single *sql.Conn, toggle FK OFF on
// it OUTSIDE any transaction, run the migration tx on that SAME conn, then
// restore FK ON before releasing it. We always restore FK and close the conn,
// even on error, so a mid-migration failure never leaves the pool with FK off.
//
// Non-risky migrations (ALTER TABLE ADD COLUMN, CREATE TABLE) don't depend on
// FK state, so they keep the simpler pooled db.Begin() path.
func (db *DB) applyMigration(m migration) error {
	if !m.Risky {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.Version, err)
		}
		if err := runMigrationTx(tx, m); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}
		return nil
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire pinned conn for migration %d: %w", m.Version, err)
	}
	// Guarantee FK is restored and the pinned conn is returned, even if the
	// migration fails partway. Restoring FK here (not just on the happy path)
	// is what keeps a failed risky migration from poisoning the pool.
	defer func() {
		_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys=ON")
		conn.Close()
	}()

	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return fmt.Errorf("disable foreign_keys for migration %d: %w", m.Version, err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.Version, err)
	}
	if err := runMigrationTx(tx, m); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %d: %w", m.Version, err)
	}
	return nil
}

// txExec is the subset of tx behavior runMigrationTx needs — satisfied by both
// *sql.Tx (non-risky path) and the tx from a pinned conn's BeginTx (risky path).
type txExec interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// runMigrationTx applies the migration body and records its schema_versions row.
// Caller owns commit/rollback.
func runMigrationTx(tx txExec, m migration) error {
	if _, err := tx.Exec(m.SQL); err != nil {
		return fmt.Errorf("migration %d (%s): %w", m.Version, m.Description, err)
	}
	if _, err := tx.Exec(
		"INSERT INTO schema_versions (version, description) VALUES (?, ?)",
		m.Version, m.Description,
	); err != nil {
		return fmt.Errorf("record migration %d: %w", m.Version, err)
	}
	return nil
}

// SchemaVersion returns the current schema version.
func (db *DB) SchemaVersion() (int, error) {
	var version int
	err := db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_versions").Scan(&version)
	return version, err
}
