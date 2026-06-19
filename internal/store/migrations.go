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

// ensureSchemaVersionsTable creates the schema_versions bookkeeping table if it
// does not exist. Split out so the risky-upgrade detection in Open() can read the
// current version through the same idempotent create that migrate() relies on.
func (db *DB) ensureSchemaVersionsTable() error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_versions (
			version     INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at  INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
		)
	`); err != nil {
		return fmt.Errorf("create schema_versions: %w", err)
	}
	return nil
}

// riskyUpgradePending reports whether opening this on-disk DB would run a RISKY
// (destructive table-rebuild) migration: an existing DB (version > 0) on an
// eligible path with at least one pending risky migration. It is the gate Open()
// uses to decide whether the migration must run under EXCLUSIVE with a fresh conn
// (no open handle across the lock transition — Finding 1, Round 6). A pure read:
// it never mutates the DB beyond the idempotent schema_versions create.
//
// Returns (false, maxApplied, nil) for fresh installs (maxApplied == 0),
// ineligible paths (:memory:/URI), or when only non-risky ALTER migrations are
// pending (those do not rewrite tables and are safe under the lifetime SHARED
// lock). Surfaces ErrSchemaTooNew when the DB is newer than this binary supports.
func (db *DB) riskyUpgradePending() (risky bool, maxApplied int, err error) {
	if err := db.ensureSchemaVersionsTable(); err != nil {
		return false, 0, err
	}
	if err := db.QueryRow(
		`SELECT COALESCE(MAX(version), 0) FROM schema_versions`,
	).Scan(&maxApplied); err != nil {
		return false, 0, fmt.Errorf("read schema_versions: %w", err)
	}
	if head := headVersion(); maxApplied > head {
		return false, maxApplied, &ErrSchemaTooNew{Found: maxApplied, Supported: head}
	}
	_, hasRisky := firstPendingRiskyVersion(maxApplied)
	return maxApplied > 0 && hasRisky && snapshotEligiblePath(db.Path), maxApplied, nil
}

func (db *DB) migrate() error {
	// Create schema_versions table if it doesn't exist
	if err := db.ensureSchemaVersionsTable(); err != nil {
		return err
	}

	// NOTE: the per-DB instance identity is intentionally NOT written here.
	// Writing continuity_meta unconditionally on every Open would MUTATE the DB
	// even when a restore point cannot be secured (e.g. a blocked/regular-file
	// sidecar). Instead it is established inside writeRestorePoint, after the
	// sidecar is proven usable and before the VACUUM INTO, so a fail-closed
	// upgrade leaves the DB completely unmutated. The identity legitimately lives
	// in the DB (per-DB, intentionally copyable); only the ORDERING relative to
	// fail-closed matters (see instance.go / snapshot.go, Finding 5).

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

	// Upgrade restore point + risky migration DDL must be SERIALIZED end-to-end.
	// If this is an existing on-disk DB (version > 0) and the pending set contains
	// at least one risky migration, hold the sidecar operation lock from BEFORE
	// the restore point is created THROUGH the entire migration loop. Holding the
	// op-lock across the DDL (not just across sidecar creation) is what makes two
	// concurrent direct opens serialize: the loser waits briefly then fails closed
	// rather than racing into the destructive CREATE/COPY/DROP/RENAME migration
	// (Finding 6). Fails closed — if the snapshot is required but cannot be
	// created/validated, abort with no schema change. Skipped for fresh installs
	// (maxApplied == 0), :memory:, and SQLite URI/DSN paths.
	//
	// SERIALIZATION IS DECOUPLED FROM THE SNAPSHOT OPT-OUT (Finding 4): a risky
	// on-disk upgrade against an eligible path acquires the op-lock REGARDLESS of
	// CONTINUITY_DISABLE_MIGRATION_SNAPSHOT. The env var only suppresses creating
	// the restore point (handled inside ensureUpgradeRestorePoint*), NOT the
	// lock/serialization boundary. Otherwise two opt-out processes could both enter
	// the destructive mem_nodes rebuild concurrently and tear the schema. The lock
	// still requires an eligible path (the op-lock lives beside the sidecar, which
	// is path-derived); :memory:/URI/DSN upgrades cannot take it and the
	// restore-point helper fails closed on them unless opted out.
	_, hasRisky := firstPendingRiskyVersion(maxApplied)
	riskyUpgrade := maxApplied > 0 && hasRisky && snapshotEligiblePath(db.Path)

	if riskyUpgrade {
		// NO-OPEN-HANDLE-ACROSS-LOCK-TRANSITION INVARIANT (Finding 1, Round 6).
		// The destructive restore-point creation + migration DDL run ONLY under the
		// EXCLUSIVE lock. There are exactly two ways to be here safely:
		//
		//   (a) migratingUnderExclusive: Open() already took EXCLUSIVE on a FRESH
		//       conn AFTER closing the first conn and releasing SHARED — no *sql.DB
		//       handle existed across the shared→exclusive transition. Run directly,
		//       NO re-acquire (the in-process RWMutex is not re-entrant).
		//
		//   (b) db.lock == nil: the caller's *DB holds NO lifetime SHARED lock (a
		//       lock-less handle — e.g. a direct migrate() in tests, or an
		//       ineligible-path open). It NEVER held shared, so self-acquiring a
		//       standalone EXCLUSIVE here introduces no shared-release-with-live-conn
		//       window. We take it (bounded, fail closed) and release after the DDL.
		//
		// What is FORBIDDEN is the old dance: a *DB that HOLDS a lifetime SHARED lock
		// (db.lock != nil) self-upgrading to EXCLUSIVE while its conn is open. That is
		// exactly the Finding-1 hazard; Open() now routes risky upgrades through (a),
		// so reaching here with a lifetime lock held is a contract violation.
		if db.migratingUnderExclusive {
			if err := db.ensureUpgradeRestorePointLocked(maxApplied); err != nil {
				return err
			}
			return db.runPendingMigrations()
		}
		if db.lock != nil && db.lock.mu != nil {
			return fmt.Errorf(
				"internal: risky migration on a DB holding a lifetime shared lock " +
					"(no-open-handle-across-lock-transition invariant violated); " +
					"the risky-upgrade path must transition the lock in Open()")
		}
		// Lock-less caller: take a standalone EXCLUSIVE lock across restore-point
		// creation + the migration loop (bounded wait, fail closed with ErrDBLocked).
		// This serializes concurrent direct migrate() callers — the loser waits then
		// fails closed rather than racing into the destructive rebuild.
		release, lerr := acquireExclusiveLockForOwner(db)
		if lerr != nil {
			return lerr
		}
		defer release()
		if err := db.ensureUpgradeRestorePointLocked(maxApplied); err != nil {
			return err
		}
		return db.runPendingMigrations()
	}

	// No risky migration pending (or ineligible path): the restore-point helper
	// still handles the opt-out/ineligible warnings, but no exclusive lock is held —
	// non-risky ALTER-only migrations do not rewrite tables and are safe to run
	// under the lifetime SHARED lock without cross-process serialization.
	if err := db.ensureUpgradeRestorePoint(maxApplied); err != nil {
		return err
	}
	return db.runPendingMigrations()
}

// runPendingMigrations applies every not-yet-recorded migration in order, each
// in its own transaction. Split out of migrate() so it can run either under the
// held operation lock (risky upgrade) or directly (non-risky path).
func (db *DB) runPendingMigrations() error {
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
