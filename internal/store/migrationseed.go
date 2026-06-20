package store

import (
	"database/sql"
	"fmt"
)

// Version-aware seed helpers for the migration regression harness.
//
// These are the single source of truth for "representative data at schema
// version N." Two callers share them:
//
//   - scripts/genfixtures: mints the committed golden DBs by booting a REAL
//     released binary to create schema vN, then seeding through these helpers.
//   - migration_e2e_test.go / migration_fixture_test.go: seed the same rows so
//     the replay-based and real-artifact tests assert against identical data.
//
// Every column live at the target schema gets a distinguishable value, so a
// column-misalignment bug introduced by a future migration surfaces as garbled
// data rather than a silent pass. They take a raw *sql.DB (not *DB) so the
// generator can drive an old-schema database the current engine would refuse to
// Open (its schema is behind head). They INSERT data only — never migrate.

// seedConst pins timestamps so generated fixtures are byte-stable across runs
// (Date.now() drift would make every regeneration a spurious git diff). The
// value is a fixed UnixMilli: 2026-01-01T00:00:00Z.
const seedConst int64 = 1767225600000

// seedV5Categories are the categories valid at schema v5 (pre-moments,
// pre-feedback/reference). One node per category exercises the v5 CHECK range
// and, downstream, the v6/v9 table rebuilds that must carry every row across.
var seedV5Categories = []string{"profile", "preferences", "entities", "events", "patterns", "cases"}

// SeedSchemaV5 inserts the baseline row set valid at schema v5.
func SeedSchemaV5(db *sql.DB) error { return seedV5Base(db) }

// SeedSchemaV7 builds on V5 with a v6 'moments' row and a non-NULL v7 tone.
func SeedSchemaV7(db *sql.DB) error {
	if err := seedV5Base(db); err != nil {
		return err
	}
	if err := seedMoment(db); err != nil {
		return err
	}
	return seedTone(db)
}

// SeedSchemaV8 builds on V7 with a tombstoned mem_node exercising the v8
// retraction columns — the load-bearing seed for the v9 INSERT SELECT * rebuild.
func SeedSchemaV8(db *sql.DB) error {
	if err := SeedSchemaV7(db); err != nil {
		return err
	}
	return seedTombstone(db)
}

// --- Granular seed steps (single source of truth; package-internal so the
// in-package e2e tests can compose them à la carte). ---

// seedV5Base inserts one mem_node per v5 category, a session (with v5's
// extracted_at), and one observation.
func seedV5Base(db *sql.DB) error {
	now := seedConst
	for i, cat := range seedV5Categories {
		uri := fmt.Sprintf("mem://user/%s/v5-seed-%d", cat, i)
		if _, err := db.Exec(`
			INSERT INTO mem_nodes (
				uri, node_type, category,
				l0_abstract, l1_overview, l2_content,
				mergeable, relevance, last_access, access_count,
				source_session, created_at, updated_at
			) VALUES (?, 'leaf', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, uri, cat,
			fmt.Sprintf("L0 for %s", cat),
			fmt.Sprintf("L1 overview for %s with enough length", cat),
			fmt.Sprintf("L2 detail for %s", cat),
			i%2, 0.75, now, i*3,
			"v5-test-session", now, now); err != nil {
			return fmt.Errorf("seed mem_node %s: %w", cat, err)
		}
	}

	if _, err := db.Exec(`
		INSERT INTO sessions (
			session_id, project, started_at, ended_at, status,
			message_count, tool_count, extracted_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, "v5-test-session", "/tmp/v5-project", now-3600000, now, "completed",
		7, 3, now); err != nil {
		return fmt.Errorf("seed session: %w", err)
	}

	if _, err := db.Exec(`
		INSERT INTO observations (session_id, tool_name, tool_input, tool_response, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, "v5-test-session", "Write", `{"file":"v5.txt"}`, `{"ok":true}`, now); err != nil {
		return fmt.Errorf("seed observation: %w", err)
	}
	return nil
}

// seedMoment inserts the v6-introduced 'moments' category row.
func seedMoment(db *sql.DB) error {
	now := seedConst
	if _, err := db.Exec(`
		INSERT INTO mem_nodes (
			uri, node_type, category,
			l0_abstract, l1_overview,
			created_at, updated_at
		) VALUES (?, 'leaf', 'moments', ?, ?, ?, ?)
	`, "mem://user/moments/v6-first-gift",
		"received a thoughtful gift from a mentor",
		"Detailed body content about the moment with enough length.",
		now, now); err != nil {
		return fmt.Errorf("seed moment: %w", err)
	}
	return nil
}

// seedTone sets a non-NULL v7 tone on the seeded session.
func seedTone(db *sql.DB) error {
	if _, err := db.Exec(`UPDATE sessions SET tone = ? WHERE session_id = ?`,
		"focused", "v5-test-session"); err != nil {
		return fmt.Errorf("set tone: %w", err)
	}
	return nil
}

// seedTombstone inserts a retracted mem_node with distinguishable values for
// every v8 retraction column, so a v9-rebuild misalignment shows as garbled data.
func seedTombstone(db *sql.DB) error {
	now := seedConst
	if _, err := db.Exec(`
		INSERT INTO mem_nodes (
			uri, node_type, category,
			l0_abstract, l1_overview,
			tombstoned_at, tombstone_reason, superseded_by,
			created_at, updated_at
		) VALUES (?, 'leaf', 'events', ?, ?, ?, ?, ?, ?, ?)
	`, "mem://user/events/v8-retracted-row",
		"PII captured by mistake",
		"Original L1 with enough length to look real.",
		now-1000, "captured operator's home address by accident",
		"mem://user/events/v8-replacement",
		now-3600000, now-1000); err != nil {
		return fmt.Errorf("seed tombstone: %w", err)
	}
	return nil
}

// SeedSchemaVersion dispatches to the correct seeder for a shipped schema
// version. Used by the fixture generator, which knows only the numeric schema
// of the binary it just booted.
func SeedSchemaVersion(db *sql.DB, version int) error {
	switch version {
	case 5:
		return SeedSchemaV5(db)
	case 7:
		return SeedSchemaV7(db)
	case 8:
		return SeedSchemaV8(db)
	default:
		return fmt.Errorf("no seeder for schema version %d (have 5, 7, 8)", version)
	}
}
