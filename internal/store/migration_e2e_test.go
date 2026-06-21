//go:build !windows

package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/lazypower/continuity/internal/testharness"
)

// TestMigrationE2E_* tests pin the upgrade-path contract: a continuity DB
// built at an OLD schema version must round-trip cleanly when the current
// binary opens it — schema migrates to head, every seeded row survives with
// new columns nullable-defaulted, and the HTTP read surface returns the same
// data we put in.
//
// What this covers that the in-process tests in db_test.go do NOT:
//
//   - In-process tests build a fresh DB straight to v9 (via OpenMemory).
//     They cannot exercise the incremental upgrade path a real user takes.
//   - In-process tests cannot prove the BINARY boots against an old DB —
//     migrations may succeed in isolation but the engine/server/embedder
//     init that runs after Open could trip on an unexpected state.
//   - The HTTP read surface is never exercised against a migrated DB in
//     the in-process suite; a regression that broke /api/memories on
//     freshly-migrated rows would slip through.
//
// Why this matters: the v6 and v9 migrations are full-table rebuilds
// (CREATE _new + INSERT SELECT * + DROP + RENAME). SQLite's SELECT * uses
// source column order — any drift between source and destination column
// lists silently misaligns columns. Pre-existing column-count parity is
// the only thing keeping current migrations safe; this suite locks it in.
//
// Tests run with `-tags noembed` to inherit the hermetic CI story from
// the PR #27 / #28 e2e jobs.

// buildDBAtVersion programmatically applies migrations [1..target] to a
// fresh SQLite DB and returns its path. This is the time machine: we use it
// to manufacture DB images that look like a user upgrading from an earlier
// continuity release.
//
// Reuses the same migration code the production Open path uses. Avoiding
// hand-rolled per-version schemas keeps drift impossible — if a future PR
// changes how a migration runs, this helper reflects that change for free.
func buildDBAtVersion(t *testing.T, dir string, target int) string {
	t.Helper()
	if target < 0 || target > len(migrations) {
		t.Fatalf("buildDBAtVersion: invalid target %d (have %d migrations)", target, len(migrations))
	}
	path := filepath.Join(dir, "test.db")

	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS schema_versions (
			version     INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at  INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
		)
	`); err != nil {
		t.Fatalf("schema_versions: %v", err)
	}

	for _, m := range migrations {
		if m.Version > target {
			break
		}
		tx, err := sqlDB.Begin()
		if err != nil {
			t.Fatalf("begin v%d: %v", m.Version, err)
		}
		if _, err := tx.Exec(m.SQL); err != nil {
			tx.Rollback()
			t.Fatalf("apply v%d: %v", m.Version, err)
		}
		if _, err := tx.Exec(
			"INSERT INTO schema_versions (version, description) VALUES (?, ?)",
			m.Version, m.Description,
		); err != nil {
			tx.Rollback()
			t.Fatalf("record v%d: %v", m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit v%d: %v", m.Version, err)
		}
	}

	return path
}

// The seed helpers below delegate to the shared, version-aware seeders in
// migrationseed.go so the replay-based tests here and the real-artifact tests
// in migration_fixture_test.go insert byte-identical data. Each opens the DB,
// runs one shared seed step, and fails the test on error. seedV5Data is the
// v5 baseline; seedV6Moment / seedV7Tone / seedV8Tombstone layer the v6/v7/v8
// additions à la carte (the historical call sites add them incrementally).

// seedV5Data inserts the v5-era baseline (pre-moments, pre-tone, pre-retraction).
func seedV5Data(t *testing.T, dbPath string) {
	t.Helper()
	withSeedDB(t, dbPath, seedV5Base)
}

// seedV7Tone adds a non-NULL tone value to the v5 session (v7 ALTER path).
func seedV7Tone(t *testing.T, dbPath string) {
	t.Helper()
	withSeedDB(t, dbPath, seedTone)
}

// seedV6Moment adds a 'moments' row (v6-introduced category).
func seedV6Moment(t *testing.T, dbPath string) {
	t.Helper()
	withSeedDB(t, dbPath, seedMoment)
}

// seedV8Tombstone adds a retracted (tombstoned) mem_node row (v8 retraction
// columns; load-bearing for the v9 INSERT SELECT * rebuild).
func seedV8Tombstone(t *testing.T, dbPath string) {
	t.Helper()
	withSeedDB(t, dbPath, seedTombstone)
}

// withSeedDB opens dbPath with the raw sqlite driver, runs one shared seed
// step, and fails the test on any error.
func withSeedDB(t *testing.T, dbPath string, step func(*sql.DB) error) {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open for seed: %v", err)
	}
	defer sqlDB.Close()
	if err := step(sqlDB); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// startSubprocessAgainstDB boots `continuity serve` against the given DB.
// Returns the server's resolved URL, the env vector (for follow-up CLI
// calls), and the live ServerProcess (caller registers Stop via t.Cleanup).
func startSubprocessAgainstDB(t *testing.T, bin, workDir, dbPath string) (string, []string, *testharness.ServerProcess) {
	t.Helper()
	serverURL, env := testharness.HermeticEnv(t, workDir, dbPath, 0)
	srv := testharness.StartServeProcess(t, bin, env)
	testharness.WaitForReady(t, serverURL+"/api/health")
	return serverURL, env, srv
}

// fetchMemoryByURI queries /api/memories?uri=<uri> and decodes the JSON
// payload. Returns nil on 404 so callers can assert presence/absence.
func fetchMemoryByURI(t *testing.T, serverURL, uri string) map[string]any {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(serverURL + "/api/memories?uri=" + uri)
	if err != nil {
		t.Fatalf("GET memories: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET memories %s = %s", uri, resp.Status)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

// assertSchemaV9 reopens the DB and asserts SchemaVersion() returns the current
// head version. (Named for v9, the rebuild this suite was written around; it
// tracks head so additive migrations don't break it.) Inline so call sites read
// top-down.
func assertSchemaV9(t *testing.T, dbPath string) {
	t.Helper()
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
	defer db.Close()
	v, err := db.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != headVersion() {
		t.Errorf("schema_version = %d, want head %d (have %d migrations)", v, headVersion(), len(migrations))
	}
}

// =========================================================================
// Upgrade-path tests — one per historical starting point
// =========================================================================

// TestMigrationE2E_UpgradeFromV5_PreservesData covers the longest upgrade
// chain we exercise: v5 (pre-moments) all the way to v9. Touches both
// table rebuilds (v6 and v9). Seeds one row per v5-valid category so a
// CHECK-constraint drift would surface.
func TestMigrationE2E_UpgradeFromV5_PreservesData(t *testing.T) {
	if testing.Short() {
		t.Skip("migration e2e: skipped under -short")
	}

	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)

	serverURL, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	t.Cleanup(srv.Stop)

	assertSchemaV9(t, dbPath)

	// Every seeded category should still be reachable via the HTTP API
	// (server boot + migrate + read path all functional end-to-end).
	cats := []string{"profile", "preferences", "entities", "events", "patterns", "cases"}
	for i, cat := range cats {
		uri := fmt.Sprintf("mem://user/%s/v5-seed-%d", cat, i)
		got := fetchMemoryByURI(t, serverURL, uri)
		if got == nil {
			t.Errorf("v5 seed %s lost after migration to v9", uri)
			continue
		}
		if got["category"] != cat {
			t.Errorf("seed %s category drift: got %v, want %s", uri, got["category"], cat)
		}
		if got["uri"] != uri {
			t.Errorf("seed %s uri drift: got %v", uri, got["uri"])
		}
	}

	// Re-open the DB and check the v8-added columns exist and default to
	// NULL on rows that pre-existed them. SQLite ALTER TABLE ADD COLUMN
	// defaults are NULL, and v9's INSERT SELECT * must preserve that.
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	var nullCount int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM mem_nodes
		WHERE tombstoned_at IS NULL
		  AND tombstone_reason IS NULL
		  AND superseded_by IS NULL
	`).Scan(&nullCount); err != nil {
		t.Fatalf("count nulls: %v", err)
	}
	if nullCount != len(cats) {
		t.Errorf("rows with NULL retraction columns = %d, want %d (one per seeded category)",
			nullCount, len(cats))
	}

	// And the session/observations tables came through intact.
	var msgCount int
	if err := db.QueryRow(`SELECT message_count FROM sessions WHERE session_id = ?`,
		"v5-test-session").Scan(&msgCount); err != nil {
		t.Fatalf("session lost: %v", err)
	}
	if msgCount != 7 {
		t.Errorf("session.message_count = %d, want 7", msgCount)
	}
	var obsCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM observations WHERE session_id = ?`,
		"v5-test-session").Scan(&obsCount); err != nil {
		t.Fatalf("obs count: %v", err)
	}
	if obsCount != 1 {
		t.Errorf("observation count = %d, want 1", obsCount)
	}
}

// TestMigrationE2E_UpgradeFromV7_PreservesToneAndMoments covers the user
// path most likely to be live in the wild today: a DB built before
// retraction landed (PR #20 / #19). Pins that the v7 tone column and a
// v6 moments row survive the v8 ALTER and v9 rebuild.
func TestMigrationE2E_UpgradeFromV7_PreservesToneAndMoments(t *testing.T) {
	if testing.Short() {
		t.Skip("migration e2e: skipped under -short")
	}

	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 7)
	seedV5Data(t, dbPath)
	seedV6Moment(t, dbPath)
	seedV7Tone(t, dbPath)

	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	t.Cleanup(srv.Stop)

	assertSchemaV9(t, dbPath)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	// Tone column survives v8 ALTER and v9 rebuild.
	var tone sql.NullString
	if err := db.QueryRow(`SELECT tone FROM sessions WHERE session_id = ?`,
		"v5-test-session").Scan(&tone); err != nil {
		t.Fatalf("read tone: %v", err)
	}
	if !tone.Valid || tone.String != "focused" {
		t.Errorf("tone lost or mangled: got valid=%v string=%q", tone.Valid, tone.String)
	}

	// moments row still present with original category + content.
	var (
		category   string
		l0Abstract sql.NullString
	)
	if err := db.QueryRow(`
		SELECT category, l0_abstract FROM mem_nodes WHERE uri = ?
	`, "mem://user/moments/v6-first-gift").Scan(&category, &l0Abstract); err != nil {
		t.Fatalf("read moment: %v", err)
	}
	if category != "moments" {
		t.Errorf("moments category lost: got %q", category)
	}
	if !l0Abstract.Valid || l0Abstract.String == "" {
		t.Errorf("moment l0_abstract lost: %+v", l0Abstract)
	}
}

// TestMigrationE2E_UpgradeFromV8_PreservesTombstones is the load-bearing
// test for the v9 INSERT SELECT * rebuild. v8 ALTER TABLE added three
// retraction columns AT THE END of the mem_nodes column list. v9's
// CREATE TABLE mem_nodes_new declares those same three columns AT THE END
// too. The rebuild relies on this column-order parity; a regression that
// moved retraction columns in v9 (or added new columns in the wrong
// position) would silently misalign data.
//
// We seed a tombstoned row with distinguishable values for every retraction
// column, then verify each one survives the v9 rebuild byte-identical.
func TestMigrationE2E_UpgradeFromV8_PreservesTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("migration e2e: skipped under -short")
	}

	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 8)
	seedV8Tombstone(t, dbPath)

	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	t.Cleanup(srv.Stop)

	assertSchemaV9(t, dbPath)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	var (
		tombstonedAt    sql.NullInt64
		tombstoneReason sql.NullString
		supersededBy    sql.NullString
	)
	err = db.QueryRow(`
		SELECT tombstoned_at, tombstone_reason, superseded_by
		FROM mem_nodes WHERE uri = ?
	`, "mem://user/events/v8-retracted-row").Scan(&tombstonedAt, &tombstoneReason, &supersededBy)
	if err != nil {
		t.Fatalf("read tombstone after v9: %v", err)
	}
	if !tombstonedAt.Valid || tombstonedAt.Int64 == 0 {
		t.Error("tombstoned_at lost through v9 rebuild")
	}
	if !tombstoneReason.Valid || tombstoneReason.String != "captured operator's home address by accident" {
		t.Errorf("tombstone_reason mangled: %+v", tombstoneReason)
	}
	if !supersededBy.Valid || supersededBy.String != "mem://user/events/v8-replacement" {
		t.Errorf("superseded_by mangled: %+v", supersededBy)
	}

	// The v9 CHECK constraint additions (feedback, reference) must be live.
	// If v9 didn't run, this insert would be rejected.
	_, err = db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://user/feedback/post-v9', 'leaf', 'feedback', 1000, 1000)
	`)
	if err != nil {
		t.Errorf("v9 did not run on this DB: feedback category still rejected: %v", err)
	}
}

// TestMigrationE2E_FreshInstallReachesV9 pins the cold-start path: no
// pre-existing DB, the binary creates one and migrates straight to v9.
// Covers the new-user install case.
func TestMigrationE2E_FreshInstallReachesV9(t *testing.T) {
	if testing.Short() {
		t.Skip("migration e2e: skipped under -short")
	}

	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "fresh.db")

	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	t.Cleanup(srv.Stop)

	assertSchemaV9(t, dbPath)
}

// TestMigrationE2E_IdempotentSecondBoot pins that booting against an
// already-migrated DB is a no-op (no errors, schema stays at head).
// The in-process TestMigrationsIdempotent covers the migrate() call path
// in isolation; this covers the same invariant through the binary's full
// boot path, where a regression that re-ran migrations could destroy data.
func TestMigrationE2E_IdempotentSecondBoot(t *testing.T) {
	if testing.Short() {
		t.Skip("migration e2e: skipped under -short")
	}

	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	// First boot — fresh DB → v9.
	dbPath := filepath.Join(workDir, "twice.db")
	_, _, srv1 := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	assertSchemaV9(t, dbPath)

	// Seed a marker row so a destructive re-migration would be visible.
	{
		db, err := Open(dbPath)
		if err != nil {
			srv1.Stop()
			t.Fatal(err)
		}
		_, err = db.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/idempotency-marker', 'leaf', 'events', 'survives second boot', 1000, 1000)
		`)
		db.Close()
		if err != nil {
			srv1.Stop()
			t.Fatal(err)
		}
	}
	srv1.Stop()

	// Second boot — same DB, fresh subprocess.
	workDir2 := t.TempDir() // for HOME
	_, env := testharness.HermeticEnv(t, workDir2, dbPath, 0)
	srv2 := testharness.StartServeProcess(t, bin, env)
	t.Cleanup(srv2.Stop)
	testharness.WaitForReady(t, fmt.Sprintf("http://127.0.0.1:%s/api/health",
		envGet(env, "CONTINUITY_PORT")))

	assertSchemaV9(t, dbPath)

	// Marker row must still be present — a re-run of v6 or v9 (full table
	// rebuilds) would either fail (table already exists in their CREATE
	// statements) or, worse, wipe rows.
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var marker string
	if err := db.QueryRow(`SELECT l0_abstract FROM mem_nodes WHERE uri = ?`,
		"mem://user/events/idempotency-marker").Scan(&marker); err != nil {
		t.Fatalf("marker row lost on second boot: %v", err)
	}
	if marker != "survives second boot" {
		t.Errorf("marker mangled: %q", marker)
	}
}

// =========================================================================
// FK-cascade regression — the risky table-rebuild migrations must NOT wipe
// mem_vectors (issue #40)
// =========================================================================

// seedVectorRow inserts a mem_vectors row referencing the given node_id with a
// distinguishable embedding BLOB. mem_vectors exists from v4; this exercises
// the FK ON DELETE CASCADE relationship that a careless DROP TABLE mem_nodes
// (in the v6/v9 rebuilds) would trigger.
func seedVectorRow(t *testing.T, dbPath string, nodeID int64) {
	t.Helper()
	withSeedDB(t, dbPath, func(db *sql.DB) error {
		_, err := db.Exec(`
			INSERT INTO mem_vectors (node_id, embedding, model, dimensions, created_at)
			VALUES (?, ?, ?, ?, ?)
		`, nodeID, []byte{0xde, 0xad, 0xbe, 0xef}, "test-model", 4, seedConst)
		return err
	})
}

// insertNodeReturningID inserts one mem_node and returns its rowid so a
// mem_vectors row can reference it.
func insertNodeReturningID(t *testing.T, dbPath, uri string) int64 {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open for node insert: %v", err)
	}
	defer sqlDB.Close()
	res, err := sqlDB.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES (?, 'leaf', 'profile', ?, ?)
	`, uri, seedConst, seedConst)
	if err != nil {
		t.Fatalf("insert node: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

// assertVectorSurvives reopens the DB and asserts exactly one mem_vectors row
// exists for nodeID after migration. Without the FK-off-on-pinned-conn fix, the
// v6/v9 DROP TABLE mem_nodes cascade-deletes it and this count is 0.
func assertVectorSurvives(t *testing.T, dbPath string, nodeID int64) {
	t.Helper()
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM mem_vectors WHERE node_id = ?`, nodeID,
	).Scan(&n); err != nil {
		t.Fatalf("count mem_vectors: %v", err)
	}
	if n != 1 {
		t.Fatalf("mem_vectors row for node %d = %d, want 1 "+
			"(risky rebuild cascade-deleted the embedding cache — FK was not "+
			"disabled on the migration's pinned connection)", nodeID, n)
	}
}

// TestMigrationE2E_V9RebuildPreservesVectors is the regression test for issue
// #40: the v9 full-table rebuild DROPs mem_nodes, and with FK enforcement on
// that cascade-deletes every mem_vectors row (FK ON DELETE CASCADE, v4). We
// build a DB at v8 (mem_vectors present, pre-v9-rebuild), seed a node + a
// vector referencing it, migrate forward through v9 via Open(), and assert the
// vector survives. Fails before the fix (count 0), passes after.
func TestMigrationE2E_V9RebuildPreservesVectors(t *testing.T) {
	dir := t.TempDir()
	dbPath := buildDBAtVersion(t, dir, 8)

	nodeID := insertNodeReturningID(t, dbPath, "mem://user/profile/v8-with-vector")
	seedVectorRow(t, dbPath, nodeID)

	// Migrate forward through the v9 rebuild.
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open (migrate to head): %v", err)
	}
	db.Close()

	assertVectorSurvives(t, dbPath, nodeID)
}

// TestMigrationE2E_V6RebuildPreservesVectors covers the earlier risky rebuild
// (v6). Build at v5 (mem_vectors present from v4), seed a node + vector, migrate
// forward through both v6 and v9 rebuilds, and assert the vector survives both.
func TestMigrationE2E_V6RebuildPreservesVectors(t *testing.T) {
	dir := t.TempDir()
	dbPath := buildDBAtVersion(t, dir, 5)

	nodeID := insertNodeReturningID(t, dbPath, "mem://user/profile/v5-with-vector")
	seedVectorRow(t, dbPath, nodeID)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open (migrate to head): %v", err)
	}
	db.Close()

	assertVectorSurvives(t, dbPath, nodeID)
}

// envGet pulls a single KEY=VAL out of an env vector (the env slice returned
// by testharness.HermeticEnv). Used so the idempotency test can recover the
// kernel-allocated port for its second-boot wait.
func envGet(env []string, key string) string {
	prefix := key + "="
	for _, kv := range env {
		if len(kv) > len(prefix) && kv[:len(prefix)] == prefix {
			return kv[len(prefix):]
		}
	}
	return ""
}
