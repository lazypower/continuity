//go:build !windows

package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/lazypower/continuity/internal/testharness"
)

// TestMigrationFixtureE2E_* migrate a database image MINTED BY A REAL RELEASED
// BINARY forward to head, and assert nothing is lost on the way.
//
// This is the real-artifact complement to migration_e2e_test.go. That suite
// manufactures "old" databases by replaying the CURRENT source tree's own
// migration SQL (buildDBAtVersion) — which can only ever prove column-order
// parity within today's source. It cannot catch a historical migration whose
// SQL was edited after the fact, or on-disk state an old binary wrote that the
// current source no longer describes.
//
// The fixtures here close that gap: each testdata/migration/v<N>/continuity.db
// was created by the actual shipped continuity binary (v0.2.2 → v5, v0.4.0 →
// v7, v0.5.0 → v8) via scripts/gen-migration-fixtures.sh, then seeded through
// the shared seeder in migrationseed.go. The committed goldens make the PR-gate
// run hermetic (no network, no old-binary builds); the regen/drift workflow
// re-pulls the real binaries on a schedule and points this same test at the
// freshly-minted images via CONTINUITY_FIXTURE_DIR.
//
// To regenerate the goldens (e.g. after shipping a new release that introduces
// a new distinct schema): `make migration-fixtures` (needs `gh auth` + network).
//
// Tests run with `-tags noembed`, inheriting the hermetic CI story from the
// existing e2e jobs, and ride the `-run 'E2E|Subprocess'` filter via their name.

// fixtureDir resolves where the golden images live: CONTINUITY_FIXTURE_DIR when
// set (the regen workflow points this at freshly-minted images), else the
// committed testdata directory.
func fixtureDir() string {
	if d := os.Getenv("CONTINUITY_FIXTURE_DIR"); d != "" {
		return d
	}
	return filepath.Join("testdata", "migration")
}

// loadFixtureCopy copies the committed golden for a schema version into a fresh
// temp dir and returns (workDir, dbPath). We never migrate the committed file in
// place — the current binary mutates the copy.
func loadFixtureCopy(t *testing.T, schema int) (string, string) {
	t.Helper()
	src := filepath.Join(fixtureDir(), fmt.Sprintf("v%d", schema), "continuity.db")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fixture %s (run `make migration-fixtures`?): %v", src, err)
	}
	workDir := t.TempDir()
	dbPath := filepath.Join(workDir, "continuity.db")
	if err := os.WriteFile(dbPath, data, 0o600); err != nil {
		t.Fatalf("copy fixture: %v", err)
	}
	return workDir, dbPath
}

// assertRawSchema opens dbPath with the raw driver (no migration) and asserts
// MAX(schema_versions.version) == want. Used for both pre-state (the fixture's
// shipped version) and snapshot recoverability (the pre-migration version).
func assertRawSchema(t *testing.T, dbPath string, want int) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("raw open %s: %v", dbPath, err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_versions`).Scan(&v); err != nil {
		t.Fatalf("read schema_versions: %v", err)
	}
	if v != want {
		t.Fatalf("schema = %d, want %d (%s)", v, want, dbPath)
	}
}

// migrateFixtureForward boots the CURRENT binary against the fixture copy,
// driving the real Open→migrate→serve boot path, and returns the live server
// URL. Snapshots are deliberately left ON (no opt-out) so the safety net runs.
func migrateFixtureForward(t *testing.T, dbPath string) (string, *testharness.ServerProcess) {
	t.Helper()
	bin := testharness.BuildContinuityBinary(t)
	workDir := filepath.Dir(dbPath)
	serverURL, env := testharness.HermeticEnv(t, workDir, dbPath, 0)
	// Defensively clear any inherited opt-out so the snapshot net is exercised.
	env = append(env, EnvNoMigrationSnapshot+"=")
	srv := testharness.StartServeProcess(t, bin, env)
	testharness.WaitForReady(t, serverURL+"/api/health")
	return serverURL, srv
}

// assertSnapshotNetEngaged verifies the migration safety net fired and is
// recoverable: exactly one retained snapshot, taken before the v9 rebuild
// (pre_version 8, target 9), whose file exists and opens as a valid pre-v9
// image still carrying the seeded baseline.
//
// NOTE (codex finding, .notes/codex-snapshot-design-review.md §"Correctness
// fix"): for a v5 fixture the upgrade crosses BOTH risky migrations (v6 and v9).
// Current code snapshots before each and prunes the older, so the pre-v6
// rollback point is gone — only the pre-v9 snapshot survives. This assertion
// PINS that current behavior (single snapshot, pre=8) rather than asserting a
// pre-upgrade rollback point, which the code does not yet provide. If the
// "snapshot once before the first risky migration in a run" pivot lands, this
// expectation changes to pre_version == the fixture's shipped version.
func assertSnapshotNetEngaged(t *testing.T, dbPath string) {
	t.Helper()
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen for snapshot check: %v", err)
	}
	defer db.Close()

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("retained snapshots = %d, want 1 (pre-v9 net)", len(snaps))
	}
	s := snaps[0]
	if s.TargetVersion != 9 || s.PreVersion != 8 {
		t.Errorf("snapshot pre/target = %d/%d, want 8/9", s.PreVersion, s.TargetVersion)
	}
	// File must live in this DB's namespaced dir and exist on disk.
	wantDir := snapshotDirForDB(dbPath)
	if filepath.Dir(s.Path) != wantDir {
		t.Errorf("snapshot dir = %s, want %s", filepath.Dir(s.Path), wantDir)
	}
	if _, err := os.Stat(s.Path); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}
	// Recoverable: the snapshot is a valid SQLite image at the pre-v9 schema
	// still holding the seeded baseline (a v5 category node survives).
	assertRawSchema(t, s.Path, 8)
	snap, err := sql.Open("sqlite", s.Path+"?mode=ro")
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.Close()
	var n int
	if err := snap.QueryRow(
		`SELECT COUNT(*) FROM mem_nodes WHERE uri = ?`,
		"mem://user/profile/v5-seed-0",
	).Scan(&n); err != nil {
		t.Fatalf("read snapshot rows: %v", err)
	}
	if n != 1 {
		t.Errorf("snapshot missing pre-migration baseline row (got %d)", n)
	}
}

// assertV5Baseline checks the v5 seed survived the upgrade: every category node
// is readable over HTTP with category/uri intact, and the session + observation
// rows came through. Shared by all three fixtures (each layers on top of v5).
func assertV5Baseline(t *testing.T, serverURL, dbPath string) {
	t.Helper()
	for i, cat := range seedV5Categories {
		uri := fmt.Sprintf("mem://user/%s/v5-seed-%d", cat, i)
		got := fetchMemoryByURI(t, serverURL, uri)
		if got == nil {
			t.Errorf("v5 seed %s lost after migration", uri)
			continue
		}
		if got["category"] != cat || got["uri"] != uri {
			t.Errorf("v5 seed %s drift: category=%v uri=%v", uri, got["category"], got["uri"])
		}
	}

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
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

// assertMomentAndTone checks the v6 moment row and v7 tone survived.
func assertMomentAndTone(t *testing.T, dbPath string) {
	t.Helper()
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	var category string
	var l0 sql.NullString
	if err := db.QueryRow(`SELECT category, l0_abstract FROM mem_nodes WHERE uri = ?`,
		"mem://user/moments/v6-first-gift").Scan(&category, &l0); err != nil {
		t.Fatalf("read moment: %v", err)
	}
	if category != "moments" || !l0.Valid || l0.String == "" {
		t.Errorf("moment drift: category=%q l0=%+v", category, l0)
	}

	var tone sql.NullString
	if err := db.QueryRow(`SELECT tone FROM sessions WHERE session_id = ?`,
		"v5-test-session").Scan(&tone); err != nil {
		t.Fatalf("read tone: %v", err)
	}
	if !tone.Valid || tone.String != "focused" {
		t.Errorf("tone lost or mangled: valid=%v string=%q", tone.Valid, tone.String)
	}
}

// assertTombstone checks the v8 retraction columns survived the v9 rebuild
// byte-intact — the load-bearing column-order-parity assertion.
func assertTombstone(t *testing.T, dbPath string) {
	t.Helper()
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
	if err := db.QueryRow(`
		SELECT tombstoned_at, tombstone_reason, superseded_by
		FROM mem_nodes WHERE uri = ?
	`, "mem://user/events/v8-retracted-row").Scan(&tombstonedAt, &tombstoneReason, &supersededBy); err != nil {
		t.Fatalf("read tombstone: %v", err)
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
}

// =========================================================================
// One test per distinct shipped schema, each from a real released binary.
// =========================================================================

// TestMigrationFixtureE2E_V5FromV022 migrates a database written by continuity
// v0.2.2 (schema v5) forward to head. Longest real upgrade chain: crosses both
// table-rebuild migrations (v6 and v9).
func TestMigrationFixtureE2E_V5FromV022(t *testing.T) {
	if testing.Short() {
		t.Skip("migration fixture e2e: skipped under -short")
	}
	_, dbPath := loadFixtureCopy(t, 5)
	assertRawSchema(t, dbPath, 5)

	serverURL, srv := migrateFixtureForward(t, dbPath)
	t.Cleanup(srv.Stop)

	assertRawSchema(t, dbPath, headVersion())
	assertV5Baseline(t, serverURL, dbPath)
	assertSnapshotNetEngaged(t, dbPath)
}

// TestMigrationFixtureE2E_V7FromV040 migrates a database written by continuity
// v0.4.0 (schema v7: moments + tone live, retraction not) forward to head.
func TestMigrationFixtureE2E_V7FromV040(t *testing.T) {
	if testing.Short() {
		t.Skip("migration fixture e2e: skipped under -short")
	}
	_, dbPath := loadFixtureCopy(t, 7)
	assertRawSchema(t, dbPath, 7)

	serverURL, srv := migrateFixtureForward(t, dbPath)
	t.Cleanup(srv.Stop)

	assertRawSchema(t, dbPath, headVersion())
	assertV5Baseline(t, serverURL, dbPath)
	assertMomentAndTone(t, dbPath)
	assertSnapshotNetEngaged(t, dbPath)
}

// TestMigrationFixtureE2E_V8FromV050 migrates a database written by continuity
// v0.5.0 (schema v8: retraction columns live) forward to head. Pins that real
// tombstoned rows survive the v9 INSERT SELECT * rebuild.
func TestMigrationFixtureE2E_V8FromV050(t *testing.T) {
	if testing.Short() {
		t.Skip("migration fixture e2e: skipped under -short")
	}
	_, dbPath := loadFixtureCopy(t, 8)
	assertRawSchema(t, dbPath, 8)

	serverURL, srv := migrateFixtureForward(t, dbPath)
	t.Cleanup(srv.Stop)

	assertRawSchema(t, dbPath, headVersion())
	assertV5Baseline(t, serverURL, dbPath)
	assertMomentAndTone(t, dbPath)
	assertTombstone(t, dbPath)
	assertSnapshotNetEngaged(t, dbPath)
}

// TestMigrationFixtureE2E_IdempotentReboot pins that a SECOND boot of the
// current binary against an already-migrated real-artifact DB is a no-op:
// schema stays at head and the seeded data is untouched (a re-run of a
// full-table rebuild would either error or wipe rows).
func TestMigrationFixtureE2E_IdempotentReboot(t *testing.T) {
	if testing.Short() {
		t.Skip("migration fixture e2e: skipped under -short")
	}
	_, dbPath := loadFixtureCopy(t, 8)

	_, srv1 := migrateFixtureForward(t, dbPath)
	assertRawSchema(t, dbPath, headVersion())
	srv1.Stop()

	// Second boot, fresh subprocess, same DB.
	bin := testharness.BuildContinuityBinary(t)
	workDir2 := t.TempDir()
	serverURL2, env := testharness.HermeticEnv(t, workDir2, dbPath, 0)
	srv2 := testharness.StartServeProcess(t, bin, env)
	t.Cleanup(srv2.Stop)
	testharness.WaitForReady(t, serverURL2+"/api/health")

	assertRawSchema(t, dbPath, headVersion())
	assertV5Baseline(t, serverURL2, dbPath)
	assertTombstone(t, dbPath)
}
