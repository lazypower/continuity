package store

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// =========================================================================
// Test helpers
// =========================================================================

// applyMigrationsUpTo executes migrations[1..target] directly against the
// connection, bypassing DB.migrate() entirely. Used to construct DB files
// at historical schema versions so we can exercise the upgrade path that
// store.Open() takes when those files are reopened by the current binary.
func applyMigrationsUpTo(t *testing.T, sqlDB *sql.DB, target int) {
	t.Helper()
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
}

// buildSnapshotDBAtVersion creates a DB file at path with schema at the given
// version. Returns nothing because the path is the input.
func buildSnapshotDBAtVersion(t *testing.T, path string, target int) {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sqlDB.Close()
	applyMigrationsUpTo(t, sqlDB, target)
}

// =========================================================================
// Creation behavior
// =========================================================================

// TestSnapshot_CreatedBeforeRiskyMigration pins the central invariant:
// when a risky migration runs against an on-disk DB, a snapshot file
// lands in the sibling snapshots/ directory before the migration touches
// the DB.
func TestSnapshot_CreatedBeforeRiskyMigration(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 5) // pre-v6, so v6 (risky) will run

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Snapshots are namespaced under snapshots/<db-filename>/ so sibling DBs
	// never collide; this DB is test.db.
	snapDir := filepath.Join(dir, "snapshots", "test.db")
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("snapshots dir not created: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no snapshot file in snapshots/ after v5→v9 upgrade")
	}
	// After v5→v9 only ONE snapshot must remain (the pre-v9 one); recording
	// pruned the pre-v6 file when v9's snapshot landed.
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("expected exactly 1 retained snapshot file, got %d: %v", len(entries), names)
	}
	if !strings.HasPrefix(entries[0].Name(), "continuity-pre-v9-") {
		t.Errorf("retained snapshot should be pre-v9; got %q", entries[0].Name())
	}
}

// TestSnapshot_OptOutEnvVarSkips pins the explicit opt-out: with
// CONTINUITY_NO_MIGRATION_SNAPSHOT set, even a risky migration runs
// without snapshotting. The operator who sets this is accepting the
// recoverability tradeoff knowingly.
func TestSnapshot_OptOutEnvVarSkips(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "1")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 5)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open with opt-out: %v", err)
	}
	defer db.Close()

	snapDir := filepath.Join(dir, "snapshots")
	if _, err := os.Stat(snapDir); err == nil {
		entries, _ := os.ReadDir(snapDir)
		if len(entries) > 0 {
			t.Errorf("opt-out set, but %d snapshot file(s) created", len(entries))
		}
	}

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 0 {
		t.Errorf("opt-out set, but %d snapshot row(s) recorded", len(snaps))
	}
}

// TestSnapshot_NotCreatedForAdditiveMigration unit-tests the gate directly
// against migrations marked Risky=false. Risky=false → no snapshot, no
// error. This is the load-bearing test for the "additive migrations skip
// snapshots" half of the policy.
func TestSnapshot_NotCreatedForAdditiveMigration(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 9) // current head

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Walk every migration and confirm snapshotBeforeRiskyMigration ONLY
	// returns a path for the ones we explicitly mark Risky.
	for _, m := range migrations {
		path, err := db.snapshotBeforeRiskyMigration(m)
		if err != nil {
			t.Fatalf("v%d: %v", m.Version, err)
		}
		switch {
		case m.Risky && path == "":
			t.Errorf("v%d is Risky but snapshot returned empty path", m.Version)
		case !m.Risky && path != "":
			t.Errorf("v%d is additive but snapshot returned path %q", m.Version, path)
		}
		// Clean up any file the test wrote so we don't leak across iterations.
		if path != "" {
			_ = os.Remove(path)
		}
	}
}

// TestSnapshot_MemoryDBSkipsSnapshot pins that :memory: DBs don't try to
// snapshot themselves. In-memory tests (which is most of the suite) would
// otherwise crash trying to VACUUM INTO an unwriteable path.
func TestSnapshot_MemoryDBSkipsSnapshot(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// All migrations have already run via OpenMemory. Probe the gate
	// directly with a risky migration descriptor and assert no path
	// returned, no error.
	for _, m := range migrations {
		if !m.Risky {
			continue
		}
		path, err := db.snapshotBeforeRiskyMigration(m)
		if err != nil {
			t.Errorf("memory DB: v%d snapshot returned error %v", m.Version, err)
		}
		if path != "" {
			t.Errorf("memory DB: v%d returned snapshot path %q", m.Version, path)
		}
	}
}

// =========================================================================
// Failure behavior
// =========================================================================

// TestSnapshot_FailureBlocksMigration pins the contract: if the snapshot
// CANNOT be created (e.g., the snapshots/ directory cannot be made),
// migrate() refuses to proceed. This is the load-bearing safety net
// invariant — a "snapshots are optional" implementation would silently
// run the risky migration without protection.
func TestSnapshot_FailureBlocksMigration(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 5)

	// Plant a regular FILE where the snapshots/ directory would land,
	// so MkdirAll fails with "not a directory".
	blockPath := filepath.Join(dir, "snapshots")
	if err := os.WriteFile(blockPath, []byte("blocked"), 0o600); err != nil {
		t.Fatalf("plant block file: %v", err)
	}

	_, err := Open(dbPath)
	if err == nil {
		t.Fatal("Open succeeded; migration must abort when snapshot fails")
	}
	// Message must surface the env-var escape hatch so the operator knows
	// they have a path forward.
	if !strings.Contains(err.Error(), EnvNoMigrationSnapshot) {
		t.Errorf("error must name the opt-out env var; got: %v", err)
	}
	if !strings.Contains(err.Error(), "snapshot") {
		t.Errorf("error must mention snapshot in some form; got: %v", err)
	}

	// Schema must still be at the pre-failure version (v5) — the
	// migration was aborted before any change.
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var pre int
	if err := raw.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_versions`).Scan(&pre); err != nil {
		t.Fatal(err)
	}
	if pre != 5 {
		t.Errorf("schema must be at v5 after aborted migration; got v%d", pre)
	}
}

// =========================================================================
// VACUUM INTO contract — protects against a future maintainer replacing
// the snapshot with a naïve file copy
// =========================================================================

// TestSnapshot_CapturesWALActiveData pins the WAL/source-locking contract
// that the snapshot code depends on. In WAL mode (Continuity's default —
// see configurePragmas), committed data may live in the <path>-wal sidecar
// until a checkpoint moves it into the main .db file. A file-level copy
// (os.Rename, io.Copy, `cp`, `tar`) of <path> alone would silently miss
// that data — the snapshot would look intact but be missing the most
// recent writes.
//
// VACUUM INTO routes through the SQLite engine and walks a consistent
// transaction view that includes the WAL. This test demonstrates the
// difference would matter: a WAL-resident row written via a held-open
// connection MUST appear in the snapshot. If a future maintainer
// replaces VACUUM INTO with a file copy, this test fails — and the
// comment on snapshotBeforeRiskyMigration explains why before the
// reviewer even reads the test.
func TestSnapshot_CapturesWALActiveData(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8) // pre-v9 so v9 (risky) triggers the snapshot

	// Open a keeper connection in WAL mode and write a row. Keep the
	// connection OPEN so its close-time checkpoint does not quietly
	// migrate the row into the main DB file before the snapshot runs.
	// The row must still be WAL-resident when the snapshot is taken.
	keeper, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("keeper open: %v", err)
	}
	defer keeper.Close()

	for _, pragma := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
	} {
		if _, err := keeper.Exec(pragma); err != nil {
			t.Fatalf("%s: %v", pragma, err)
		}
	}

	const walResidentURI = "mem://user/events/wal-resident-row"
	const walResidentBody = "lives in WAL until checkpoint"
	if _, err := keeper.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
		VALUES (?, 'leaf', 'events', ?, 1000, 1000)
	`, walResidentURI, walResidentBody); err != nil {
		t.Fatalf("insert WAL-resident row: %v", err)
	}

	// Sanity precondition: the WAL sidecar should exist now. If it doesn't,
	// the runtime SQLite checkpointed eagerly and the WAL path isn't
	// being exercised — skip rather than pass for the wrong reason.
	if info, err := os.Stat(dbPath + "-wal"); err != nil || info.Size() == 0 {
		t.Skipf("WAL sidecar not populated (err=%v); environment did not exercise the WAL path", err)
	}

	// Trigger the production snapshot via store.Open → migrate() → v9 →
	// VACUUM INTO. The keeper connection stays open during this, which
	// is the realistic scenario (a user's hooks may be holding connections
	// when serve restarts and migrates).
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open during keeper hold: %v", err)
	}
	defer db.Close()

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		t.Fatalf("ListMigrationSnapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot from v9 migration, got %d", len(snaps))
	}

	// The load-bearing assertion: a row that was WAL-resident at snapshot
	// time MUST land in the snapshot file. A naïve file copy of the .db
	// would have produced a snapshot missing this row.
	snap, err := sql.Open("sqlite", snaps[0].Path)
	if err != nil {
		t.Fatalf("open snapshot file: %v", err)
	}
	defer snap.Close()

	var gotBody string
	err = snap.QueryRow(
		`SELECT l0_abstract FROM mem_nodes WHERE uri = ?`,
		walResidentURI,
	).Scan(&gotBody)
	if err == sql.ErrNoRows {
		t.Fatal("WAL-resident row missing from snapshot — VACUUM INTO did not " +
			"capture WAL state. This is the failure mode a future replacement of " +
			"VACUUM INTO with a file copy would produce. See the comment block on " +
			"snapshotBeforeRiskyMigration for why VACUUM INTO is load-bearing here.")
	}
	if err != nil {
		t.Fatalf("read WAL row from snapshot: %v", err)
	}
	if gotBody != walResidentBody {
		t.Errorf("WAL row content mangled in snapshot: got %q, want %q", gotBody, walResidentBody)
	}
}

// =========================================================================
// Content fidelity
// =========================================================================

// TestSnapshot_ContentMatchesPreMigrationState seeds a row at v5, runs the
// upgrade, and opens the snapshot file directly — the snapshot must be a
// valid SQLite DB AT THE PRE-MIGRATION SCHEMA, with the seeded row intact.
// This is what makes restoration meaningful: the operator can cp the
// snapshot over the live DB and get back to a working state.
func TestSnapshot_ContentMatchesPreMigrationState(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 5)

	// Seed a v5-era row directly.
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
		VALUES ('mem://user/events/pre-migration', 'leaf', 'events', 'lands in the snapshot', 1000, 1000)
	`)
	raw.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Now run the upgrade.
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 retained snapshot, got %d", len(snaps))
	}

	// Open the snapshot file as a regular SQLite DB and read back the
	// row. If the snapshot is corrupted, this open or query fails.
	snap, err := sql.Open("sqlite", snaps[0].Path)
	if err != nil {
		t.Fatalf("snapshot is not a readable SQLite DB: %v", err)
	}
	defer snap.Close()

	var abstract string
	if err := snap.QueryRow(`SELECT l0_abstract FROM mem_nodes WHERE uri = ?`,
		"mem://user/events/pre-migration").Scan(&abstract); err != nil {
		t.Fatalf("seeded row missing from snapshot: %v", err)
	}
	if abstract != "lands in the snapshot" {
		t.Errorf("seeded row mangled: got %q", abstract)
	}

	// Snapshot's schema must be the PRE-v9 schema. Specifically: the
	// feedback category was added in v9, so an attempt to insert a
	// feedback row into the snapshot would fail under the v8-era CHECK
	// constraint.
	_, err = snap.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, created_at, updated_at)
		VALUES ('mem://user/feedback/post-snapshot', 'leaf', 'feedback', 1000, 1000)
	`)
	// We seeded at v5; the snapshot was taken right before v6, so feedback
	// (added v9) would fail. If it succeeded, the snapshot got post-v9
	// content somehow, meaning the snapshot was taken AFTER instead of
	// before.
	if err == nil {
		t.Error("snapshot accepted post-v9 category; was taken AFTER migration instead of BEFORE")
	}
}

// =========================================================================
// Retention
// =========================================================================

// TestSnapshot_OnlyMostRecentRetained pins the "single snapshot" policy:
// after v5→v9 (two risky migrations: v6 and v9), exactly one snapshot
// remains. The pre-v6 snapshot is replaced by the pre-v9 one when v9
// runs.
func TestSnapshot_OnlyMostRecentRetained(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 5)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Errorf("expected exactly 1 retained snapshot row, got %d: %+v", len(snaps), snaps)
	}
	if len(snaps) > 0 && snaps[0].TargetVersion != 9 {
		t.Errorf("retained snapshot should be pre-v9; got target_version=%d", snaps[0].TargetVersion)
	}

	// File system should match: one file in this DB's snapshots/<db>/ dir.
	files, _ := os.ReadDir(filepath.Join(dir, "snapshots", "test.db"))
	if len(files) != 1 {
		t.Errorf("expected 1 file in snapshots/test.db/, got %d", len(files))
	}
}

// TestSnapshot_RetentionDeletesAfterNBoots pins the auto-delete contract:
// after SnapshotRetentionBoots successful boot ticks, the snapshot file
// AND its tracking row are gone.
func TestSnapshot_RetentionDeletesAfterNBoots(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8) // v9 is risky, so this triggers exactly one snapshot

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, _ := db.ListMigrationSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("setup: expected 1 snapshot row, got %d", len(snaps))
	}
	snapPath := snaps[0].Path
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("setup: snapshot file missing: %v", err)
	}

	for i := 0; i < SnapshotRetentionBoots; i++ {
		if err := db.TickSnapshotRetention(); err != nil {
			t.Fatalf("tick %d: %v", i+1, err)
		}
	}

	// After N ticks the snapshot should be gone — file and row.
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Errorf("snapshot file should have been deleted after %d boots; stat: %v",
			SnapshotRetentionBoots, err)
	}
	after, _ := db.ListMigrationSnapshots()
	if len(after) != 0 {
		t.Errorf("snapshot row should have been deleted after %d boots; got %+v",
			SnapshotRetentionBoots, after)
	}
}

// TestSnapshot_RetentionLeavesSnapshotBeforeThreshold is the boundary
// guard: TickSnapshotRetention must NOT delete a snapshot until it has
// actually crossed the threshold. Off-by-one here would mean snapshots
// disappear after N-1 boots, shortening the safety window.
func TestSnapshot_RetentionLeavesSnapshotBeforeThreshold(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < SnapshotRetentionBoots-1; i++ {
		if err := db.TickSnapshotRetention(); err != nil {
			t.Fatal(err)
		}
	}
	snaps, _ := db.ListMigrationSnapshots()
	if len(snaps) != 1 {
		t.Errorf("snapshot must persist for at least %d ticks; got %d remaining",
			SnapshotRetentionBoots, len(snaps))
	}
	if len(snaps) > 0 && snaps[0].BootsSince != SnapshotRetentionBoots-1 {
		t.Errorf("boots_since = %d, want %d after %d ticks",
			snaps[0].BootsSince, SnapshotRetentionBoots-1, SnapshotRetentionBoots-1)
	}
}

// TestSnapshot_RetentionKeepsRowWhenRemoveFails pins the failure-path contract:
// when os.Remove can't delete an expired snapshot file (permission denied, a DB
// held open on Windows, etc.), TickSnapshotRetention must KEEP the tracking row
// so the file stays visible to `snapshot list/prune` and a later tick can retry.
// Dropping the row on a failed remove would strand the file on disk, untracked
// and unreclaimable — a silent leak of exactly the data we promised to manage.
func TestSnapshot_RetentionKeepsRowWhenRemoveFails(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8) // v9 is risky → one snapshot recorded

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, _ := db.ListMigrationSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("setup: expected 1 snapshot row, got %d", len(snaps))
	}
	snapPath := snaps[0].Path

	// Make the snapshot path undeletable in a portable way: replace the file
	// with a NON-EMPTY directory. os.Remove on a non-empty dir fails with a
	// non-IsNotExist error (ENOTEMPTY) on every OS — no permission games.
	if err := os.Remove(snapPath); err != nil {
		t.Fatalf("setup: remove real snapshot file: %v", err)
	}
	if err := os.MkdirAll(snapPath, 0o700); err != nil {
		t.Fatalf("setup: mkdir at snapshot path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapPath, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: write blocker file: %v", err)
	}

	// Tick past the threshold. Each tick will try (and fail) to remove the
	// "file"; the row must survive every time.
	for i := 0; i < SnapshotRetentionBoots+1; i++ {
		if err := db.TickSnapshotRetention(); err != nil {
			t.Fatalf("tick %d returned error; a failed os.Remove must not fail the tick: %v", i+1, err)
		}
	}

	after, _ := db.ListMigrationSnapshots()
	if len(after) != 1 {
		t.Fatalf("row must survive while its file can't be deleted; got %d rows", len(after))
	}
	if after[0].Path != snapPath {
		t.Errorf("surviving row path = %q, want %q", after[0].Path, snapPath)
	}
}

// =========================================================================
// Permissions
// =========================================================================

// TestSnapshot_FilePermissions pins that the snapshot file and its parent
// directory are tightened to user-only. The snapshot contains the same
// data as the DB; its permissions must match.
func TestSnapshot_FilePermissions(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, _ := db.ListMigrationSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("setup: expected 1 snapshot, got %d", len(snaps))
	}

	dirInfo, err := os.Stat(filepath.Join(dir, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm()&0o077 != 0 {
		t.Errorf("snapshots/ permissions too loose: %v", dirInfo.Mode().Perm())
	}

	fileInfo, err := os.Stat(snaps[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Mode().Perm()&0o077 != 0 {
		t.Errorf("snapshot file permissions too loose: %v", fileInfo.Mode().Perm())
	}
}

// =========================================================================
// CLI-surface helpers (List / Prune)
// =========================================================================

// TestSnapshot_ListReturnsRecordedFields pins the fields ListMigrationSnapshots
// exposes — these drive the `continuity snapshot list` output. Drift here
// shows up as wrong info in the operator's terminal.
func TestSnapshot_ListReturnsRecordedFields(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("setup: expected 1 snapshot, got %d", len(snaps))
	}
	s := snaps[0]
	if s.Path == "" || !strings.Contains(s.Path, "snapshots") {
		t.Errorf("Path = %q", s.Path)
	}
	if s.PreVersion != 8 {
		t.Errorf("PreVersion = %d, want 8", s.PreVersion)
	}
	if s.TargetVersion != 9 {
		t.Errorf("TargetVersion = %d, want 9", s.TargetVersion)
	}
	if s.CreatedAt.IsZero() {
		t.Errorf("CreatedAt is zero")
	}
	if s.BootsSince != 0 {
		t.Errorf("BootsSince = %d on fresh snapshot, want 0", s.BootsSince)
	}
}

// TestSnapshot_PruneRemovesEverything pins the destructive contract of
// PruneMigrationSnapshots: file gone, row gone, return count correct.
func TestSnapshot_PruneRemovesEverything(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, _ := db.ListMigrationSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("setup: expected 1 snapshot, got %d", len(snaps))
	}
	snapPath := snaps[0].Path

	removed, err := db.PruneMigrationSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("prune count = %d, want 1", removed)
	}

	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Errorf("snapshot file should be gone after prune; stat: %v", err)
	}
	after, _ := db.ListMigrationSnapshots()
	if len(after) != 0 {
		t.Errorf("snapshot rows should be cleared; got %+v", after)
	}
}

// TestSnapshot_PruneNoOpOnEmpty pins that prune on an empty state is
// safe and returns 0 — the `continuity snapshot prune` CLI surface
// relies on this for its "no snapshots to prune" message.
func TestSnapshot_PruneNoOpOnEmpty(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	n, err := db.PruneMigrationSnapshots()
	if err != nil {
		t.Errorf("prune on empty: %v", err)
	}
	if n != 0 {
		t.Errorf("prune on empty returned %d", n)
	}
}

// TestSnapshot_PruneReclaimsUntrackedFiles pins that prune scans the snapshots/
// directory and removes our snapshot files that have no tracking row — the
// stranded-file case that arises when recording fails after a migration commits
// or a superseded-file delete fails. Tracked-only pruning would leak these full
// DB copies forever, contradicting prune's promise to reclaim the safety net.
func TestSnapshot_PruneReclaimsUntrackedFiles(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8) // v9 risky → one tracked snapshot

	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snaps, _ := db.ListMigrationSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("setup: expected 1 tracked snapshot, got %d", len(snaps))
	}

	// Strand an untracked snapshot file in THIS DB's snapshots/<db>/ dir — a
	// real DB copy with no row, plus a foreign file prune must NOT touch.
	snapDir := filepath.Join(dir, "snapshots", "test.db")
	orphan := filepath.Join(snapDir, "continuity-pre-v6-2020-01-01T00-00-00Z.db")
	if err := os.WriteFile(orphan, []byte("orphaned snapshot copy"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	foreign := filepath.Join(snapDir, "operator-notes.txt")
	if err := os.WriteFile(foreign, []byte("do not delete"), 0o600); err != nil {
		t.Fatalf("write foreign: %v", err)
	}

	removed, err := db.PruneMigrationSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	// Tracked snapshot + the orphan = 2.
	if removed != 2 {
		t.Errorf("prune count = %d, want 2 (1 tracked + 1 untracked)", removed)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("untracked snapshot file should be reclaimed; stat: %v", err)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Errorf("prune must not touch non-snapshot files; foreign file gone: %v", err)
	}
	after, _ := db.ListMigrationSnapshots()
	if len(after) != 0 {
		t.Errorf("rows should be cleared; got %+v", after)
	}
}

// TestSnapshot_PruneViaNoMigrateOpenDoesNotMigrate pins the New-1 contract:
// inspecting/cleaning snapshots must not upgrade the schema. Opening a
// pre-head DB with OpenNoMigrate and pruning must NOT run the pending risky
// migration — which would manufacture a snapshot and then immediately delete
// it, silently discarding the only rollback point.
func TestSnapshot_PruneViaNoMigrateOpenDoesNotMigrate(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	buildSnapshotDBAtVersion(t, dbPath, 8) // pre-v9; v9 is risky

	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// No migration ran on open, so schema is still v8.
	if v, err := db.SchemaVersion(); err != nil || v != 8 {
		t.Fatalf("schema version = %d (err %v); OpenNoMigrate must not migrate (want 8)", v, err)
	}

	n, err := db.PruneMigrationSnapshots()
	if err != nil {
		t.Fatalf("prune on never-migrated DB must tolerate missing sidecar table: %v", err)
	}
	if n != 0 {
		t.Errorf("prune removed %d; a pre-migration DB has no snapshots to remove", n)
	}

	// Crucially: no snapshots/ directory full of a just-created-then-deleted
	// snapshot. The schema stayed put, so no risky migration fired.
	if entries, err := os.ReadDir(filepath.Join(dir, "snapshots")); err == nil && len(entries) > 0 {
		t.Errorf("prune manufactured snapshots via migration: %d file(s) in snapshots/", len(entries))
	}
	if v, _ := db.SchemaVersion(); v != 8 {
		t.Errorf("schema advanced to v%d during prune; must stay v8", v)
	}
}

// TestSnapshot_PruneIsolatedAcrossSiblingDBs pins the cross-DB safety contract:
// two databases sharing a parent directory each keep their snapshots under
// snapshots/<db-filename>/, so pruning one must never reach the other's
// rollback point. Without per-DB namespacing, the untracked-file scan would
// see the sibling's snapshots as "untracked" and delete them.
func TestSnapshot_PruneIsolatedAcrossSiblingDBs(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir() // shared parent for both DBs
	dbAPath := filepath.Join(dir, "a.db")
	dbBPath := filepath.Join(dir, "b.db")
	buildSnapshotDBAtVersion(t, dbAPath, 8)
	buildSnapshotDBAtVersion(t, dbBPath, 8)

	dbA, err := Open(dbAPath) // v9 risky → snapshot under snapshots/a.db/
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := Open(dbBPath) // snapshot under snapshots/b.db/
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	bSnaps, _ := dbB.ListMigrationSnapshots()
	if len(bSnaps) != 1 {
		t.Fatalf("setup: DB B should have 1 snapshot, got %d", len(bSnaps))
	}
	bSnapPath := bSnaps[0].Path

	// Prune A. B must be completely untouched — file AND row.
	if _, err := dbA.PruneMigrationSnapshots(); err != nil {
		t.Fatalf("prune A: %v", err)
	}
	if _, err := os.Stat(bSnapPath); err != nil {
		t.Errorf("pruning DB A destroyed DB B's snapshot file: %v", err)
	}
	if after, _ := dbB.ListMigrationSnapshots(); len(after) != 1 {
		t.Errorf("pruning DB A cleared DB B's tracking row; B now has %d", len(after))
	}
}

// TestSnapshot_ListSurfacesRealQueryErrors pins that ListMigrationSnapshots
// distinguishes "table absent" (tolerated → empty) from a genuine query
// failure (surfaced). A catch-all that reported a corrupt/incompatible table
// as empty would make prune treat still-tracked files as untracked.
func TestSnapshot_ListSurfacesRealQueryErrors(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Replace the sidecar with an incompatible shape: the table EXISTS but
	// lacks the expected columns, so the real SELECT fails.
	if _, err := db.Exec(`DROP TABLE migration_snapshots`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE migration_snapshots (unexpected INTEGER)`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ListMigrationSnapshots(); err == nil {
		t.Fatal("ListMigrationSnapshots must surface a real query error, not report empty")
	}
	// Prune must propagate the error and NOT proceed to delete anything.
	if _, err := db.PruneMigrationSnapshots(); err == nil {
		t.Error("PruneMigrationSnapshots must refuse to run when the snapshot table is unreadable")
	}
}

// TestSnapshot_DeletesNeverEscapeNamespace pins the deletion-side cross-DB
// guard. Tracking rows store absolute paths, so a database COPIED or RENAMED
// from another (the repo's own smoke test does `cp continuity.db scratch.db`)
// inherits rows pointing at the ORIGINAL's snapshot files. Neither prune nor
// the retention tick may unlink a file outside this DB's own namespace, no
// matter what a stale row claims.
func TestSnapshot_DeletesNeverEscapeNamespace(t *testing.T) {
	t.Setenv(EnvNoMigrationSnapshot, "")

	dir := t.TempDir()
	origPath := filepath.Join(dir, "continuity.db")
	buildSnapshotDBAtVersion(t, origPath, 8)
	orig, err := Open(origPath) // v9 risky → snapshot under snapshots/continuity.db/
	if err != nil {
		t.Fatal(err)
	}
	origSnaps, _ := orig.ListMigrationSnapshots()
	if len(origSnaps) != 1 {
		t.Fatalf("setup: orig should have 1 snapshot, got %d", len(origSnaps))
	}
	origSnapPath := origSnaps[0].Path

	// Two consistent copies (VACUUM INTO), each inheriting orig's tracking row.
	pruneCopy := filepath.Join(dir, "prune-scratch.db")
	tickCopy := filepath.Join(dir, "tick-scratch.db")
	if _, err := orig.Exec("VACUUM INTO ?", pruneCopy); err != nil {
		t.Fatal(err)
	}
	if _, err := orig.Exec("VACUUM INTO ?", tickCopy); err != nil {
		t.Fatal(err)
	}
	orig.Close()

	// Prune the copy: clears its stale row but must NOT unlink orig's file.
	pc, err := OpenNoMigrate(pruneCopy)
	if err != nil {
		t.Fatal(err)
	}
	if ss, _ := pc.ListMigrationSnapshots(); len(ss) != 1 || ss[0].Path != origSnapPath {
		t.Fatalf("setup: prune copy should carry orig's stale row pointing at %s", origSnapPath)
	}
	if _, err := pc.PruneMigrationSnapshots(); err != nil {
		t.Fatal(err)
	}
	if after, _ := pc.ListMigrationSnapshots(); len(after) != 0 {
		t.Errorf("copy's stale row should be cleared; got %d", len(after))
	}
	pc.Close()
	if _, err := os.Stat(origSnapPath); err != nil {
		t.Errorf("prune on a copied DB deleted the ORIGINAL's snapshot: %v", err)
	}

	// Retention tick on the other copy: drive past threshold; same guarantee.
	tc, err := OpenNoMigrate(tickCopy)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < SnapshotRetentionBoots+1; i++ {
		if err := tc.TickSnapshotRetention(); err != nil {
			t.Fatal(err)
		}
	}
	tc.Close()
	if _, err := os.Stat(origSnapPath); err != nil {
		t.Errorf("retention tick on a copied DB deleted the ORIGINAL's snapshot: %v", err)
	}
}
