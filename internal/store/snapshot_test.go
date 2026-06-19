//go:build !windows

package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// buildDBAtVersionStandalone applies migrations [1..target] to a fresh DB at
// the given path. Mirrors the e2e harness's buildDBAtVersion but takes an
// explicit path (not a dir) so a test can place sibling DBs precisely.
func buildDBAtVersionStandalone(t *testing.T, path string, target int) {
	t.Helper()
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
			applied_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)
		)`); err != nil {
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
		if _, err := tx.Exec(`INSERT INTO schema_versions (version, description) VALUES (?, ?)`,
			m.Version, m.Description); err != nil {
			tx.Rollback()
			t.Fatalf("record v%d: %v", m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit v%d: %v", m.Version, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Sidecar path canonicalization
// ---------------------------------------------------------------------------

func TestSidecarPath_RelativeAbsoluteSame(t *testing.T) {
	dir := t.TempDir()
	abs := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, abs, 5)

	got1, err := sidecarPath(abs)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}

	// Relative spelling from the DB's own directory must resolve identically.
	cwd, _ := os.Getwd()
	defer os.Chdir(cwd)
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	got2, err := sidecarPath("continuity.db")
	if err != nil {
		t.Fatalf("rel: %v", err)
	}
	if got1 != got2 {
		t.Errorf("relative vs absolute sidecar differ:\n abs=%s\n rel=%s", got1, got2)
	}
	if filepath.Base(got1) != "continuity.db.snapshot" {
		t.Errorf("unexpected sidecar basename: %s", got1)
	}
}

func TestSidecarPath_SymlinkedDBResolves(t *testing.T) {
	realDir := t.TempDir()
	realDB := filepath.Join(realDir, "real.db")
	buildDBAtVersionStandalone(t, realDB, 5)

	linkDir := t.TempDir()
	link := filepath.Join(linkDir, "link.db")
	if err := os.Symlink(realDB, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	viaLink, err := sidecarPath(link)
	if err != nil {
		t.Fatalf("via link: %v", err)
	}
	viaReal, err := sidecarPath(realDB)
	if err != nil {
		t.Fatalf("via real: %v", err)
	}
	// EvalSymlinks should make the symlinked DB resolve to the real path's
	// sidecar.
	if viaLink != viaReal {
		t.Errorf("symlinked DB sidecar differs from real:\n link=%s\n real=%s", viaLink, viaReal)
	}
}

func TestSidecarPath_SiblingDBsIndependent(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.db")
	b := filepath.Join(dir, "b.db")
	buildDBAtVersionStandalone(t, a, 5)
	buildDBAtVersionStandalone(t, b, 5)
	sa, _ := sidecarPath(a)
	sb, _ := sidecarPath(b)
	if sa == sb {
		t.Fatalf("sibling DBs share a sidecar: %s", sa)
	}
}

func TestSidecarPath_RejectsMemoryAndDSN(t *testing.T) {
	for _, p := range []string{":memory:", "file:foo.db?cache=shared", "/tmp/x.db?mode=ro"} {
		if _, err := sidecarPath(p); !errors.Is(err, ErrSnapshotUnsupportedPath) {
			t.Errorf("sidecarPath(%q) err = %v, want ErrSnapshotUnsupportedPath", p, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Manifest validation
// ---------------------------------------------------------------------------

func TestManifestValidateShape_RejectsBadFields(t *testing.T) {
	good := Manifest{
		Kind:                manifestKind,
		FormatVersion:       manifestFormatVersion,
		SnapshotFile:        snapshotFileName,
		PreSchemaVersion:    5,
		TargetSchemaVersion: 9,
		LineageFingerprint:  "sha256:aa",
		SnapshotSHA256:      "sha256:bb",
	}
	if err := good.validateShape(); err != nil {
		t.Fatalf("good manifest rejected: %v", err)
	}

	type mut func(*Manifest)
	cases := map[string]mut{
		"wrong kind":         func(m *Manifest) { m.Kind = "something.else" },
		"wrong version":      func(m *Manifest) { m.FormatVersion = 2 },
		"absolute file":      func(m *Manifest) { m.SnapshotFile = "/etc/passwd" },
		"dotdot file":        func(m *Manifest) { m.SnapshotFile = "../snapshot.db" },
		"separator file":     func(m *Manifest) { m.SnapshotFile = "sub/snapshot.db" },
		"non-canonical file": func(m *Manifest) { m.SnapshotFile = "other.db" },
		"zero pre":           func(m *Manifest) { m.PreSchemaVersion = 0 },
		"target below pre":   func(m *Manifest) { m.TargetSchemaVersion = 4 },
		"empty fingerprint":  func(m *Manifest) { m.LineageFingerprint = "" },
		"empty hash":         func(m *Manifest) { m.SnapshotSHA256 = "" },
	}
	for name, mutate := range cases {
		m := good
		mutate(&m)
		if err := m.validateShape(); err == nil {
			t.Errorf("%s: expected validateShape to reject, got nil", name)
		} else if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
			t.Errorf("%s: err = %v, want ErrSnapshotSidecarCorrupt", name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Lineage fingerprint
// ---------------------------------------------------------------------------

func TestLineageFingerprint_StableAcrossCopy_MismatchUnrelated(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.db")
	buildDBAtVersionStandalone(t, a, 5)

	dbA, err := OpenNoMigrate(a)
	if err != nil {
		t.Fatal(err)
	}
	fpA1, err := lineageFingerprint(dbA, 5)
	dbA.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Copy a.db → c.db; fingerprint must be identical (same schema history).
	c := filepath.Join(dir, "c.db")
	if err := copyFile(a, c); err != nil {
		t.Fatal(err)
	}
	dbC, err := OpenNoMigrate(c)
	if err != nil {
		t.Fatal(err)
	}
	fpC, err := lineageFingerprint(dbC, 5)
	dbC.Close()
	if err != nil {
		t.Fatal(err)
	}
	if fpA1 != fpC {
		t.Errorf("fingerprint changed across copy:\n a=%s\n c=%s", fpA1, fpC)
	}

	// An UNRELATED DB built independently still has the same fixed migration
	// descriptions, so its fingerprint matches by design — the lineage check
	// guards against a *different schema history*, which our deterministic
	// migrations don't produce. To prove mismatch sensitivity, tamper a
	// description row.
	b := filepath.Join(dir, "b.db")
	buildDBAtVersionStandalone(t, b, 5)
	dbB, err := sql.Open("sqlite", b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dbB.Exec(`UPDATE schema_versions SET description = 'tampered' WHERE version = 3`); err != nil {
		t.Fatal(err)
	}
	dbB.Close()
	dbB2, err := OpenNoMigrate(b)
	if err != nil {
		t.Fatal(err)
	}
	fpB, err := lineageFingerprint(dbB2, 5)
	dbB2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if fpB == fpA1 {
		t.Errorf("tampered DB produced identical fingerprint; lineage check is insensitive")
	}
}

// ---------------------------------------------------------------------------
// Snapshot-once-per-run planning
// ---------------------------------------------------------------------------

func TestFirstPendingRiskyVersion(t *testing.T) {
	// From v5, the first pending risky migration is v6.
	if v, ok := firstPendingRiskyVersion(5); !ok || v != 6 {
		t.Errorf("firstPendingRiskyVersion(5) = (%d,%v), want (6,true)", v, ok)
	}
	// From v6, v6 is already applied; next risky is v9.
	if v, ok := firstPendingRiskyVersion(6); !ok || v != 9 {
		t.Errorf("firstPendingRiskyVersion(6) = (%d,%v), want (9,true)", v, ok)
	}
	// From v9 (head), no pending risky migration.
	if _, ok := firstPendingRiskyVersion(9); ok {
		t.Errorf("firstPendingRiskyVersion(9) = ok, want no pending risky")
	}
}

// TestMigrateCreatesRestorePointOnce verifies a single restore point is
// captured before the first risky migration, and that it records
// pre_schema_version=5 / first_risky=6 even though the run also crosses v9.
func TestMigrateCreatesRestorePointOnce(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	db, err := Open(dbPath) // runs migrate() → should snapshot pre-v5
	if err != nil {
		t.Fatalf("Open/migrate: %v", err)
	}
	defer db.Close()

	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	m, err := loadValidManifest(sidecar)
	if err != nil {
		t.Fatalf("expected valid restore point: %v", err)
	}
	if m.PreSchemaVersion != 5 {
		t.Errorf("pre_schema_version = %d, want 5 (snapshot must be pre-upgrade, not pre-v9)", m.PreSchemaVersion)
	}
	if m.FirstRiskySchemaVersion != 6 {
		t.Errorf("first_risky_schema_version = %d, want 6", m.FirstRiskySchemaVersion)
	}
	if m.TargetSchemaVersion != headVersion() {
		t.Errorf("target_schema_version = %d, want %d", m.TargetSchemaVersion, headVersion())
	}

	// The snapshot image itself must be at v5.
	sv, err := snapshotSchemaVersion(snapshotDBPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}
	if sv != 5 {
		t.Errorf("snapshot image schema = v%d, want v5", sv)
	}
}

// TestFreshInstallNoRestorePoint: a brand-new DB migrated straight to head
// must NOT create a meaningless empty restore point.
func TestFreshInstallNoRestorePoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "fresh.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sidecar, _ := sidecarPath(dbPath)
	if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
		t.Errorf("fresh install created a sidecar at %s (err=%v)", sidecar, err)
	}
}

// TestExistingValidManifestReused: a second migrate run that still has a
// pending risky migration must REUSE the existing manifest, never overwrite.
func TestExistingValidManifestReused(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	// First, manually build a v5 DB and create a restore point, then roll the
	// DB forward only to v6 (still risky-pending: v9). Simulate by opening at
	// v5 and directly invoking createRestorePoint, then mutating boots, then
	// re-running create to assert no overwrite.
	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
		db.Close()
		t.Fatalf("first createRestorePoint: %v", err)
	}

	// Mutate the manifest to a sentinel boot count to detect overwrite.
	m, err := loadValidManifest(sidecar)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	m.SuccessfulBoots = 2
	if err := writeManifestAtomic(sidecar, m); err != nil {
		db.Close()
		t.Fatal(err)
	}

	// Second create with the same lineage must reuse (no-op), preserving boots=2.
	if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
		db.Close()
		t.Fatalf("second createRestorePoint (reuse): %v", err)
	}
	db.Close()

	m2, err := loadValidManifest(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	if m2.SuccessfulBoots != 2 {
		t.Errorf("manifest was overwritten on reuse: boots=%d, want 2", m2.SuccessfulBoots)
	}
}

// TestCorruptSidecarFailsClosed: a sidecar with a snapshot.db but a corrupt
// manifest must make createRestorePoint fail closed (never overwrite).
func TestCorruptSidecarFailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	// Write a snapshot.db and a garbage manifest.
	if err := os.WriteFile(snapshotDBPathIn(sidecar), []byte("not a db"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPathIn(sidecar), []byte("{ not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.createRestorePoint(5, headVersion(), 6)
	if err == nil {
		t.Fatal("expected createRestorePoint to fail closed on corrupt sidecar")
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", err)
	}
}

// TestMigrateFailsClosedOnCorruptSidecar: the full migrate() path must abort
// before applying any pending migration when a corrupt sidecar blocks the
// restore point, leaving the schema at v5.
func TestMigrateFailsClosedOnCorruptSidecar(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotDBPathIn(sidecar), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	// manifest absent → partial/unknown → fail closed.

	_, err := Open(dbPath)
	if err == nil {
		t.Fatal("expected Open to fail closed on corrupt sidecar")
	}

	// Schema must still be v5 — no pending migration ran.
	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, _ := db.SchemaVersion()
	if v != 5 {
		t.Errorf("schema advanced to v%d despite fail-closed; want v5", v)
	}
}

// TestOptOutSkipsSnapshot: with the opt-out env set, migrate proceeds without
// a restore point and reaches head.
func TestOptOutSkipsSnapshot(t *testing.T) {
	t.Setenv(envDisableSnapshot, "1")
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open with opt-out: %v", err)
	}
	defer db.Close()
	v, _ := db.SchemaVersion()
	if v != headVersion() {
		t.Errorf("schema = v%d, want head v%d", v, headVersion())
	}
	sidecar, _ := sidecarPath(dbPath)
	if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
		t.Errorf("opt-out still created a sidecar (err=%v)", err)
	}
}

// ---------------------------------------------------------------------------
// Boot retention / expiry
// ---------------------------------------------------------------------------

func TestRecordSuccessfulBoot_IncrementsThenExpires(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	db, err := Open(dbPath) // creates restore point + migrates to head
	if err != nil {
		t.Fatal(err)
	}
	curV, _ := db.SchemaVersion()
	db.Close()

	sidecar, _ := sidecarPath(dbPath)
	if _, err := loadValidManifest(sidecar); err != nil {
		t.Fatalf("restore point should exist: %v", err)
	}

	// Two ticks: still present, boots increments.
	for i := 1; i <= 2; i++ {
		if err := RecordSuccessfulBoot(dbPath, curV); err != nil {
			t.Fatalf("boot %d: %v", i, err)
		}
		m, err := loadValidManifest(sidecar)
		if err != nil {
			t.Fatalf("after boot %d: %v", i, err)
		}
		if m.SuccessfulBoots != i {
			t.Errorf("after boot %d: successful_boots=%d, want %d", i, m.SuccessfulBoots, i)
		}
	}

	// Third tick hits the default threshold (3) → expiry deletes the two files.
	if err := RecordSuccessfulBoot(dbPath, curV); err != nil {
		t.Fatalf("expiry boot: %v", err)
	}
	if _, err := os.Stat(snapshotDBPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("snapshot.db still present after expiry (err=%v)", err)
	}
	if _, err := os.Stat(manifestPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("manifest.json still present after expiry (err=%v)", err)
	}
}

// TestExpiryLeavesUnprovenFiles: expiry must remove only snapshot.db +
// manifest.json, leaving any unrelated file (and thus the sidecar dir) intact.
func TestExpiryLeavesUnprovenFiles(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	curV, _ := db.SchemaVersion()
	db.Close()

	sidecar, _ := sidecarPath(dbPath)
	stray := filepath.Join(sidecar, "operator-note.txt")
	if err := os.WriteFile(stray, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := RecordSuccessfulBoot(dbPath, curV); err != nil {
			t.Fatalf("boot %d: %v", i, err)
		}
	}
	if _, err := os.Stat(stray); err != nil {
		t.Errorf("expiry removed an unproven file: %v", err)
	}
	if _, err := os.Stat(sidecar); err != nil {
		t.Errorf("sidecar dir removed despite stray file present: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Status / Prune
// ---------------------------------------------------------------------------

func TestStatusAndPrune(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	st, err := Status(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Present || st.Manifest == nil {
		t.Fatalf("status: expected present restore point, got %+v", st)
	}
	if st.Problem != "" {
		t.Errorf("status: unexpected problem %q", st.Problem)
	}

	if err := Prune(dbPath); err != nil {
		t.Fatalf("prune: %v", err)
	}
	st2, _ := Status(dbPath)
	if st2.Present {
		t.Errorf("status after prune: still present")
	}

	// Prune again → ErrNoRestorePoint.
	if err := Prune(dbPath); !errors.Is(err, ErrNoRestorePoint) {
		t.Errorf("prune empty: err=%v, want ErrNoRestorePoint", err)
	}
}

// TestPruneRefusesCorrupt: prune must fail closed on a corrupt sidecar.
func TestPruneRefusesCorrupt(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotDBPathIn(sidecar), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	// manifest absent → corrupt.
	err := Prune(dbPath)
	if err == nil || errors.Is(err, ErrNoRestorePoint) {
		t.Fatalf("prune corrupt: err=%v, want corrupt refusal", err)
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err=%v, want ErrSnapshotSidecarCorrupt", err)
	}
	// The partial snapshot must NOT have been deleted.
	if _, err := os.Stat(snapshotDBPathIn(sidecar)); err != nil {
		t.Errorf("prune deleted an unproven file: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

// TestRestore_RoundTripsData: seed v5 data, migrate to head, mutate the DB,
// then restore — the original v5 data returns and the mutation is gone. The
// previous DB triplet is moved aside, not deleted, and no stale -wal/-shm
// remain at the live names.
func TestRestore_RoundTripsData(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	// Seed a marker row at v5.
	{
		raw, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		now := int64(1000)
		if _, err := raw.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/v5-marker', 'leaf', 'events', 'pre-upgrade', ?, ?)`, now, now); err != nil {
			t.Fatal(err)
		}
		raw.Close()
	}

	// Migrate to head (creates restore point).
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate post-migration: add a row that should NOT survive restore.
	if _, err := db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
		VALUES ('mem://user/feedback/post-upgrade', 'leaf', 'feedback', 'should vanish', 1000, 1000)`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Restore.
	movedAside, err := Restore(dbPath)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if movedAside == "" {
		t.Error("restore returned empty moved-aside prefix")
	}
	if _, err := os.Stat(movedAside); err != nil {
		t.Errorf("moved-aside DB not found at %s: %v", movedAside, err)
	}
	// No stale wal/shm at live names.
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(dbPath + suffix); !os.IsNotExist(err) {
			t.Errorf("stale %s remains at live name (err=%v)", suffix, err)
		}
	}

	// The restored DB must be at v5 with the marker present and the
	// post-upgrade feedback row gone.
	rdb, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	v, _ := rdb.SchemaVersion()
	if v != 5 {
		t.Errorf("restored schema = v%d, want v5", v)
	}
	var marker string
	if err := rdb.QueryRow(`SELECT l0_abstract FROM mem_nodes WHERE uri='mem://user/events/v5-marker'`).Scan(&marker); err != nil {
		t.Fatalf("v5 marker lost after restore: %v", err)
	}
	if marker != "pre-upgrade" {
		t.Errorf("marker mangled: %q", marker)
	}
	var cnt int
	rdb.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE uri='mem://user/feedback/post-upgrade'`).Scan(&cnt)
	if cnt != 0 {
		t.Errorf("post-upgrade row survived restore (count=%d)", cnt)
	}
}

// TestRestore_RefusesLineageMismatch: a sidecar copied next to an unrelated DB
// must be refused on lineage fingerprint mismatch.
func TestRestore_RefusesLineageMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // creates a real sidecar at dbPath.snapshot
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)

	// Build an unrelated sibling DB and copy the sidecar next to it, then
	// tamper the sibling's lineage so it differs from the manifest.
	other := filepath.Join(dir, "other.db")
	buildDBAtVersionStandalone(t, other, 5)
	raw, _ := sql.Open("sqlite", other)
	raw.Exec(`UPDATE schema_versions SET description='tampered' WHERE version=2`)
	raw.Close()

	otherSidecar, _ := sidecarPath(other)
	if err := os.MkdirAll(otherSidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(snapshotDBPathIn(sidecar), snapshotDBPathIn(otherSidecar)); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(manifestPathIn(sidecar), manifestPathIn(otherSidecar)); err != nil {
		t.Fatal(err)
	}

	_, err = Restore(other)
	if err == nil {
		t.Fatal("expected restore to refuse on lineage mismatch")
	}
	if !contains(err.Error(), "lineage") {
		t.Errorf("err = %v, want lineage mismatch", err)
	}
}

// TestRestore_RefusesWhileServeLockHeld: a live serve lock blocks restore.
func TestRestore_RefusesWhileServeLockHeld(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Acquire a lock recording THIS process's PID (alive) → restore refuses.
	release, err := AcquireServeLock(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if _, err := Restore(dbPath); err == nil {
		t.Fatal("expected restore to refuse while serve lock held")
	} else if !contains(err.Error(), "serve") {
		t.Errorf("err = %v, want serve-lock refusal", err)
	}

	// A stale lock (dead PID) must NOT block restore.
	release()
	lp, _ := serveLockPath(dbPath)
	if err := os.WriteFile(lp, []byte("999999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(dbPath); err != nil {
		t.Errorf("restore refused despite stale (dead-PID) lock: %v", err)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
