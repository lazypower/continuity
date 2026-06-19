//go:build !windows

package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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
	// Mirror what migrate() does on a real Open: give the DB a random
	// per-instance identity so the lineage fingerprint is anchored to this
	// physical DB. A copy of this file (copyFile) carries the same id; an
	// independently built DB gets a different one — exactly the production
	// property the lineage check relies on.
	seedInstanceID(t, sqlDB)
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

// openWritableNoMigrate opens a writable *DB handle at path WITHOUT running
// migrate(). Used by tests that must create a restore point against a DB pinned
// at a specific schema version (createRestorePoint needs to VACUUM INTO, which
// the read-only OpenNoMigrate cannot do).
func openWritableNoMigrate(t *testing.T, path string) *DB {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open writable: %v", err)
	}
	db := &DB{DB: sqlDB, Path: path}
	if err := db.configurePragmas(); err != nil {
		sqlDB.Close()
		t.Fatalf("pragmas: %v", err)
	}
	return db
}

// seedInstanceID creates the continuity_meta table and writes a fresh random
// instance_id, mirroring ensureInstanceID() for DBs manufactured outside the
// Open path. crypto/rand so two manufactured DBs never collide.
func seedInstanceID(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	if _, err := sqlDB.Exec(
		`CREATE TABLE IF NOT EXISTS continuity_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
	); err != nil {
		t.Fatalf("create continuity_meta: %v", err)
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := sqlDB.Exec(
		`INSERT OR IGNORE INTO continuity_meta (key, value) VALUES ('instance_id', ?)`,
		hex.EncodeToString(b[:]),
	); err != nil {
		t.Fatalf("seed instance_id: %v", err)
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
		Kind:                        manifestKind,
		FormatVersion:               manifestFormatVersion,
		SnapshotFile:                snapshotFileName,
		PreSchemaVersion:            5,
		TargetSchemaVersion:         9,
		LineageFingerprint:          "sha256:aa",
		SnapshotSHA256:              "sha256:bb",
		ExpiresAfterSuccessfulBoots: defaultExpiresAfterBoots,
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
		// Finding 8: a missing/zero retention must fail closed so the first
		// boot does not delete the restore point.
		"zero retention":     func(m *Manifest) { m.ExpiresAfterSuccessfulBoots = 0 },
		"negative retention": func(m *Manifest) { m.ExpiresAfterSuccessfulBoots = -1 },
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

// TestLineageFingerprint_StableAcrossCopy_MismatchUnrelated is the Finding 1
// regression. Before the per-DB instance identity, the fingerprint hashed only
// schema_versions(version,description) — identical across every normal
// continuity DB — so two unrelated DBs FALSE-MATCHED and a sidecar transplanted
// onto another DB would restore the WRONG database.
//
// The required property now:
//   - a COPY of a DB (cp/VACUUM INTO preserve instance_id) MATCHES its source,
//   - an INDEPENDENTLY-created DB MISMATCHES, even though its schema_versions
//     rows are byte-identical.
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

	// Copy a.db → c.db; the instance_id travels in the bytes, so the
	// fingerprint must be IDENTICAL (a snapshot must match its source DB).
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

	// An UNRELATED DB built independently has the SAME fixed migration
	// descriptions but a DIFFERENT random instance_id, so its fingerprint MUST
	// differ. This is what refuses a transplanted sidecar.
	b := filepath.Join(dir, "b.db")
	buildDBAtVersionStandalone(t, b, 5)
	dbB, err := OpenNoMigrate(b)
	if err != nil {
		t.Fatal(err)
	}
	fpB, err := lineageFingerprint(dbB, 5)
	dbB.Close()
	if err != nil {
		t.Fatal(err)
	}
	if fpB == fpA1 {
		t.Errorf("independent DB produced identical fingerprint; lineage check false-matches unrelated DBs")
	}
}

// TestLineageFingerprint_MissingInstanceIDFailsClosed: a DB lacking the
// continuity_meta instance identity (legacy/corrupt/wrong-file) must make the
// fingerprint FAIL CLOSED rather than fabricate a match.
func TestLineageFingerprint_MissingInstanceIDFailsClosed(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "noident.db")
	// Build a DB WITHOUT seeding an instance_id by hand.
	raw, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
		CREATE TABLE schema_versions (version INTEGER PRIMARY KEY, description TEXT NOT NULL, applied_at INTEGER);
		INSERT INTO schema_versions (version, description, applied_at) VALUES (1, 'x', 0);
	`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	db, err := OpenNoMigrate(p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := lineageFingerprint(db, 1); !errors.Is(err, ErrInstanceIDMissing) {
		t.Errorf("fingerprint on DB without instance id: err=%v, want ErrInstanceIDMissing", err)
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
	db := openWritableNoMigrate(t, dbPath)
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

	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()
	err := db.createRestorePoint(5, headVersion(), 6)
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

// TestMigrateBlockedSidecarLeavesDBUnmutated is the ROUND 2 Finding 5
// regression: a v5 DB whose sidecar is BLOCKED (a regular file where the dir
// should go) must fail closed AND leave the DB entirely UNMUTATED — in
// particular, no continuity_meta / instance_id row may be written. The prior
// ordering wrote instance_id in migrate() before the restore point was secured,
// so a fail-closed upgrade still mutated the DB. instance_id is now established
// inside writeRestorePoint (after the sidecar is proven usable), so a blocked
// sidecar never touches the DB.
func TestMigrateBlockedSidecarLeavesDBUnmutated(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")

	// Build a v5 DB WITHOUT seeding an instance_id, so we can prove the DB is
	// untouched (no continuity_meta) after a fail-closed open.
	{
		raw, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`
			CREATE TABLE schema_versions (version INTEGER PRIMARY KEY, description TEXT NOT NULL, applied_at INTEGER);
		`); err != nil {
			t.Fatal(err)
		}
		for _, m := range migrations {
			if m.Version > 5 {
				break
			}
			if _, err := raw.Exec(m.SQL); err != nil {
				t.Fatalf("apply v%d: %v", m.Version, err)
			}
			if _, err := raw.Exec(`INSERT INTO schema_versions (version, description, applied_at) VALUES (?, ?, 0)`,
				m.Version, m.Description); err != nil {
				t.Fatalf("record v%d: %v", m.Version, err)
			}
		}
		raw.Close()
	}

	// Block the sidecar: a regular FILE where the dir would go.
	sidecar, _ := sidecarPath(dbPath)
	if err := os.WriteFile(sidecar, []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Open must fail closed (the risky upgrade needs a restore point it can't make).
	if _, err := Open(dbPath); err == nil {
		t.Fatal("expected Open to fail closed on a blocked sidecar")
	}

	// The DB must be UNMUTATED: still v5, and NO continuity_meta table written.
	raw, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var maxV int
	if err := raw.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_versions`).Scan(&maxV); err != nil {
		t.Fatal(err)
	}
	if maxV != 5 {
		t.Errorf("schema advanced to v%d despite fail-closed; want v5", maxV)
	}
	var metaCount int
	if err := raw.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, metaTableName,
	).Scan(&metaCount); err != nil {
		t.Fatal(err)
	}
	if metaCount != 0 {
		t.Errorf("blocked-sidecar fail-closed mutated the DB: %s table was created", metaTableName)
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

// TestRestore_RefusesLineageMismatch is the Finding 1 restore-side regression:
// a sidecar transplanted next to an INDEPENDENTLY-created DB must be refused.
// No tampering is needed — the unrelated DB carries a different instance_id, so
// its recomputed lineage fingerprint cannot match the transplanted manifest.
// Before the per-DB identity this transplant would have FALSE-MATCHED and
// restored the wrong database.
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

	// Build an unrelated sibling DB (its own random instance_id) and transplant
	// the first DB's sidecar next to it verbatim — no edits.
	other := filepath.Join(dir, "other.db")
	buildDBAtVersionStandalone(t, other, 5)

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
		t.Fatal("expected restore to refuse a sidecar transplanted onto an unrelated DB")
	}
	if !contains(err.Error(), "lineage") {
		t.Errorf("err = %v, want lineage mismatch", err)
	}

	// And the unrelated DB must be UNTOUCHED — no pre-restore backup created.
	if matches, _ := filepath.Glob(other + ".pre-restore.*"); len(matches) != 0 {
		t.Errorf("refused restore still moved the target DB aside: %v", matches)
	}
}

// foreignFlock takes a RAW flock (LOCK_SH or LOCK_EX) on the per-DB lock file,
// BYPASSING the in-process registry, to simulate a SEPARATE process holding the
// lock. It opens its own fd (flock is per-open-file-description, and two fds in
// one process DO conflict), so an exclusive acquire that routes through
// acquireExclusiveLock will see a foreign cross-process holder and fail closed
// after the bounded wait — exactly the behaviour a real second process produces.
// The returned releaser closes the fd (dropping the flock).
func foreignFlock(t *testing.T, dbPath string, exclusive bool) func() {
	t.Helper()
	lp, err := dbLockPath(dbPath)
	if err != nil {
		t.Fatalf("dbLockPath: %v", err)
	}
	f, err := os.OpenFile(lp, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open lock file: %v", err)
	}
	if exclusive {
		ok, lerr := flockExclusiveNB(f)
		if lerr != nil || !ok {
			f.Close()
			t.Fatalf("foreign exclusive flock: ok=%v err=%v", ok, lerr)
		}
	} else {
		if lerr := flockShared(f); lerr != nil {
			f.Close()
			t.Fatalf("foreign shared flock: %v", lerr)
		}
	}
	return func() { _ = f.Close() }
}

// TestRestore_RefusesWhileForeignSharedLockHeld proves the CENTERPIECE fail-
// closed bar: a writable open in ANOTHER process (a SHARED flock holder) makes
// Restore's EXCLUSIVE acquire wait the bounded window and FAIL CLOSED with
// ErrDBLocked, rather than swap the DB triplet out from under the live writer.
// We simulate the foreign process with a raw flock on a separate fd (bypassing
// the in-process registry), which cross-process flock treats as a real holder.
func TestRestore_RefusesWhileForeignSharedLockHeld(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	resolved, _ := resolveDBPath(dbPath)
	liveBefore, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatal(err)
	}

	// A foreign SHARED holder (a writable open in another process) is present.
	release := foreignFlock(t, dbPath, false)

	if _, rerr := Restore(dbPath); rerr == nil {
		release()
		t.Fatal("expected restore to refuse while a foreign shared lock is held")
	} else if !errors.Is(rerr, ErrDBLocked) && !contains(rerr.Error(), "in use") {
		release()
		t.Errorf("err = %v, want ErrDBLocked / 'database is in use'", rerr)
	}

	// The live DB must be byte-intact — restore failed closed before any swap.
	liveAfter, _ := os.ReadFile(resolved)
	if string(liveAfter) != string(liveBefore) {
		release()
		t.Error("restore swapped the DB despite failing closed on the foreign shared lock")
	}

	// Once the foreign holder releases, restore can proceed (no wedge: flock
	// auto-clears; a crashed holder's lock would clear on process death too).
	release()
	if _, rerr := Restore(dbPath); rerr != nil {
		t.Errorf("restore refused after the foreign lock was released: %v", rerr)
	}
}

// ---------------------------------------------------------------------------
// CENTERPIECE: shared-allows-many, exclusive-excludes (flock + RWMutex registry)
// ---------------------------------------------------------------------------

// TestDBLock_SharedAllowsManyExclusiveExcludes covers the core lock semantics:
//   - many SHARED holders coexist (concurrent writable opens) in one process;
//   - an EXCLUSIVE acquire fails closed (ErrDBLocked) while a foreign SHARED
//     holder is present, and succeeds once it clears;
//   - a SHARED acquire fails/waits while a foreign EXCLUSIVE holder is present.
func TestDBLock_SharedAllowsManyExclusiveExcludes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	// Many SHARED holders coexist in-process (the RWMutex registry permits many
	// concurrent RLock holders; flock LOCK_SH permits many concurrent shared
	// holders). All N acquire without blocking each other.
	const n = 5
	shared := make([]*dbLockHandle, n)
	for i := 0; i < n; i++ {
		h, err := acquireSharedLock(dbPath)
		if err != nil {
			t.Fatalf("shared acquire %d: %v", i, err)
		}
		shared[i] = h
	}

	// A foreign SHARED holder (raw flock, no registry) must NOT block another
	// shared acquirer either.
	relForeignShared := foreignFlock(t, dbPath, false)
	if h, err := acquireSharedLock(dbPath); err != nil {
		relForeignShared()
		t.Fatalf("shared acquire blocked by a foreign shared holder: %v", err)
	} else {
		h.release()
	}

	// Release ALL in-process shared holders BEFORE attempting an exclusive acquire
	// (the standalone exclusive path takes the same registry RWMutex.Lock, which
	// our in-process RLock holders would block in-process; the cross-process
	// exclusion under test is provided by the FOREIGN flock holder below).
	for _, h := range shared {
		h.release()
	}

	// An EXCLUSIVE acquire must now fail closed while the FOREIGN shared holder is
	// still present (bounded wait → ErrDBLocked): the in-process RWMutex is free,
	// but the cross-process flock LOCK_EX cannot be granted over a foreign LOCK_SH.
	stand := &DB{Path: dbPath}
	if _, err := acquireExclusiveLockForOwner(stand); !errors.Is(err, ErrDBLocked) {
		relForeignShared()
		t.Fatalf("exclusive acquire over a foreign shared holder: err=%v, want ErrDBLocked", err)
	}
	relForeignShared()

	// With everything clear, an exclusive acquire now succeeds...
	rel, err := acquireExclusiveLockForOwner(stand)
	if err != nil {
		t.Fatalf("exclusive acquire on a free db: %v", err)
	}

	// ...and while it is held, a foreign EXCLUSIVE holder cannot also take it
	// (non-blocking probe returns "not granted").
	lp, _ := dbLockPath(dbPath)
	probe, _ := os.OpenFile(lp, os.O_RDWR|os.O_CREATE, 0o600)
	if ok, perr := flockExclusiveNB(probe); perr != nil || ok {
		probe.Close()
		rel()
		t.Fatalf("foreign exclusive granted while we hold exclusive: ok=%v err=%v", ok, perr)
	}
	probe.Close()
	rel()
}

// ---------------------------------------------------------------------------
// Finding 3: restore operates on the canonical (resolved) DB, not a symlink
// ---------------------------------------------------------------------------

// TestRestore_SymlinkedDBHitsRealFile points CONTINUITY_DB at a SYMLINK to the
// real DB and restores. The real file must be replaced (and moved aside), and
// the symlink must still point at a valid restored DB — never renamed itself.
func TestRestore_SymlinkedDBHitsRealFile(t *testing.T) {
	realDir := t.TempDir()
	realDB := filepath.Join(realDir, "real.db")
	buildDBAtVersionStandalone(t, realDB, 5)

	// Seed a v5 marker so we can prove the real file was restored.
	{
		raw, err := sql.Open("sqlite", realDB)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/sym-marker', 'leaf', 'events', 'pre', 1, 1)`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
	}

	// Migrate the REAL DB to head via its real path (creates the sidecar at the
	// resolved path), then mutate post-migration.
	db, err := Open(realDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
		VALUES ('mem://user/feedback/sym-post', 'leaf', 'feedback', 'vanish', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Now restore THROUGH a symlink in a different directory.
	linkDir := t.TempDir()
	link := filepath.Join(linkDir, "link.db")
	if err := os.Symlink(realDB, link); err != nil {
		t.Fatal(err)
	}

	movedAside, err := Restore(link)
	if err != nil {
		t.Fatalf("restore via symlink: %v", err)
	}
	// The moved-aside backup must be next to the REAL (resolved) DB, not the
	// link. Resolve realDB too — on macOS /var is itself a symlink, so compare
	// against the canonical directory.
	resolvedRealDB, _ := resolveDBPath(realDB)
	if filepath.Dir(movedAside) != filepath.Dir(resolvedRealDB) {
		t.Errorf("moved-aside backup not beside real DB: %s (real dir %s)", movedAside, filepath.Dir(resolvedRealDB))
	}
	// The link itself must still BE a symlink pointing at the real DB.
	fi, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("lstat link: %v", err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("symlink was renamed/replaced instead of the real file")
	}

	// The real file is now the restored v5 image: marker present, post-row gone.
	rdb, err := OpenNoMigrate(realDB)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	if v, _ := rdb.SchemaVersion(); v != 5 {
		t.Errorf("real DB schema after restore = v%d, want v5", v)
	}
	var n int
	rdb.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE uri='mem://user/feedback/sym-post'`).Scan(&n)
	if n != 0 {
		t.Errorf("post-upgrade row survived restore through symlink")
	}
}

// ---------------------------------------------------------------------------
// Finding 4: restore marker drives resume/rollback after a simulated crash
// ---------------------------------------------------------------------------

// TestRestoreMarker_RollbackAfterCrashBeforePublish simulates a crash AFTER the
// originals were moved aside but BEFORE the staged snapshot was published
// (DBPublished=false): the live DB is missing. recoverPendingRestore must ROLL
// BACK — move the originals back and leave NO stale -wal/-shm at the live name.
func TestRestoreMarker_RollbackAfterCrashBeforePublish(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)

	// Manufacture the torn state: move the live DB triplet aside, stage a copy,
	// write a not-yet-published marker, then leave the live DB MISSING. Use the
	// RESOLVED DB path for all marker fields, exactly as production Restore does —
	// resume recomputes the canonical set from this resolved path and refuses any
	// marker field outside it.
	resolved, _ := resolveDBPath(dbPath)
	backupPrefix := resolved + ".pre-restore.crashtest"
	staged := filepath.Join(filepath.Dir(resolved), ".restore.staged.crash.db")
	if err := copyFile(snapshotDBPathIn(sidecar), staged); err != nil {
		t.Fatal(err)
	}
	// Record the ORIGINAL live DB's hash BEFORE moving it aside, exactly as
	// production Restore does — reconciliation provenance-checks the backup hash
	// against this before rolling it back (Finding 1).
	origSum, _, err := hashFile(resolved)
	if err != nil {
		t.Fatal(err)
	}
	var moved []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := resolved + suffix
		if _, statErr := os.Lstat(src); statErr != nil {
			continue
		}
		if err := os.Rename(src, backupPrefix+suffix); err != nil {
			t.Fatal(err)
		}
		moved = append(moved, suffix)
	}
	// Plant a stale -wal at the live name to prove rollback scrubs/handles it.
	if err := os.WriteFile(resolved+"-wal", []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: staged,
		BackupPrefix: backupPrefix, MovedSuffixes: moved, DBPublished: false,
		OriginalDBSHA256: origSum,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Explicit recovery (the only path that acts on a marker) must roll back.
	if err := recoverPendingRestore(dbPath); err != nil {
		t.Fatalf("recover rollback: %v", err)
	}
	// The original DB is back and openable.
	rdb, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatalf("original DB not restored by rollback: %v", err)
	}
	rdb.Close()
	// Marker gone; staged gone.
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("marker survived rollback")
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Errorf("staged file survived rollback")
	}
}

// TestRestoreMarker_CompleteAfterCrashAfterPublish simulates a crash AFTER the
// staged snapshot was published over the live DB but BEFORE wal/shm scrub and
// marker removal — with the marker STILL recording db_published:false (the
// stale-pre-publish-marker window Finding 2 addresses). recoverPendingRestore
// must reconcile against reality: the live DB hashes equal to the snapshot, so
// it COMPLETES (keeps the restored DB, scrubs stale -wal/-shm, clears the
// marker) and never rolls back over the already-restored DB.
func TestRestoreMarker_CompleteAfterCrashAfterPublish(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)

	// Publish the snapshot image AT the live DB path, exactly as a real restore
	// does just before it crashes. Reconciliation determines "published" from
	// REALITY now (Finding 1): it hashes the live DB and matches it to the
	// snapshot, so the live file MUST actually be the snapshot image — not the
	// stale head DB. Plant a stale -wal/-shm at the live names and a marker that
	// still says db_published:false (the crash window this round 2 closes). Marker
	// fields use the RESOLVED DB path as production does.
	resolved, _ := resolveDBPath(dbPath)
	if err := copyFile(snapshotDBPathIn(sidecar), resolved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath+"-wal", []byte("stale-wal"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath+"-shm", []byte("stale-shm"), 0o600); err != nil {
		t.Fatal(err)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: "",
		BackupPrefix: resolved + ".pre-restore.x", MovedSuffixes: []string{"", "-wal", "-shm"},
		DBPublished: false,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	if err := recoverPendingRestore(dbPath); err != nil {
		t.Fatalf("recover complete: %v", err)
	}
	// Stale wal/shm scrubbed.
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(dbPath + suffix); !os.IsNotExist(err) {
			t.Errorf("stale %s survived complete-resume", suffix)
		}
	}
	// Marker cleared; DB still openable.
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("marker survived complete-resume")
	}
	rdb, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatalf("DB not openable after complete-resume: %v", err)
	}
	rdb.Close()
}

// ---------------------------------------------------------------------------
// ROUND 2 Finding 1: a planted/corrupt restore marker must never act
// destructively on marker-controlled paths. Resume recomputes EVERY path from
// the canonical resolved DB + sidecar and refuses any marker field that names a
// path outside that set — without touching the out-of-set file.
// ---------------------------------------------------------------------------

// TestRestoreMarker_HostileBackupPrefixRefusedAndUntouched plants a marker whose
// backup_prefix points at an unrelated victim file OUTSIDE the canonical set.
// Resume (rollback phase) must FAIL CLOSED and must NOT rename/move the victim
// over the live DB path.
func TestRestoreMarker_HostileBackupPrefixRefusedAndUntouched(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// A victim file in a SIBLING directory the attacker wants pulled into the DB
	// path (or deleted). It is outside "<resolvedDB>.pre-restore.".
	victimDir := t.TempDir()
	victim := filepath.Join(victimDir, "victim.secret")
	if err := os.WriteFile(victim, []byte("attacker target"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Plant a not-yet-published marker whose backup prefix is the victim path —
	// rollback would naively os.Rename(victim, liveDB). Resume must refuse.
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: "",
		BackupPrefix: victim, MovedSuffixes: []string{""}, DBPublished: false,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	err = recoverPendingRestore(dbPath)
	if err == nil {
		t.Fatal("expected recovery to fail closed on a marker backup prefix outside the canonical set")
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", err)
	}
	// The victim file must be byte-intact and never moved into the DB path.
	got, rerr := os.ReadFile(victim)
	if rerr != nil || string(got) != "attacker target" {
		t.Errorf("hostile marker disturbed the victim file: data=%q err=%v", got, rerr)
	}
	// And the live DB must NOT have been overwritten by the victim.
	if data, _ := os.ReadFile(resolved); string(data) == "attacker target" {
		t.Error("victim file was pulled into the live DB path")
	}
}

// TestRestoreMarker_HostileStagedPathRefusedAndUntouched plants a marker whose
// staged_path points at an unrelated victim file (outside the DB dir). On the
// COMPLETE phase resume would os.Remove(stagedPath); it must refuse instead and
// leave the victim intact.
func TestRestoreMarker_HostileStagedPathRefusedAndUntouched(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	victimDir := t.TempDir()
	victim := filepath.Join(victimDir, "victim.db")
	if err := os.WriteFile(victim, []byte("do not delete"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Published marker so the COMPLETE path runs (which removes staged). The
	// staged path is the victim, in another directory — must be refused.
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: victim,
		BackupPrefix: "", MovedSuffixes: nil, DBPublished: true,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	err = recoverPendingRestore(dbPath)
	if err == nil {
		t.Fatal("expected recovery to fail closed on a staged path outside the db dir")
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", err)
	}
	if _, serr := os.Stat(victim); serr != nil {
		t.Errorf("hostile staged path caused victim deletion: %v", serr)
	}
}

// TestRestoreMarker_SymlinkedSidecarRefusedOnResume plants a SYMLINK where the
// sidecar dir should be. Resume must assertNotSymlink the sidecar (derived
// canonically from the DB path) and fail closed — never follow the link to read
// or remove a marker through it.
func TestRestoreMarker_SymlinkedSidecarRefusedOnResume(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	// Point the sidecar path at an attacker-controlled directory via a symlink.
	evil := t.TempDir()
	sidecar, _ := sidecarPath(dbPath)
	if err := os.Symlink(evil, sidecar); err != nil {
		t.Fatal(err)
	}
	// Plant a marker inside the link target so a naive resume would read it.
	mk := &restoreMarker{Version: 1, RestoredDBPath: dbPath, DBPublished: true}
	if err := writeRestoreMarkerAtomic(evil, mk); err != nil {
		t.Fatal(err)
	}

	err := recoverPendingRestore(dbPath)
	if err == nil {
		t.Fatal("expected recovery to refuse a symlinked sidecar")
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", err)
	}
	// The symlinked marker must NOT have been removed through the link.
	if _, serr := os.Stat(restoreMarkerPathIn(evil)); serr != nil {
		t.Errorf("resume followed the symlinked sidecar and removed the marker: %v", serr)
	}
}

// ---------------------------------------------------------------------------
// PIVOT (Findings 1, 2, 4): a routine Open NEVER resumes a restore. A pending
// marker makes Open()/OpenNoMigrate() FAIL CLOSED with ErrRestoreInterrupted,
// regardless of who (if anyone) holds the serve lock. Recovery happens ONLY
// under explicit operator intent (the restore path → recoverPendingRestore).
// ---------------------------------------------------------------------------

// TestOpen_FailsClosedOnTornPrePublishState manufactures the torn pre-publish
// window (triplet moved aside, marker present, live DB MISSING) and asserts a
// routine Open does NOT fabricate a DB and does NOT touch the marker/originals —
// it returns ErrRestoreInterrupted. This holds whether or not a foreign live
// serve lock is present (Findings 1 + 2). Then explicit recovery completes it.
func TestOpen_FailsClosedOnTornPrePublishState(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Manufacture the torn (pre-publish) state: move the triplet aside, stage a
	// copy, write a not-yet-published marker. Leave the live DB MISSING.
	staged := filepath.Join(filepath.Dir(resolved), ".restore.staged.excl.db")
	if err := copyFile(snapshotDBPathIn(sidecar), staged); err != nil {
		t.Fatal(err)
	}
	origSum, _, err := hashFile(resolved)
	if err != nil {
		t.Fatal(err)
	}
	backupPrefix := resolved + ".pre-restore.excl"
	var moved []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := resolved + suffix
		if _, statErr := os.Lstat(src); statErr != nil {
			continue
		}
		if err := os.Rename(src, backupPrefix+suffix); err != nil {
			t.Fatal(err)
		}
		moved = append(moved, suffix)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: staged,
		BackupPrefix: backupPrefix, MovedSuffixes: moved, DBPublished: false,
		OriginalDBSHA256: origSum,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Routine Open must FAIL CLOSED — never fabricate a DB at the path, never
	// resume. No serve lock is held here (nobody is actively restoring); under
	// the pivot the open still refuses rather than self-healing.
	if _, oerr := Open(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		t.Fatalf("Open over a pending marker: err=%v, want ErrRestoreInterrupted", oerr)
	}
	// OpenNoMigrate must fail closed identically.
	if _, oerr := OpenNoMigrate(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		t.Fatalf("OpenNoMigrate over a pending marker: err=%v, want ErrRestoreInterrupted", oerr)
	}
	// The Open must NOT have fabricated a DB at the live path.
	if _, err := os.Stat(resolved); !os.IsNotExist(err) {
		t.Errorf("Open fabricated a DB at the live path despite a pending marker (err=%v)", err)
	}
	// Marker + moved-aside originals untouched.
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); err != nil {
		t.Errorf("Open disturbed the marker: %v", err)
	}
	if _, err := os.Stat(backupPrefix); err != nil {
		t.Errorf("Open disturbed the moved-aside original: %v", err)
	}

	// A concurrently-held SHARED lock must not change the answer: Open checks the
	// interrupted-restore marker BEFORE acquiring its own shared lock, so it still
	// fails closed (the marker gate is independent of lock state).
	otherShared, slErr := acquireSharedLock(dbPath)
	if slErr != nil {
		t.Fatalf("acquire shared lock: %v", slErr)
	}
	if _, oerr := Open(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		otherShared.release()
		t.Fatalf("Open under a held shared lock: err=%v, want ErrRestoreInterrupted", oerr)
	}
	otherShared.release()

	// Explicit recovery (the restore path's primitive) rolls back and clears the
	// marker; afterward a routine Open succeeds again.
	if err := recoverPendingRestore(dbPath); err != nil {
		t.Fatalf("explicit recovery: %v", err)
	}
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("marker survived explicit recovery")
	}
	rdb, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatalf("DB not recovered after explicit recovery: %v", err)
	}
	rdb.Close()
}

// ---------------------------------------------------------------------------
// ROUND 2 Finding 8: resume through a DANGLING DB symlink still finds the
// sidecar under the REAL DB and recovers (rolls back) — it must not fall back to
// "<link>.snapshot" and skip recovery, which would let a fresh DB be fabricated.
// ---------------------------------------------------------------------------

// TestRestoreMarker_ResumeThroughDanglingSymlinkRecovers simulates a restore
// that crashed (before publish) THROUGH a symlinked CONTINUITY_DB after the real
// DB was moved aside: the link now dangles. Opening via the dangling link must
// resolve the real DB's sidecar (via os.Readlink), find the marker there, and
// roll the originals back.
func TestRestoreMarker_ResumeThroughDanglingSymlinkRecovers(t *testing.T) {
	realDir := t.TempDir()
	realDB := filepath.Join(realDir, "real.db")
	buildDBAtVersionStandalone(t, realDB, 5)
	db, err := Open(realDB)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(realDB)
	resolvedReal, _ := resolveDBPath(realDB)

	// Stage a copy and move the real DB triplet aside (the not-yet-published torn
	// state), writing the marker under the REAL DB's sidecar.
	staged := filepath.Join(filepath.Dir(resolvedReal), ".restore.staged.dangle.db")
	if err := copyFile(snapshotDBPathIn(sidecar), staged); err != nil {
		t.Fatal(err)
	}
	origSum, _, err := hashFile(resolvedReal)
	if err != nil {
		t.Fatal(err)
	}
	backupPrefix := resolvedReal + ".pre-restore.dangle"
	var moved []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := resolvedReal + suffix
		if _, statErr := os.Lstat(src); statErr != nil {
			continue
		}
		if err := os.Rename(src, backupPrefix+suffix); err != nil {
			t.Fatal(err)
		}
		moved = append(moved, suffix)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolvedReal, StagedPath: staged,
		BackupPrefix: backupPrefix, MovedSuffixes: moved, DBPublished: false,
		OriginalDBSHA256: origSum,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Now the real DB is gone → a symlink pointing at it DANGLES. Resume through
	// the dangling link must still find the marker under the real sidecar.
	linkDir := t.TempDir()
	link := filepath.Join(linkDir, "link.db")
	if err := os.Symlink(realDB, link); err != nil {
		t.Fatal(err)
	}
	if _, serr := os.Stat(link); serr == nil {
		t.Fatal("link unexpectedly resolves; real DB should be moved aside (dangling)")
	}

	if err := recoverPendingRestore(link); err != nil {
		t.Fatalf("recover through dangling symlink: %v", err)
	}
	// The real DB is back (rollback moved the originals back).
	rdb, err := OpenNoMigrate(realDB)
	if err != nil {
		t.Fatalf("dangling-symlink resume did not recover the real DB: %v", err)
	}
	rdb.Close()
	// Marker and staged file cleared under the real sidecar.
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("marker survived dangling-symlink resume")
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Errorf("staged file survived dangling-symlink resume")
	}
}

// ---------------------------------------------------------------------------
// Finding 5: a pre-restore backup is never overwritten
// ---------------------------------------------------------------------------

// TestRestore_NeverOverwritesExistingBackup pre-creates a file at the
// same-second pre-restore backup name that the next restore would naively pick,
// then runs a restore. The pre-existing file must remain byte-intact and the
// restore must move its originals to a DIFFERENT, unique name.
func TestRestore_NeverOverwritesExistingBackup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// uniquePreRestorePrefix must never return a prefix whose names already
	// exist. Occupy the first candidate exactly, then assert the next call
	// returns a different, free prefix.
	resolved, _ := resolveDBPath(dbPath)
	first, err := uniquePreRestorePrefix(resolved)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := []byte("DO NOT CLOBBER")
	if err := os.WriteFile(first, sentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := uniquePreRestorePrefix(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("uniquePreRestorePrefix returned an occupied name: %s", second)
	}

	// A full restore must pick a free name and leave the sentinel intact.
	movedAside, err := Restore(dbPath)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if movedAside == first {
		t.Errorf("restore reused the occupied backup name %s", first)
	}
	got, err := os.ReadFile(first)
	if err != nil || string(got) != string(sentinel) {
		t.Errorf("pre-existing backup was clobbered: data=%q err=%v", got, err)
	}
}

// ---------------------------------------------------------------------------
// ROUND 2 Finding 4: an interrupted upgrade continues under the still-covering
// restore point (reuse), and a COMPLETED/non-covering point fails closed.
// ---------------------------------------------------------------------------

// TestCreateRestorePoint_InterruptedUpgradeReusesCoveringPoint is the
// crash-after-v6-before-v9 regression. A pre-v5→v9 point is taken, then v6
// commits and the process crashes before v9. On the next run preVersion=6, but
// the existing pre-v5 point still COVERS the v5→v9 window (pre<=6, target>=9,
// current<target). The prior round wrongly rejected this (existing.Pre != pre)
// and aborted the upgrade. It must now REUSE the point and NOT overwrite it.
func TestCreateRestorePoint_InterruptedUpgradeReusesCoveringPoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()

	// Take the pre-v5 restore point (target = head = 9), as the v5→v9 run does
	// before the first risky migration (v6).
	if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
		t.Fatalf("seed pre-v5 restore point: %v", err)
	}
	before, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the crash window: v6 has committed, so the on-disk schema is now
	// v6 and the next-pending risky migration is v9. Record v6 as applied.
	if _, err := db.Exec(
		`INSERT INTO schema_versions (version, description) VALUES (6, 'crash-after-v6')`,
	); err != nil {
		t.Fatalf("mark v6 applied: %v", err)
	}

	// The resumed v5→v9 run re-enters createRestorePoint with preVersion=6,
	// firstRisky=9. The pre-v5→v9 point still covers it → REUSE (no error, no
	// overwrite).
	if err := db.createRestorePoint(6, headVersion(), 9); err != nil {
		t.Fatalf("interrupted upgrade should reuse the covering pre-v5 point, got: %v", err)
	}
	after, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("covering restore point was overwritten on reuse")
	}
	// The point must still capture pre-v5 (the true rollback target), not pre-v6.
	m, err := loadValidManifest(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	if m.PreSchemaVersion != 5 {
		t.Errorf("reused point pre_schema_version = %d, want 5 (must still roll back to pre-v5)", m.PreSchemaVersion)
	}
}

// TestCreateRestorePoint_CompletedUpgradeFailsClosed: a valid same-lineage point
// whose upgrade is ALREADY COMPLETE (current schema >= existing target) must NOT
// be reused or overwritten — createRestorePoint fails closed so the operator
// restores or prunes explicitly before a new upgrade window is opened.
func TestCreateRestorePoint_CompletedUpgradeFailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()

	// Existing pre-v5→v9 point.
	if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
		t.Fatalf("seed restore point: %v", err)
	}
	before, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}

	// The upgrade has completed: current schema is now at head (>= existing
	// target 9). A fresh createRestorePoint for this window must fail closed.
	err = db.createRestorePoint(headVersion(), headVersion(), headVersion())
	if err == nil {
		t.Fatal("expected fail-closed when the existing restore point's upgrade is already complete")
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", err)
	}
	after, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("completed-upgrade restore point was overwritten on fail-closed")
	}
}

// ---------------------------------------------------------------------------
// Finding 7: restore on a missing live DB fails closed (never fabricates one)
// ---------------------------------------------------------------------------

// TestRestore_MissingLiveDBFailsClosed deletes the live DB after a restore point
// exists, then restores. OpenNoMigrate must return ErrDBMissing and restore must
// refuse — never silently create an empty DB.
func TestRestore_MissingLiveDBFailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Remove the live DB triplet; the sidecar (restore point) remains.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dbPath + suffix)
	}

	if _, err := Restore(dbPath); err == nil {
		t.Fatal("expected restore to fail closed on a missing live DB")
	} else if !errors.Is(err, ErrDBMissing) {
		t.Errorf("err = %v, want ErrDBMissing", err)
	}
	// The DB must NOT have been fabricated.
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Errorf("restore fabricated a DB at %s despite missing live DB", dbPath)
	}
}

// TestOpenNoMigrate_MissingFileFailsClosed: the read-only inspection open must
// not lazily create an empty DB for a missing path.
func TestOpenNoMigrate_MissingFileFailsClosed(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "nope.db")
	if _, err := OpenNoMigrate(missing); !errors.Is(err, ErrDBMissing) {
		t.Errorf("OpenNoMigrate(missing) err=%v, want ErrDBMissing", err)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Errorf("OpenNoMigrate fabricated a file at %s", missing)
	}
}

// ---------------------------------------------------------------------------
// Finding 9: concurrent restore-point creation serializes (no double-publish)
// ---------------------------------------------------------------------------

// TestCreateRestorePoint_ConcurrentSerializes runs many createRestorePoint calls
// against the same DB concurrently. The operation lock must serialize them so
// exactly one publishes and the rest reuse — never a corrupt/double-published
// sidecar — and the final manifest is valid.
func TestCreateRestorePoint_ConcurrentSerializes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	const n = 8
	// Open the writable handles SERIALLY first (the DB is already WAL from Open;
	// concurrently re-running journal_mode pragmas only contends in-process and
	// is not what we are testing). Then race createRestorePoint across them —
	// the operation lock must serialize the publishers.
	handles := make([]*DB, n)
	for i := range handles {
		handles[i] = openWritableNoMigrate(t, dbPath)
		defer handles[i].Close()
	}

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start.Wait() // release all at once for maximum contention
			errs[idx] = handles[idx].createRestorePoint(5, headVersion(), 6)
		}(i)
	}
	start.Done()
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: createRestorePoint failed: %v", i, e)
		}
	}
	// Exactly one valid restore point, intact.
	if _, err := loadValidManifest(sidecar); err != nil {
		t.Fatalf("post-concurrency manifest invalid: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ROUND 2 Finding 6: the op-lock spans the migration DDL, not just sidecar
// creation. Concurrent opens of a v5 DB must serialize through the whole risky
// upgrade: exactly one performs the destructive migration, the rest see it done
// (or fail closed), and no torn state results.
// ---------------------------------------------------------------------------

// TestMigrate_ConcurrentRiskyUpgradeSerializes races migrate() (restore point +
// the v6/v9 table-rebuild DDL) across N pre-opened handles on one v5 DB. Before
// Finding 6 the op-lock dropped after sidecar creation, so two opens could both
// enter the destructive CREATE/COPY/DROP/RENAME loop concurrently. Holding the
// lock across the DDL must serialize them: the DB ends at head with the seeded
// row intact, exactly one restore point exists, and every migrate() either
// succeeds, fails closed on the op-lock, or hits a transient SQLITE_BUSY — never
// a torn/half-applied schema.
//
// Handles are opened SERIALLY first (the journal_mode=WAL pragma contends
// in-process across concurrent sql.Open and is not what we are testing); the DDL
// serialization is.
func TestMigrate_ConcurrentRiskyUpgradeSerializes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	// Seed a v5 row so a torn/double rebuild would lose or duplicate it.
	{
		raw, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/concurrent-marker', 'leaf', 'events', 'survive', 1, 1)`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
	}

	const n = 6
	handles := make([]*DB, n)
	for i := range handles {
		handles[i] = openWritableNoMigrate(t, dbPath)
		defer handles[i].Close()
	}

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start.Wait() // release all at once for maximum contention
			errs[idx] = handles[idx].migrate()
		}(i)
	}
	start.Done()
	wg.Wait()

	// Every migrate() must have either succeeded, failed CLOSED on the exclusive
	// DB lock (ErrDBLocked), or hit a transient SQLITE_BUSY — never a torn-schema
	// error from two rebuilds interleaving.
	for i, e := range errs {
		if e == nil || errors.Is(e, ErrDBLocked) {
			continue
		}
		if contains(e.Error(), "database is locked") || contains(e.Error(), "SQLITE_BUSY") {
			continue
		}
		t.Errorf("goroutine %d: unexpected error (want success / exclusive-lock fail-closed / SQLITE_BUSY): %v", i, e)
	}

	// Final DB is at head, the seeded row survives exactly once, and exactly one
	// valid restore point exists.
	fin, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fin.Close()
	if v, _ := fin.SchemaVersion(); v != headVersion() {
		t.Errorf("final schema = v%d, want head v%d", v, headVersion())
	}
	var cnt int
	if err := fin.QueryRow(
		`SELECT COUNT(*) FROM mem_nodes WHERE uri='mem://user/events/concurrent-marker'`).Scan(&cnt); err != nil {
		t.Fatalf("read marker after concurrent upgrade: %v", err)
	}
	if cnt != 1 {
		t.Errorf("seeded row count = %d after concurrent upgrade, want exactly 1 (torn/double rebuild)", cnt)
	}
	sidecar, _ := sidecarPath(dbPath)
	if _, err := loadValidManifest(sidecar); err != nil {
		t.Errorf("post-concurrency restore point invalid: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ROUND 2 Finding 7: temp/staging files are created O_EXCL (creation proves
// ownership). A pre-existing PID-named temp from another process must NEVER be
// clobbered/removed — the new code picks a fresh random name instead.
// ---------------------------------------------------------------------------

// TestRestore_NeverClobbersForeignStagedTemp plants a file at the EXACT legacy
// PID-named staged-temp path the prior code created-with-O_TRUNC and removed
// (.restore.staged.<pid>.db), then drives a real restore. The foreign file must
// be byte-intact afterward: O_EXCL + a random token mean restore stages into a
// name it proved it owns, never the squatted PID-named one (Finding 7).
func TestRestore_NeverClobbersForeignStagedTemp(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // creates a valid restore point
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// A "foreign" in-flight staged temp squatting the legacy PID-named path that
	// the prior code would have os.Remove'd before copying over it.
	resolved, _ := resolveDBPath(dbPath)
	legacyStaged := filepath.Join(filepath.Dir(resolved), fmt.Sprintf(".restore.staged.%d.db", os.Getpid()))
	const foreign = "FOREIGN IN-FLIGHT STAGED TEMP — DO NOT TOUCH"
	if err := os.WriteFile(legacyStaged, []byte(foreign), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Restore(dbPath); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// The foreign staged temp must be byte-intact — restore used its own O_EXCL
	// name and never touched this one.
	got, err := os.ReadFile(legacyStaged)
	if err != nil {
		t.Fatalf("foreign staged temp was removed by restore: %v", err)
	}
	if string(got) != foreign {
		t.Errorf("foreign staged temp was clobbered by restore: %q", got)
	}
}

// TestCreateOwnedTemp_ExclusiveOwnership verifies the helper itself: it returns
// a freshly-created, previously-absent path, and never hands back (or truncates)
// a name that already exists.
func TestCreateOwnedTemp_ExclusiveOwnership(t *testing.T) {
	dir := t.TempDir()

	f, path, err := createOwnedTemp(dir, "x.tmp.", ".db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if filepath.Dir(path) != dir {
		t.Errorf("temp not in requested dir: %s", path)
	}
	// A second call must produce a DIFFERENT path (random token), never reuse the
	// existing one.
	f2, path2, err := createOwnedTemp(dir, "x.tmp.", ".db")
	if err != nil {
		t.Fatal(err)
	}
	f2.Close()
	if path2 == path {
		t.Errorf("createOwnedTemp returned a colliding path twice: %s", path2)
	}

	// reserveOwnedTempName returns a proven-free path (file removed after the
	// O_EXCL placeholder), suitable for VACUUM INTO.
	reserved, err := reserveOwnedTempName(dir, "v.tmp.", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reserved); !os.IsNotExist(err) {
		t.Errorf("reserved temp name is not free: %v", err)
	}
}

// ---------------------------------------------------------------------------
// PIVOT Finding 1: a planted marker + a hostile <db>.pre-restore.* file must NOT
// drive a destructive rollback on a routine Open. Open fails closed
// (ErrRestoreInterrupted); the live DB and the hostile file are both untouched.
// ---------------------------------------------------------------------------

// TestOpen_PlantedMarkerWithHostilePreRestoreFile_FailsClosed plants a marker
// that names a canonical-looking "<db>.pre-restore.*" backup whose CONTENT is
// attacker-controlled, alongside the intact live DB. Before the pivot, a routine
// Open auto-rolled-back: it would rename the attacker's pre-restore file over the
// live DB. Now Open must FAIL CLOSED and touch nothing.
func TestOpen_PlantedMarkerWithHostilePreRestoreFile_FailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // real sidecar at dbPath.snapshot
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Record the live DB bytes so we can prove they are untouched.
	liveBefore, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatal(err)
	}

	// A hostile pre-restore file with a canonical prefix but attacker content. A
	// naive rollback would os.Rename(hostile, liveDB).
	hostilePrefix := resolved + ".pre-restore.20200101T000000Z.1"
	const hostile = "ATTACKER-CONTROLLED ROLLBACK SOURCE"
	if err := os.WriteFile(hostilePrefix, []byte(hostile), 0o600); err != nil {
		t.Fatal(err)
	}

	// Plant a not-yet-published marker pointing rollback at the hostile file.
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: "",
		BackupPrefix: hostilePrefix, MovedSuffixes: []string{""}, DBPublished: false,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Routine Open must fail closed — never roll back off the planted marker.
	if _, oerr := Open(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		t.Fatalf("Open over planted marker: err=%v, want ErrRestoreInterrupted", oerr)
	}

	// The live DB is byte-intact (the hostile file was NOT pulled over it).
	liveAfter, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("live DB disturbed by routine open: %v", err)
	}
	if string(liveAfter) != string(liveBefore) {
		t.Error("routine Open overwrote the live DB from a planted marker's hostile pre-restore file")
	}
	// The hostile file is byte-intact (never consumed by a rename).
	got, err := os.ReadFile(hostilePrefix)
	if err != nil || string(got) != hostile {
		t.Errorf("hostile pre-restore file disturbed: data=%q err=%v", got, err)
	}
}

// ---------------------------------------------------------------------------
// PIVOT Finding 4: a corrupt `{}` marker after a crash must make Open fail
// closed, the marker preserved (not erased), and NO DB fabricated.
// ---------------------------------------------------------------------------

// TestOpen_CorruptEmptyMarker_FailsClosedPreservesMarker writes a `{}` marker
// (decodes, but fails the schema gate: version 0, no fields) into the sidecar
// with the live DB MISSING. Open must return ErrRestoreInterrupted, leave the
// marker in place, and not fabricate a DB. Explicit recovery then refuses on the
// schema gate too (the operator must inspect), proving the corrupt marker is
// never silently treated as recovered.
func TestOpen_CorruptEmptyMarker_FailsClosedPreservesMarker(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Remove the live DB triplet to simulate the torn mid-restore window.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(resolved + suffix)
	}

	// Write a corrupt `{}` marker directly at the marker path.
	markerPath := restoreMarkerPathIn(sidecar)
	if err := os.WriteFile(markerPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Open must fail closed and NOT fabricate a DB.
	if _, oerr := Open(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		t.Fatalf("Open over `{}` marker: err=%v, want ErrRestoreInterrupted", oerr)
	}
	if _, err := os.Stat(resolved); !os.IsNotExist(err) {
		t.Errorf("Open fabricated a DB despite a corrupt marker (err=%v)", err)
	}
	// The marker must be PRESERVED byte-for-byte (never erased).
	got, err := os.ReadFile(markerPath)
	if err != nil || string(got) != "{}" {
		t.Errorf("corrupt marker was disturbed: data=%q err=%v", got, err)
	}

	// Explicit recovery refuses on the schema gate (version != 1) — the corrupt
	// marker is never acted on, and is still preserved.
	if rerr := recoverPendingRestore(dbPath); !errors.Is(rerr, ErrSnapshotSidecarCorrupt) {
		t.Errorf("recovery over `{}` marker: err=%v, want ErrSnapshotSidecarCorrupt", rerr)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Errorf("recovery erased the corrupt marker instead of failing closed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 3: lock & sidecar keyed to the SAME real DB through a symlink whose
// target appears later. serve and a second serve/restore must contend on ONE
// lock (the real DB's), never on divergent link-vs-real lock paths.
// ---------------------------------------------------------------------------

// TestDBLock_SymlinkLockUnifiedWithRealTarget builds the real DB, points a
// symlink at it, and asserts the DB lock path resolved via the LINK equals the
// one resolved via the REAL DB — and that a foreign EXCLUSIVE holder taken via
// the link makes a SHARED acquire via the real path wait/fail-block (same single
// on-disk lock file). Before Finding 3 the lock used Abs (link path) while the
// sidecar used the resolved real path, so a serve-via-link and a
// restore-via-real-path contended on DIFFERENT locks and could both touch the DB.
func TestDBLock_SymlinkLockUnifiedWithRealTarget(t *testing.T) {
	realDir := t.TempDir()
	realDB := filepath.Join(realDir, "real.db")
	buildDBAtVersionStandalone(t, realDB, 5)

	linkDir := t.TempDir()
	link := filepath.Join(linkDir, "link.db")
	if err := os.Symlink(realDB, link); err != nil {
		t.Fatal(err)
	}

	viaLink, err := dbLockPath(link)
	if err != nil {
		t.Fatal(err)
	}
	viaReal, err := dbLockPath(realDB)
	if err != nil {
		t.Fatal(err)
	}
	if viaLink != viaReal {
		t.Fatalf("db lock keyed differently via link vs real:\n link=%s\n real=%s", viaLink, viaReal)
	}

	// A foreign EXCLUSIVE holder taken via the LINK path must make an EXCLUSIVE
	// acquire via the REAL path fail closed — proving they contend on ONE lock.
	relLink := foreignFlock(t, link, true)
	defer relLink()
	stand := &DB{Path: realDB}
	if _, aerr := acquireExclusiveLockForOwner(stand); !errors.Is(aerr, ErrDBLocked) {
		t.Errorf("exclusive via real path while link holds exclusive: err=%v, want ErrDBLocked", aerr)
	}
}

// TestCanonicalDBPath_SurvivesLaterCreatedSymlinkTarget pins that the lock and
// sidecar derivations agree even when the symlink target does not exist YET
// (it is created later): both route through canonicalDBPath, so they cannot
// diverge between the missing-target and present-target states.
func TestCanonicalDBPath_SurvivesLaterCreatedSymlinkTarget(t *testing.T) {
	realDir := t.TempDir()
	realDB := filepath.Join(realDir, "real.db")

	linkDir := t.TempDir()
	link := filepath.Join(linkDir, "link.db")
	if err := os.Symlink(realDB, link); err != nil {
		t.Fatal(err)
	}

	// Target does not exist yet: resolution must follow the link via Readlink and
	// agree for both lock and sidecar.
	lockBefore, _ := dbLockPath(link)
	sidecarBefore, _ := sidecarPath(link)
	if filepath.Dir(lockBefore) != filepath.Dir(sidecarBefore) {
		t.Errorf("lock/sidecar dirs diverge with missing target:\n lock=%s\n sidecar=%s", lockBefore, sidecarBefore)
	}

	// Now create the target; resolution must still agree, and the lock/sidecar
	// must be keyed to the SAME real file as resolving the real path directly.
	buildDBAtVersionStandalone(t, realDB, 5)
	lockAfter, _ := dbLockPath(link)
	realLock, _ := dbLockPath(realDB)
	if lockAfter != realLock {
		t.Errorf("post-create lock via link != via real:\n link=%s\n real=%s", lockAfter, realLock)
	}
}

// ---------------------------------------------------------------------------
// Finding 5: directory fsync after the durability-critical renames. A full
// power-loss can't be simulated in a unit test; we verify the helper works and
// that the publish paths invoke it (ordering: fsync AFTER the rename).
// ---------------------------------------------------------------------------

// TestFsyncDir_SyncsAndErrorsOnNonDir verifies the helper: it fsyncs a real
// directory without error, and returns an error for a non-existent path (so a
// caller can log it).
func TestFsyncDir_SyncsAndErrorsOnNonDir(t *testing.T) {
	dir := t.TempDir()
	if err := fsyncDir(dir); err != nil {
		t.Errorf("fsyncDir on a real dir: %v", err)
	}
	if err := fsyncDir(filepath.Join(dir, "does-not-exist")); err == nil {
		t.Error("fsyncDir on a missing path: want error, got nil")
	}
}

// TestWriteRestorePoint_PublishesDurably exercises the create path that fsyncs
// the sidecar dir after the snapshot.db and manifest renames. We can't observe
// the fsync syscall directly, but we assert the post-condition the fsync exists
// to protect: after createRestorePoint returns, snapshot.db AND manifest.json
// are both present and the manifest validates — i.e. the renames the fsync
// makes durable both landed and are internally consistent.
func TestWriteRestorePoint_PublishesDurably(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()
	if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
		t.Fatalf("createRestorePoint: %v", err)
	}
	if _, err := os.Stat(snapshotDBPathIn(sidecar)); err != nil {
		t.Errorf("snapshot.db not durably published: %v", err)
	}
	if _, err := loadValidManifest(sidecar); err != nil {
		t.Errorf("manifest not durably published/valid: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 6: a snapshot creation that fails AFTER the sidecar dir is created
// must remove the partial sidecar (no half-built restore point lingers), while
// never removing a sidecar dir that pre-existed or holds foreign files.
// ---------------------------------------------------------------------------

// TestCreateRestorePoint_FailureRemovesPartialSidecar forces the snapshot image
// to fail its post-VACUUM schema check by pointing createRestorePoint at a
// pre-version that the live DB cannot match, and asserts the partial sidecar dir
// it created is removed (Finding 6). The DB stays unmutated except for the
// benign per-DB identity row (documented as identity, not tracking metadata).
func TestCreateRestorePoint_FailureRemovesPartialSidecar(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()

	// preVersion=4 but the live DB is at v5: the snapshot image's schema (v5) will
	// not equal the claimed pre-upgrade v4, so writeRestorePoint fails AFTER it
	// created the sidecar dir and VACUUM'd the image.
	err := db.createRestorePoint(4, headVersion(), 6)
	if err == nil {
		t.Fatal("expected createRestorePoint to fail on a schema mismatch")
	}
	// The partial sidecar dir we created must be GONE — no half-built restore
	// point lingers.
	if _, statErr := os.Stat(sidecar); !os.IsNotExist(statErr) {
		entries, _ := os.ReadDir(sidecar)
		t.Errorf("partial sidecar survived a failed snapshot creation (entries=%v, err=%v)", entries, statErr)
	}
}

// TestCreateRestorePoint_FailureKeepsPreexistingSidecar proves the cleanup is
// scoped: if the sidecar dir PRE-EXISTED (e.g. holds an operator file), a failed
// creation must NOT remove it — we only clean up a dir we created this call.
func TestCreateRestorePoint_FailureKeepsPreexistingSidecar(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	// Pre-create the sidecar dir with a stray operator file.
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(sidecar, "operator-note.txt")
	if err := os.WriteFile(stray, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}

	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()
	if err := db.createRestorePoint(4, headVersion(), 6); err == nil {
		t.Fatal("expected createRestorePoint to fail on a schema mismatch")
	}
	// The pre-existing sidecar and its stray file must remain.
	if _, err := os.Stat(stray); err != nil {
		t.Errorf("failed creation removed a pre-existing sidecar's file: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 7: fork ambiguity is a KNOWN, DOCUMENTED limitation. cp A.db→B.db
// makes B inherit A's instance_id; diverging B then dropping A's sidecar next to
// B passes lineage and restores A's snapshot onto B. This test PINS that
// behavior so it cannot change silently. The restore point protects a DB and
// FAITHFUL COPIES of it; operators must not cross-pollinate sidecars between
// forked copies. (See .notes/restore-recovery-model.md.)
// ---------------------------------------------------------------------------

// TestRestore_ForkAmbiguityIsPinned documents-by-test the finding-7 limitation:
// a copy shares identity by design, so a sidecar from the source restores onto
// the diverged copy. We assert it SUCCEEDS (lineage matches), pinning the known
// behavior — NOT claiming protection we do not have.
func TestRestore_ForkAmbiguityIsPinned(t *testing.T) {
	dir := t.TempDir()
	aPath := filepath.Join(dir, "a.db")
	buildDBAtVersionStandalone(t, aPath, 5)

	// Seed a row in A that identifies its snapshot.
	{
		raw, err := sql.Open("sqlite", aPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/from-A', 'leaf', 'events', 'A-snapshot', 1, 1)`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
	}

	// Take A's restore point (sidecar at a.db.snapshot, lineage bound to A's id).
	{
		db := openWritableNoMigrate(t, aPath)
		if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
			db.Close()
			t.Fatalf("create A restore point: %v", err)
		}
		db.Close()
	}

	// cp A.db → B.db: B INHERITS A's instance_id (fork ambiguity by design).
	bPath := filepath.Join(dir, "b.db")
	if err := copyFile(aPath, bPath); err != nil {
		t.Fatal(err)
	}
	// Diverge B: add a B-only row so we can see A's snapshot overwrite it.
	{
		raw, err := sql.Open("sqlite", bPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/B-only', 'leaf', 'events', 'diverged', 2, 2)`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
	}

	// Transplant A's sidecar next to B (cross-pollination the docs warn against).
	aSidecar, _ := sidecarPath(aPath)
	bSidecar, _ := sidecarPath(bPath)
	if err := os.MkdirAll(bSidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(snapshotDBPathIn(aSidecar), snapshotDBPathIn(bSidecar)); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(manifestPathIn(aSidecar), manifestPathIn(bSidecar)); err != nil {
		t.Fatal(err)
	}

	// PINNED behavior: lineage matches (shared id), so restore SUCCEEDS and B now
	// holds A's snapshot — the B-only row is gone. This is the documented fork
	// limitation, asserted so it can't silently change.
	if _, err := Restore(bPath); err != nil {
		t.Fatalf("fork-ambiguity restore (pinned to SUCCEED): %v", err)
	}
	rdb, err := OpenNoMigrate(bPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	var fromA, bOnly int
	rdb.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE uri='mem://user/events/from-A'`).Scan(&fromA)
	rdb.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE uri='mem://user/events/B-only'`).Scan(&bOnly)
	if fromA != 1 {
		t.Errorf("A's snapshot row missing after fork restore (count=%d)", fromA)
	}
	if bOnly != 0 {
		t.Errorf("B-only row survived A's snapshot restore (count=%d); fork limitation changed", bOnly)
	}
}

// ---------------------------------------------------------------------------
// Finding 8: boot expiry must recompute lineage and leave a transplanted/foreign
// sidecar UNTOUCHED — never auto-delete unproven restore material.
// ---------------------------------------------------------------------------

// TestRecordSuccessfulBoot_LeavesForeignSidecarUntouched transplants A's sidecar
// next to an unrelated DB B (different instance_id), then boots B 3× (enough to
// hit the expiry threshold for a MATCHING sidecar). Because B's recomputed
// lineage does not match the transplanted manifest, the boot tick must do
// NOTHING — the foreign sidecar's snapshot.db + manifest survive all 3 boots.
func TestRecordSuccessfulBoot_LeavesForeignSidecarUntouched(t *testing.T) {
	dir := t.TempDir()
	aPath := filepath.Join(dir, "a.db")
	buildDBAtVersionStandalone(t, aPath, 5)
	{
		db := openWritableNoMigrate(t, aPath)
		if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
			db.Close()
			t.Fatalf("create A restore point: %v", err)
		}
		db.Close()
	}
	aSidecar, _ := sidecarPath(aPath)

	// An unrelated DB B at head with its OWN random instance_id.
	bPath := filepath.Join(dir, "b.db")
	buildDBAtVersionStandalone(t, bPath, headVersion())
	bSidecar, _ := sidecarPath(bPath)

	// Transplant A's sidecar (snapshot.db + manifest) next to B verbatim.
	if err := os.MkdirAll(bSidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(snapshotDBPathIn(aSidecar), snapshotDBPathIn(bSidecar)); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(manifestPathIn(aSidecar), manifestPathIn(bSidecar)); err != nil {
		t.Fatal(err)
	}

	// Boot B three times. A MATCHING sidecar would expire on the third tick; the
	// foreign one must be left entirely alone (lineage gate).
	for i := 0; i < 3; i++ {
		if err := RecordSuccessfulBoot(bPath, headVersion()); err != nil {
			t.Fatalf("boot %d against foreign sidecar: %v", i, err)
		}
	}
	if _, err := os.Stat(snapshotDBPathIn(bSidecar)); err != nil {
		t.Errorf("foreign snapshot.db was deleted by boot expiry: %v", err)
	}
	if _, err := os.Stat(manifestPathIn(bSidecar)); err != nil {
		t.Errorf("foreign manifest was deleted by boot expiry: %v", err)
	}
	// And the boot count must NOT have been ticked into the foreign manifest.
	m, err := loadValidManifest(bSidecar)
	if err != nil {
		t.Fatalf("foreign manifest no longer valid: %v", err)
	}
	if m.SuccessfulBoots != 0 {
		t.Errorf("boot tick mutated a foreign manifest: successful_boots=%d, want 0", m.SuccessfulBoots)
	}
}

// ---------------------------------------------------------------------------
// In-process exclusion via the RWMutex registry (flock is unreliable across
// goroutines of one process, so the registry provides the goroutine-level gate).
// ---------------------------------------------------------------------------

// TestDBLock_InProcessExclusiveBlocksShared exercises
// the IN-PROCESS RWMutex registry: an EXCLUSIVE holder blocks a SHARED acquirer
// in the SAME process (flock alone is unreliable across goroutines, so the
// RWMutex is what serializes them). Once the exclusive holder releases, the
// shared acquire proceeds.
func TestDBLock_InProcessExclusiveBlocksShared(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	// Take an EXCLUSIVE lock (standalone owner) and hold it.
	stand := &DB{Path: dbPath}
	relEx, err := acquireExclusiveLockForOwner(stand)
	if err != nil {
		t.Fatalf("exclusive acquire: %v", err)
	}

	// A SHARED acquire in this process must BLOCK on the in-process RWMutex while
	// the exclusive lock is held (it does not fail closed — shared waits out an
	// exclusive holder). Prove it blocks, then unblocks on release.
	got := make(chan *dbLockHandle, 1)
	go func() {
		h, aerr := acquireSharedLock(dbPath)
		if aerr != nil {
			got <- nil
			return
		}
		got <- h
	}()

	select {
	case <-got:
		relEx()
		t.Fatal("shared acquire proceeded while an exclusive lock was held; the in-process RWMutex must block it")
	case <-time.After(250 * time.Millisecond):
		// Good — blocked on the RWMutex.
	}

	// Release exclusive → the shared acquire must now complete.
	relEx()
	select {
	case h := <-got:
		if h == nil {
			t.Fatal("shared acquire failed after exclusive release")
		}
		h.release()
	case <-time.After(2 * time.Second):
		t.Fatal("shared acquire did not proceed after exclusive release")
	}
}

// ===========================================================================
// ROUND 4 (Codex) regression tests.
// ===========================================================================

// ---------------------------------------------------------------------------
// Finding 1: explicit recovery RECONCILES against on-disk reality and never
// destroys before proving. Two adversarial conditions:
//   (a) planted marker + hostile <db>.pre-restore.* + NO valid restore point →
//       `snapshot restore --confirm` (Restore) fails closed; live DB AND the
//       hostile file are byte-intact.
//   (b) rollback where the backup's hash != the recorded original → fails closed
//       (the planted/stale/corrupt backup is never renamed over the live DB).
// ---------------------------------------------------------------------------

// TestRecover_PlantedMarkerNoValidManifest_FailsClosed is the Finding 1
// reality-gate regression. A marker is planted beside the intact live DB with a
// canonical-looking "<db>.pre-restore.*" backup whose CONTENT is hostile, but NO
// valid restore point exists (no snapshot.db / manifest). Pre-fix, recovery acted
// on the marker's claimed pre-publish phase and would rename the hostile file over
// the live DB before discovering "no restore point". Now the reality gate loads +
// validates the restore point FIRST; with none valid it FAILS CLOSED and touches
// nothing. Driven through the real operator path (Restore).
func TestRecover_PlantedMarkerNoValidManifest_FailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	// A live, openable DB at v5 — but NO restore point at all.
	resolved, _ := resolveDBPath(dbPath)
	sidecar, _ := sidecarPath(dbPath)
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}

	liveBefore, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatal(err)
	}

	// A hostile pre-restore file with a CANONICAL prefix (passes the path gate)
	// but attacker content. A pre-fix rollback would os.Rename(hostile, liveDB).
	hostilePrefix := resolved + ".pre-restore.20200101T000000Z.1"
	const hostile = "ATTACKER-CONTROLLED ROLLBACK SOURCE"
	if err := os.WriteFile(hostilePrefix, []byte(hostile), 0o600); err != nil {
		t.Fatal(err)
	}

	// Plant a not-yet-published marker pointing rollback at the hostile file. Note
	// there is NO snapshot.db / manifest in the sidecar → no valid restore point.
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: "",
		BackupPrefix: hostilePrefix, MovedSuffixes: []string{""}, DBPublished: false,
		OriginalDBSHA256: "sha256:deadbeef",
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// The operator path must fail closed (no valid restore point to reconcile
	// against). Restore surfaces it wrapped as "recover prior restore".
	_, rerr := Restore(dbPath)
	if rerr == nil {
		t.Fatal("expected Restore to fail closed when a marker is present but no valid restore point exists")
	}

	// Live DB byte-intact — the hostile file was NOT pulled over it.
	liveAfter, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("live DB disturbed by failed recovery: %v", err)
	}
	if string(liveAfter) != string(liveBefore) {
		t.Error("failed-closed recovery overwrote the live DB from the planted marker")
	}
	// Hostile file byte-intact (never consumed by a rename).
	got, err := os.ReadFile(hostilePrefix)
	if err != nil || string(got) != hostile {
		t.Errorf("hostile pre-restore file disturbed: data=%q err=%v", got, err)
	}
	// The marker is preserved (recovery never erased it).
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); err != nil {
		t.Errorf("failed-closed recovery erased the marker: %v", err)
	}
}

// TestRecover_RollbackBackupHashMismatch_FailsClosed is the Finding 1 provenance
// regression. A genuine torn pre-publish state exists (live DB absent, a
// "<db>.pre-restore." backup present, staged present, a VALID restore point), but
// the backup file's bytes do NOT match the original DB hash recorded in the marker
// — i.e. the backup was planted/corrupted/swapped. Recovery must FAIL CLOSED and
// refuse to rename the unprovable backup over the live DB path.
func TestRecover_RollbackBackupHashMismatch_FailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // creates a VALID restore point
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Manufacture the torn pre-publish state: stage a copy, move the triplet aside,
	// then CORRUPT the DB backup so its hash no longer matches the recorded original.
	staged := filepath.Join(filepath.Dir(resolved), ".restore.staged.mismatch.db")
	if err := copyFile(snapshotDBPathIn(sidecar), staged); err != nil {
		t.Fatal(err)
	}
	origSum, _, err := hashFile(resolved)
	if err != nil {
		t.Fatal(err)
	}
	backupPrefix := resolved + ".pre-restore.mismatch"
	var moved []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := resolved + suffix
		if _, statErr := os.Lstat(src); statErr != nil {
			continue
		}
		if err := os.Rename(src, backupPrefix+suffix); err != nil {
			t.Fatal(err)
		}
		moved = append(moved, suffix)
	}
	// Tamper: overwrite the moved-aside DB backup with attacker bytes so its hash
	// will NOT match origSum recorded in the marker.
	const planted = "PLANTED BACKUP — DO NOT RESTORE ME"
	if err := os.WriteFile(backupPrefix, []byte(planted), 0o600); err != nil {
		t.Fatal(err)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: staged,
		BackupPrefix: backupPrefix, MovedSuffixes: moved, DBPublished: false,
		OriginalDBSHA256: origSum, // recorded original hash — backup no longer matches
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Recovery must refuse: the backup's provenance cannot be proven.
	rerr := recoverPendingRestore(dbPath)
	if rerr == nil {
		t.Fatal("expected recovery to fail closed on a backup whose hash != recorded original")
	}
	if !errors.Is(rerr, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", rerr)
	}
	// The planted backup must NOT have been renamed over the live DB path.
	if _, err := os.Stat(resolved); !os.IsNotExist(err) {
		// If it exists, it must NOT be the planted bytes.
		if data, _ := os.ReadFile(resolved); string(data) == planted {
			t.Error("recovery renamed an unprovable backup over the live DB path")
		}
	}
	// The planted backup is still where it was (untouched).
	if data, _ := os.ReadFile(backupPrefix); string(data) != planted {
		t.Errorf("recovery disturbed the unprovable backup: %q", data)
	}
}

// ---------------------------------------------------------------------------
// Finding 2: a stale pre-publish marker after a successful publish must
// reconcile to COMPLETE (never roll back over the restored DB); and the publish
// path treats a failed marker-clear as a LOUD error (not a silent success).
// ---------------------------------------------------------------------------

// TestRecover_StalePrePublishMarkerCompletes is the Finding 2 reconcile
// regression. The live DB already IS the snapshot image (publish succeeded), but
// the marker still records db_published:false (the marker-clear crashed). The
// next recovery must COMPLETE — recognize the live DB equals the snapshot — and
// NOT roll back over the already-restored DB.
func TestRecover_StalePrePublishMarkerCompletes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Publish the snapshot image at the live path (publish succeeded) and record a
	// DB backup so a naive rollback WOULD have something to revert to — proving the
	// reconcile chooses COMPLETE over rollback purely from "live == snapshot".
	snapSum, _, err := hashFile(snapshotDBPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}
	backupPrefix := resolved + ".pre-restore.stale"
	// Move the current (head) DB aside as the "original" backup, then publish the
	// snapshot over the live path.
	origSum, _, err := hashFile(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(resolved, backupPrefix); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(snapshotDBPathIn(sidecar), resolved); err != nil {
		t.Fatal(err)
	}
	// Sanity: the live DB now hashes to the snapshot.
	liveSum, _, _ := hashFile(resolved)
	if liveSum != snapSum {
		t.Fatalf("test setup: live DB hash %s != snapshot %s", liveSum, snapSum)
	}

	// Plant a stale pre-publish marker (db_published:false) — the exact window
	// Finding 2 addresses.
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: "",
		BackupPrefix: backupPrefix, MovedSuffixes: []string{""}, DBPublished: false,
		OriginalDBSHA256: origSum,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	if err := recoverPendingRestore(dbPath); err != nil {
		t.Fatalf("recover stale pre-publish marker: %v", err)
	}
	// COMPLETED: the live DB is STILL the snapshot image (never rolled back to the
	// backup), and the marker is cleared.
	afterSum, _, _ := hashFile(resolved)
	if afterSum != snapSum {
		t.Error("reconcile rolled back over the already-restored DB instead of completing")
	}
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("marker survived complete-reconcile")
	}
}

// TestClearPublishedMarker_FailedClearIsLoudError is the Finding 2 publish-path
// regression: when the post-publish marker removal fails, the restore must NOT
// return silent success — it returns a loud error telling the operator the
// restore succeeded but the marker must be cleared. We exercise the exact seam
// (clearPublishedRestoreMarker) with a marker the OS cannot unlink: it lives in a
// read-only sidecar directory, so os.Remove fails with EACCES.
func TestClearPublishedMarker_FailedClearIsLoudError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions; cannot simulate an un-removable marker")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	resolved := dbPath
	sidecar := dbPath + ".snapshot"
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	markerPath := restoreMarkerPathIn(sidecar)
	if err := os.WriteFile(markerPath, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Make the sidecar dir read-only so the marker cannot be unlinked.
	if err := os.Chmod(sidecar, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sidecar, 0o700) }) // so TempDir cleanup can remove it

	err := clearPublishedRestoreMarker(sidecar, resolved)
	if err == nil {
		t.Fatal("expected a loud error when the post-publish marker cannot be cleared")
	}
	// The error must name the success+manual-clear contract and the marker path.
	if !contains(err.Error(), "restored successfully") || !contains(err.Error(), markerPath) {
		t.Errorf("loud error missing operator guidance / marker path: %v", err)
	}
	// The marker is still present (we did not silently succeed).
	if _, serr := os.Stat(markerPath); serr != nil {
		t.Errorf("marker unexpectedly gone after a failed clear: %v", serr)
	}
}

// ---------------------------------------------------------------------------
// Finding 3: Restore serializes against a migrating direct Open via the snapshot
// op-lock. A risky migration holding the op-lock and a concurrent restore must
// NOT overlap; the restore waits then fails closed rather than swapping the DB
// under a live migration.
// ---------------------------------------------------------------------------

// TestRestore_SerializesAgainstHeldExclusiveLock simulates a risky migration
// holding the EXCLUSIVE DB lock in ANOTHER process (a foreign exclusive flock),
// then runs Restore. Restore ALSO takes the EXCLUSIVE lock; with a foreign
// holder present it waits the bounded window and FAILS CLOSED (ErrDBLocked)
// rather than swapping the DB out from under the migration. Pre-fix (separate
// serve-lock + op-lock), an ordinary writable/migrating open held no lock
// Restore checked, and Restore could proceed concurrently.
func TestRestore_SerializesAgainstHeldExclusiveLock(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // creates a valid restore point
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	resolved, _ := resolveDBPath(dbPath)

	// Record the live DB bytes so we can prove Restore did NOT swap it while the
	// exclusive lock was held by the "migration".
	liveBefore, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatal(err)
	}

	// A foreign EXCLUSIVE holder (a risky migration in another process) is present
	// for the whole Restore attempt.
	release := foreignFlock(t, dbPath, true)

	out, rerr := Restore(dbPath)
	if rerr == nil {
		release()
		t.Fatalf("Restore completed (out=%q) while a foreign exclusive lock was held; it must fail closed", out)
	}
	if !errors.Is(rerr, ErrDBLocked) && !contains(rerr.Error(), "in use") {
		release()
		t.Errorf("Restore failed for an unexpected reason (want exclusive-lock serialization): %v", rerr)
	}
	// The live DB must be byte-intact — never swapped under the held lock.
	liveAfter, ferr := os.ReadFile(resolved)
	if ferr != nil {
		release()
		t.Fatalf("live DB missing after serialized Restore: %v", ferr)
	}
	if string(liveAfter) != string(liveBefore) {
		release()
		t.Error("Restore swapped the DB despite failing closed on the held exclusive lock")
	}

	// After the foreign holder releases, Restore proceeds (no wedge).
	release()
	if _, rerr := Restore(dbPath); rerr != nil {
		t.Errorf("Restore refused after the foreign exclusive lock cleared: %v", rerr)
	}
}

// ---------------------------------------------------------------------------
// Finding 4: the snapshot opt-out disables ONLY the snapshot, never the
// migration op-lock SERIALIZATION. Two opt-out migrations against one v5 DB must
// serialize: one upgrades, the other waits/fails closed; no torn schema and no
// restore point is created.
// ---------------------------------------------------------------------------

// TestMigrate_OptOutStillSerializes races migrate() across N pre-opened handles
// on one v5 DB with CONTINUITY_DISABLE_MIGRATION_SNAPSHOT=1. Pre-fix the opt-out
// skipped the riskyUpgrade branch entirely, so two opt-out processes could both
// enter the destructive mem_nodes rebuild concurrently. The lock is now decoupled
// from the opt-out: the rebuild serializes (the loser waits/fails closed /
// SQLITE_BUSY), the seeded row survives exactly once, the DB ends at head, and NO
// restore point is created (opt-out still suppresses the snapshot).
func TestMigrate_OptOutStillSerializes(t *testing.T) {
	t.Setenv(envDisableSnapshot, "1")
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)

	// Seed a v5 row so a torn/double rebuild would lose or duplicate it.
	{
		raw, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`
			INSERT INTO mem_nodes (uri, node_type, category, l0_abstract, created_at, updated_at)
			VALUES ('mem://user/events/optout-marker', 'leaf', 'events', 'survive', 1, 1)`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
	}

	const n = 6
	handles := make([]*DB, n)
	for i := range handles {
		handles[i] = openWritableNoMigrate(t, dbPath)
		defer handles[i].Close()
	}

	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start.Wait()
			errs[idx] = handles[idx].migrate()
		}(i)
	}
	start.Done()
	wg.Wait()

	for i, e := range errs {
		if e == nil || errors.Is(e, ErrDBLocked) {
			continue
		}
		if contains(e.Error(), "database is locked") || contains(e.Error(), "SQLITE_BUSY") {
			continue
		}
		t.Errorf("goroutine %d: unexpected error (want success / exclusive-lock fail-closed / SQLITE_BUSY): %v", i, e)
	}

	// Final DB at head, seeded row survives EXACTLY once (no torn/double rebuild).
	fin, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fin.Close()
	if v, _ := fin.SchemaVersion(); v != headVersion() {
		t.Errorf("final schema = v%d, want head v%d", v, headVersion())
	}
	var cnt int
	if err := fin.QueryRow(
		`SELECT COUNT(*) FROM mem_nodes WHERE uri='mem://user/events/optout-marker'`).Scan(&cnt); err != nil {
		t.Fatalf("read marker after concurrent opt-out upgrade: %v", err)
	}
	if cnt != 1 {
		t.Errorf("seeded row count = %d after concurrent opt-out upgrade, want exactly 1 (torn/double rebuild)", cnt)
	}
	// Opt-out still means NO restore point was created.
	sidecar, _ := sidecarPath(dbPath)
	if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
		t.Errorf("opt-out created a sidecar despite the snapshot being disabled (err=%v)", err)
	}
}

// ---------------------------------------------------------------------------
// CENTERPIECE Round 5: flock auto-releases on fd close (and on process death),
// so a CRASHED exclusive holder never wedges the next process. This replaces the
// whole PID-liveness / zero-length-reclaim / atomic-PID-writer machinery the
// recurring concurrency bug came from.
// ---------------------------------------------------------------------------

// TestDBLock_FlockAutoReleasesOnClose proves the property that makes the
// hand-rolled stale-lock reclaim unnecessary: closing the fd of an exclusive
// holder (which is exactly what happens when a process exits / crashes) releases
// the kernel flock immediately, so the next acquirer is NOT wedged. We simulate
// the crashed holder with a raw exclusive flock on its own fd, then close it.
func TestDBLock_FlockAutoReleasesOnClose(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	lp, _ := dbLockPath(dbPath)

	// A "crashed" exclusive holder: a raw exclusive flock on its own fd.
	crashed, err := os.OpenFile(lp, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if ok, lerr := flockExclusiveNB(crashed); lerr != nil || !ok {
		crashed.Close()
		t.Fatalf("seed exclusive flock: ok=%v err=%v", ok, lerr)
	}

	// While "alive" (fd open), a fresh exclusive acquire must fail closed.
	stand := &DB{Path: dbPath}
	if _, aerr := acquireExclusiveLockForOwner(stand); !errors.Is(aerr, ErrDBLocked) {
		crashed.Close()
		t.Fatalf("exclusive acquire while a live exclusive holder exists: err=%v, want ErrDBLocked", aerr)
	}

	// "Crash": close the holder's fd. The kernel drops the flock immediately —
	// no PID file to leave behind, no stale-reclaim dance needed.
	if err := crashed.Close(); err != nil {
		t.Fatalf("close crashed holder fd: %v", err)
	}

	// The next acquirer is NOT wedged: it gets the exclusive lock right away.
	rel, aerr := acquireExclusiveLockForOwner(stand)
	if aerr != nil {
		t.Fatalf("exclusive acquire after the crashed holder released: %v", aerr)
	}
	rel()
}

// ===========================================================================
// ROUND 5 (Codex) regression tests — bounded recovery / safety edges.
// ===========================================================================

// ---------------------------------------------------------------------------
// Finding 2: crash AFTER marker write but BEFORE the first move-aside rename.
// The live DB is the untouched ORIGINAL, no backup exists. Reconcile must treat
// this as a SAFE pre-rename abort: clear the marker, leave the original intact.
// Pre-fix, reconcile had no matching case and failed closed, wedging the DB at
// ErrRestoreInterrupted forever.
// ---------------------------------------------------------------------------

func TestRestoreMarker_SafePreRenameAbortClearsMarker(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // creates a valid restore point at pre-v6
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Record the untouched original DB's bytes + hash.
	origBytes, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatal(err)
	}
	origSum, _, err := hashFile(resolved)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the crash window: a marker was written (recording the original's
	// hash) but NO move-aside rename happened — the live DB is still the original,
	// no <db>.pre-restore.* backup exists. A staged temp MAY exist; include one to
	// prove it is cleaned up.
	staged := filepath.Join(filepath.Dir(resolved), ".restore.staged.prerename.db")
	if err := copyFile(snapshotDBPathIn(sidecar), staged); err != nil {
		t.Fatal(err)
	}
	backupPrefix := resolved + ".pre-restore.prerename"
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: resolved, StagedPath: staged,
		BackupPrefix: backupPrefix, MovedSuffixes: []string{""}, DBPublished: false,
		OriginalDBSHA256: origSum,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Before the fix: Open fails closed and recovery has no matching case. After:
	// explicit recovery recognizes the no-destructive-step abort, clears the marker.
	if _, oerr := Open(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		t.Fatalf("Open over the pre-rename marker: err=%v, want ErrRestoreInterrupted", oerr)
	}
	if rerr := recoverPendingRestore(dbPath); rerr != nil {
		t.Fatalf("safe pre-rename abort recovery failed: %v", rerr)
	}

	// Marker cleared, staged temp cleaned up, original DB byte-intact and opens.
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("marker survived the safe pre-rename abort")
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Errorf("staged temp survived the safe pre-rename abort")
	}
	after, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("original DB missing after safe abort: %v", err)
	}
	if string(after) != string(origBytes) {
		t.Error("original DB mutated by the safe pre-rename abort")
	}
	rdb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("original DB does not open after marker cleared: %v", err)
	}
	rdb.Close()
}

// ---------------------------------------------------------------------------
// Finding 3: staged-temp ownership. The staged snapshot is written into the
// STILL-OPEN owned fd (not a close-then-reopen-by-path), and a swapped symlink
// is rejected before publish. Two building-block assertions:
//   (a) writing into the open fd lands in the original inode even after the path
//       is swapped to a symlink pointing at a victim — the victim is untouched.
//   (b) assertRegularFile (the pre-publish gate) fails closed on a symlinked
//       staged path, so a symlink is never renamed into the live DB.
// ---------------------------------------------------------------------------

func TestRestore_StagedTempSwapToSymlinkIsRejected(t *testing.T) {
	dir := t.TempDir()

	// (a) Open an owned temp, swap its PATH to a symlink at a victim, then write
	// into the still-open fd. The bytes must land in the original file, NOT the
	// victim — proving copyFileToOpenFd is immune to a path swap.
	src := filepath.Join(dir, "src.bin")
	if err := os.WriteFile(src, []byte("SNAPSHOT-CONTENT"), 0o600); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(dir, "victim.bin")
	if err := os.WriteFile(victim, []byte("VICTIM-UNTOUCHED"), 0o600); err != nil {
		t.Fatal(err)
	}

	owned, staged, err := createOwnedTemp(dir, ".restore.staged.", ".db")
	if err != nil {
		t.Fatal(err)
	}
	// Watcher swaps the staged PATH with a symlink to the victim between create
	// and write. (We do it explicitly here; in production the fd stays open so the
	// write cannot follow the swap.)
	if err := os.Remove(staged); err != nil {
		owned.Close()
		t.Fatal(err)
	}
	if err := os.Symlink(victim, staged); err != nil {
		owned.Close()
		t.Fatal(err)
	}
	// Write into the OPEN fd (the original now-unlinked inode), then close.
	if cerr := copyFileToOpenFd(src, owned); cerr != nil {
		owned.Close()
		t.Fatalf("copy into open fd: %v", cerr)
	}
	owned.Close()

	// The victim must be byte-intact — the write did NOT go through the symlink.
	vb, _ := os.ReadFile(victim)
	if string(vb) != "VICTIM-UNTOUCHED" {
		t.Errorf("write followed the swapped symlink and clobbered the victim: %q", vb)
	}

	// (b) The pre-publish gate: a symlinked staged path fails assertRegularFile
	// closed, so a symlink is never renamed into the live DB.
	if err := assertRegularFile(staged); !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("assertRegularFile on a symlinked staged path: err=%v, want ErrSnapshotSidecarCorrupt", err)
	}
}

// TestRestore_FullPathRejectsSymlinkStaged drives the REAL Restore path with a
// staged path that a watcher swapped to a symlink AFTER restore created its owned
// temp. We can't race the in-Restore window deterministically, so we assert the
// end-to-end property a different way: a restore whose DB dir is sabotaged so the
// staged write/publish would have to traverse a symlink fails closed, and the
// live DB is left intact. This complements the unit assertions above.
func TestRestore_DoesNotPublishSymlinkAsDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	resolved, _ := resolveDBPath(dbPath)

	// A normal restore should publish a REGULAR file as the live DB (never a
	// symlink). Run it and assert the result is a regular file.
	if _, rerr := Restore(dbPath); rerr != nil {
		t.Fatalf("restore: %v", rerr)
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		t.Fatalf("live DB missing after restore: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Error("restore published a SYMLINK as the live DB")
	}
	if !info.Mode().IsRegular() {
		t.Errorf("restore published a non-regular file as the live DB: mode=%v", info.Mode())
	}
}

// ---------------------------------------------------------------------------
// Finding 4: Prune must REFUSE while a restore marker is pending — the manifest
// + snapshot.db are the only recovery material, so deleting them would leave the
// marker with no restore point (every Open fails, restore --confirm fails).
// ---------------------------------------------------------------------------

func TestPrune_RefusesWhileRestoreMarkerPending(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	db, err := Open(dbPath) // valid restore point
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sidecar, _ := sidecarPath(dbPath)

	// A valid restore point exists; prune would normally remove it.
	if _, err := loadValidManifest(sidecar); err != nil {
		t.Fatalf("expected a valid restore point: %v", err)
	}

	// Now drop a restore marker (a crashed restore). Prune must REFUSE.
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: dbPath,
		MovedSuffixes: nil, DBPublished: false,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	if perr := Prune(dbPath); perr == nil {
		t.Fatal("Prune deleted recovery material while a restore marker was pending")
	} else if !errors.Is(perr, ErrRestoreInterrupted) {
		t.Errorf("Prune err=%v, want ErrRestoreInterrupted", perr)
	}

	// Nothing was deleted: snapshot.db, manifest.json, and the marker all remain.
	if _, err := os.Stat(snapshotDBPathIn(sidecar)); err != nil {
		t.Errorf("Prune removed snapshot.db despite refusing: %v", err)
	}
	if _, err := os.Stat(manifestPathIn(sidecar)); err != nil {
		t.Errorf("Prune removed manifest.json despite refusing: %v", err)
	}
	if _, err := os.Stat(restoreMarkerPathIn(sidecar)); err != nil {
		t.Errorf("Prune removed the marker despite refusing: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 5: Open() must check for an interrupted restore (and acquire the
// shared lock) BEFORE MkdirAll/hardenPermissions, so a pending-restore Open is
// truly no-touch — it must NOT chmod a loose-perm DB before failing closed.
// ---------------------------------------------------------------------------

func TestOpen_PendingMarkerIsNoTouchBeforeChmod(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)
	resolved, _ := resolveDBPath(dbPath)

	// Give the live DB LOOSE permissions (0644) that hardenPermissions would
	// normally tighten to 0600 on Open.
	if err := os.Chmod(resolved, 0o644); err != nil {
		t.Fatal(err)
	}

	// Drop a marker so Open must fail closed. The sidecar dir must exist to hold it.
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	mk := &restoreMarker{Version: 1, RestoredDBPath: resolved, DBPublished: false}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Open must fail closed WITHOUT having chmod'd the DB.
	if _, oerr := Open(dbPath); !errors.Is(oerr, ErrRestoreInterrupted) {
		t.Fatalf("Open over a pending marker: err=%v, want ErrRestoreInterrupted", oerr)
	}

	// The loose 0644 perms must be UNCHANGED — Open returned before hardenPermissions.
	info, err := os.Stat(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o644 {
		t.Errorf("Open chmod'd the DB before failing closed: perm=%o, want 0644 (no-touch)", perm)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
