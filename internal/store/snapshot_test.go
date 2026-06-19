//go:build !windows

package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// ---------------------------------------------------------------------------
// Finding 2: serve lock exclusion + dead-PID reclaim + own-PID release
// ---------------------------------------------------------------------------

// TestServeLock_ExclusionReclaimAndOwnRelease covers all three serve-lock
// hardening properties:
//   - a SECOND acquire while a LIVE lock is held is refused (ErrServeLockHeld),
//     so a second serve cannot clobber the first's lock;
//   - a STALE (dead-PID) lock is reclaimed by a new acquire;
//   - release removes the lock ONLY if we still own it (a foreign-PID lock that
//     we did not write is left intact).
func TestServeLock_ExclusionReclaimAndOwnRelease(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	lp, _ := serveLockPath(dbPath)

	// First acquire (our live PID) succeeds.
	rel1, err := AcquireServeLock(dbPath)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Simulate a SECOND, different live process by planting a lock with another
	// live PID (the test process's parent is alive; use PID 1 which always is).
	if err := os.WriteFile(lp, []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireServeLock(dbPath); !errors.Is(err, ErrServeLockHeld) {
		t.Errorf("acquire over live foreign lock: err=%v, want ErrServeLockHeld", err)
	}

	// rel1 must NOT remove the foreign lock (we no longer own it: it records
	// PID 1, not us).
	rel1()
	if _, err := os.Stat(lp); err != nil {
		t.Errorf("release removed a lock we no longer own: %v", err)
	}

	// Replace with a STALE (dead) PID; a new acquire must reclaim it.
	if err := os.WriteFile(lp, []byte("999999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rel2, err := AcquireServeLock(dbPath)
	if err != nil {
		t.Fatalf("acquire over stale lock should reclaim: %v", err)
	}
	// The reclaimed lock must now record OUR pid.
	owner, alive, _ := readServeLockOwner(lp)
	if owner != os.Getpid() || !alive {
		t.Errorf("reclaimed lock owner=%d alive=%v, want our live pid", owner, alive)
	}
	// And our release removes it (we own it).
	rel2()
	if _, err := os.Stat(lp); !os.IsNotExist(err) {
		t.Errorf("own-lock release did not remove the lock (err=%v)", err)
	}
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
// (DBPublished=false): the live DB is missing. resumeRestoreIfPending must ROLL
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
	// write a not-yet-published marker, then leave the live DB MISSING.
	backupPrefix := dbPath + ".pre-restore.crashtest"
	staged := filepath.Join(dir, ".restore.staged.crash.db")
	if err := copyFile(snapshotDBPathIn(sidecar), staged); err != nil {
		t.Fatal(err)
	}
	var moved []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := dbPath + suffix
		if _, statErr := os.Lstat(src); statErr != nil {
			continue
		}
		if err := os.Rename(src, backupPrefix+suffix); err != nil {
			t.Fatal(err)
		}
		moved = append(moved, suffix)
	}
	// Plant a stale -wal at the live name to prove rollback scrubs/handles it.
	if err := os.WriteFile(dbPath+"-wal", []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: dbPath, StagedPath: staged,
		BackupPrefix: backupPrefix, MovedSuffixes: moved, DBPublished: false,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	// Resume: must roll back.
	if err := resumeRestoreIfPending(dbPath); err != nil {
		t.Fatalf("resume rollback: %v", err)
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
// staged snapshot was published (DBPublished=true) but BEFORE wal/shm scrub and
// marker removal. resumeRestoreIfPending must COMPLETE: keep the restored DB,
// scrub any stale live -wal/-shm, and clear the marker.
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

	// The DB at dbPath is already the (head) live DB; pretend it is the freshly
	// published restored image. Plant a stale -wal/-shm at the live names and a
	// published marker.
	if err := os.WriteFile(dbPath+"-wal", []byte("stale-wal"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath+"-shm", []byte("stale-shm"), 0o600); err != nil {
		t.Fatal(err)
	}
	mk := &restoreMarker{
		Version: 1, RestoredDBPath: dbPath, StagedPath: "",
		BackupPrefix: dbPath + ".pre-restore.x", MovedSuffixes: []string{"", "-wal", "-shm"},
		DBPublished: true,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		t.Fatal(err)
	}

	if err := resumeRestoreIfPending(dbPath); err != nil {
		t.Fatalf("resume complete: %v", err)
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
// Finding 6: a stale restore point from an earlier upgrade window fails closed
// ---------------------------------------------------------------------------

// TestCreateRestorePoint_StalePreVersionFailsClosed: a valid same-lineage
// manifest whose pre_schema_version does NOT match the current upgrade's
// pre-version must NOT be reused and must NOT be overwritten — createRestorePoint
// fails closed so the operator restores or prunes explicitly.
func TestCreateRestorePoint_StalePreVersionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "continuity.db")
	buildDBAtVersionStandalone(t, dbPath, 5)
	sidecar, _ := sidecarPath(dbPath)

	// Create a restore point recording pre-v5 (the existing window).
	db := openWritableNoMigrate(t, dbPath)
	defer db.Close()
	if err := db.createRestorePoint(5, headVersion(), 6); err != nil {
		t.Fatalf("seed restore point: %v", err)
	}

	// Snapshot the on-disk manifest bytes so we can prove no overwrite.
	before, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}

	// Now a DIFFERENT upgrade run starts at pre-v8 (same DB lineage). Reuse must
	// be refused because the existing point captures pre-v5, not pre-v8.
	err = db.createRestorePoint(8, headVersion(), 9)
	if err == nil {
		t.Fatal("expected fail-closed when existing restore point is from a different upgrade window")
	}
	if !errors.Is(err, ErrSnapshotSidecarCorrupt) {
		t.Errorf("err = %v, want ErrSnapshotSidecarCorrupt", err)
	}
	after, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("stale restore point was overwritten on fail-closed")
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

func contains(s, sub string) bool { return strings.Contains(s, sub) }
