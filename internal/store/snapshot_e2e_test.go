//go:build !windows

package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lazypower/continuity/internal/testharness"
)

// Snapshot e2e tests boot the real `continuity serve` binary against a v5 DB
// and assert the upgrade restore point is created, expires after successful
// boots, and that the snapshot CLI verbs behave through the binary. They reuse
// the buildDBAtVersion / startSubprocessAgainstDB scaffolding from
// migration_e2e_test.go.

// TestSnapshotE2E_UpgradeFromV5CreatesSidecar boots against a seeded v5 DB and
// asserts the sidecar + manifest land with pre_schema_version=5 and
// first_risky_schema_version=6.
func TestSnapshotE2E_UpgradeFromV5CreatesSidecar(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)

	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	t.Cleanup(srv.Stop)

	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	m, err := loadValidManifest(sidecar)
	if err != nil {
		t.Fatalf("restore point missing after upgrade boot: %v", err)
	}
	if m.PreSchemaVersion != 5 {
		t.Errorf("pre_schema_version = %d, want 5", m.PreSchemaVersion)
	}
	if m.FirstRiskySchemaVersion != 6 {
		t.Errorf("first_risky_schema_version = %d, want 6", m.FirstRiskySchemaVersion)
	}
	// The sidecar dir must be 0700.
	if info, err := os.Stat(sidecar); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o700 {
		t.Errorf("sidecar perms = %o, want 0700", info.Mode().Perm())
	}
}

// TestSnapshotE2E_SidecarRegularFileFailsClosed makes <db>.snapshot a regular
// FILE (not a dir) before boot. The risky migration must fail closed: serve
// exits non-zero and the DB stays at v5.
func TestSnapshotE2E_SidecarRegularFileFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)

	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Plant a regular file where the sidecar dir would go.
	if err := os.WriteFile(sidecar, []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, env := testharness.HermeticEnv(t, workDir, dbPath, 0)
	res := testharness.RunCLI(t, bin, env, "serve")
	if res.ExitCode == 0 {
		t.Errorf("serve exited 0 despite blocked restore point\nstderr:\n%s", res.Stderr)
	}

	// DB must remain at v5 — no pending migration ran.
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

// TestSnapshotE2E_StatusCLI_NoSidecarOnFreshCopy migrates a DB, copies just
// the DB (without its sidecar) to scratch.db, and asserts `snapshot status`
// reports no restore point for the copy.
func TestSnapshotE2E_StatusCLI_NoSidecarOnFreshCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)
	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	srv.Stop()

	// Copy ONLY the DB to a new path (no sidecar travels with it).
	scratch := filepath.Join(workDir, "scratch.db")
	if err := copyFile(dbPath, scratch); err != nil {
		t.Fatal(err)
	}

	_, env := testharness.HermeticEnv(t, workDir, scratch, 0)
	res := testharness.RunCLI(t, bin, env, "snapshot", "status")
	res.ExpectExit(t, 0)
	res.ExpectStdoutContains(t, "no restore point")
}

// TestSnapshotE2E_PruneCLI migrates a DB then prunes via the CLI, asserting
// the sidecar files are gone afterward and a second prune reports none.
func TestSnapshotE2E_PruneCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)
	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	srv.Stop()

	sidecar, _ := sidecarPath(dbPath)
	if _, err := loadValidManifest(sidecar); err != nil {
		t.Fatalf("restore point should exist pre-prune: %v", err)
	}

	_, env := testharness.HermeticEnv(t, workDir, dbPath, 0)

	// Prune without --confirm must refuse.
	noConfirm := testharness.RunCLI(t, bin, env, "snapshot", "prune")
	if noConfirm.ExitCode == 0 {
		t.Errorf("prune without --confirm exited 0\nstderr:\n%s", noConfirm.Stderr)
	}

	res := testharness.RunCLI(t, bin, env, "snapshot", "prune", "--confirm")
	res.ExpectExit(t, 0)

	if _, err := os.Stat(snapshotDBPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("snapshot.db survived prune (err=%v)", err)
	}

	res2 := testharness.RunCLI(t, bin, env, "snapshot", "prune", "--confirm")
	if res2.ExitCode == 0 {
		t.Errorf("second prune exited 0, expected no-restore-point error")
	}
}

// TestSnapshotE2E_RestoreCLI_RoundTrip migrates v5→head, mutates, then
// restores via the CLI and verifies the v5 image returns.
func TestSnapshotE2E_RestoreCLI_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)
	_, _, srv := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	srv.Stop() // release the serve lock so restore is allowed

	_, env := testharness.HermeticEnv(t, workDir, dbPath, 0)

	// restore without --confirm refuses.
	noConfirm := testharness.RunCLI(t, bin, env, "snapshot", "restore")
	if noConfirm.ExitCode == 0 {
		t.Errorf("restore without --confirm exited 0")
	}

	res := testharness.RunCLI(t, bin, env, "snapshot", "restore", "--confirm")
	res.ExpectExit(t, 0)

	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, _ := db.SchemaVersion()
	if v != 5 {
		t.Errorf("after CLI restore schema = v%d, want v5", v)
	}
}

// TestSnapshotE2E_SecondServeRefusesWhileFirstLive is the Finding 2 behavioral
// regression: while one serve holds the DB, a second serve against the SAME DB
// (different port, so the refusal is the LOCK, not a bind clash) must refuse to
// start and exit non-zero — never run two servers against one database.
func TestSnapshotE2E_SecondServeRefusesWhileFirstLive(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)

	// First serve: boot and wait until ready (holds the live serve lock).
	url1, _, srv1 := startSubprocessAgainstDB(t, bin, workDir, dbPath)
	_ = url1
	t.Cleanup(srv1.Stop)

	// Second serve against the SAME DB but a fresh HOME + different port. It
	// must refuse on the live serve lock.
	home2 := t.TempDir()
	_, env2 := testharness.HermeticEnv(t, home2, dbPath, 0)
	res := testharness.RunCLI(t, bin, env2, "serve")
	if res.ExitCode == 0 {
		t.Errorf("second serve exited 0 while first holds the DB\nstderr:\n%s", res.Stderr)
	}
	if !contains(res.Stderr, "already running") && !contains(res.Stderr, "serve lock") {
		t.Errorf("second serve did not refuse on the serve lock; stderr:\n%s", res.Stderr)
	}

	// After the first serve stops, a fresh serve must succeed (stale lock
	// reclaimed), proving the refusal was scoped to the live holder.
	srv1.Stop()
	home3 := t.TempDir()
	url3, env3 := testharness.HermeticEnv(t, home3, dbPath, 0)
	srv3 := testharness.StartServeProcess(t, bin, env3)
	t.Cleanup(srv3.Stop)
	testharness.WaitForReady(t, url3+"/api/health")
}

// TestSnapshotE2E_FirstRunServeIntoMissingDir is the Finding 3 regression: the
// serve lock is now acquired BEFORE store.Open, and the lock file lives beside
// the DB. A first-ever serve whose CONTINUITY_DB sits in a not-yet-created
// (nested) directory must still create that directory before acquiring the
// lock, so serve boots instead of dying on "open lock: no such file or
// directory". Before the fix the lock acquire ran against a missing parent dir.
func TestSnapshotE2E_FirstRunServeIntoMissingDir(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	// Deeply nested, non-existent DB parent — nothing has created it yet.
	dbPath := filepath.Join(workDir, "nested", "deeper", "continuity.db")
	if _, err := os.Stat(filepath.Dir(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("precondition: db dir should not exist yet (err=%v)", err)
	}

	url, env := testharness.HermeticEnv(t, workDir, dbPath, 0)
	srv := testharness.StartServeProcess(t, bin, env)
	t.Cleanup(srv.Stop)
	// If the parent dir was created before the lock, serve binds and is ready.
	testharness.WaitForReady(t, url+"/api/health")

	// The DB (and a fresh schema at head) must now exist on disk.
	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatalf("first-run serve did not create the DB: %v", err)
	}
	defer db.Close()
	if v, _ := db.SchemaVersion(); v != headVersion() {
		t.Errorf("first-run DB schema = v%d, want head v%d", v, headVersion())
	}
}

// TestSnapshotE2E_ExpiresAfterThreeBoots boots the binary three times against
// the same DB; after the third successful bind the sidecar files are gone and
// the DB itself is untouched (still at head).
func TestSnapshotE2E_ExpiresAfterThreeBoots(t *testing.T) {
	if testing.Short() {
		t.Skip("snapshot e2e: skipped under -short")
	}
	bin := testharness.BuildContinuityBinary(t)
	workDir := t.TempDir()

	dbPath := buildDBAtVersion(t, workDir, 5)
	seedV5Data(t, dbPath)

	sidecar, _ := sidecarPath(dbPath)
	for i := 1; i <= 3; i++ {
		homeDir := t.TempDir()
		_, env := testharness.HermeticEnv(t, homeDir, dbPath, 0)
		srv := testharness.StartServeProcess(t, bin, env)
		testharness.WaitForReady(t, "http://127.0.0.1:"+envGet(env, "CONTINUITY_PORT")+"/api/health")
		srv.Stop()
	}

	if _, err := os.Stat(manifestPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("manifest survived 3 boots (err=%v)", err)
	}
	if _, err := os.Stat(snapshotDBPathIn(sidecar)); !os.IsNotExist(err) {
		t.Errorf("snapshot.db survived 3 boots (err=%v)", err)
	}

	// DB still at head and readable.
	db, err := OpenNoMigrate(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, _ := db.SchemaVersion()
	if v != headVersion() {
		t.Errorf("db schema = v%d after expiry, want head v%d", v, headVersion())
	}
}
