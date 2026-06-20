// Command genfixtures mints a migration golden fixture from a REAL released
// continuity binary.
//
// Given the path to an old binary and the schema version it ships, it:
//
//  1. boots that binary with an isolated HOME so it creates + self-migrates a
//     throwaway database to its own schema (migrate() runs inside store.Open,
//     before the HTTP server binds — so we never depend on the old binary's
//     port or health surface);
//  2. polls that database until schema_versions reports the expected version;
//  3. stops the binary, seeds version-appropriate rows through the shared
//     seeder in internal/store (single source of truth with the tests);
//  4. emits a clean, self-contained single-file image via VACUUM INTO (no
//     -wal/-shm sidecars), which becomes the committed golden.
//
// It deliberately uses the raw sqlite driver — never store.Open — for the poll
// and seed so the current engine does not migrate the old image forward; that
// migration is exactly what the regression test exercises.
//
// Usage:
//
//	go run ./scripts/genfixtures -bin <old-binary> -schema <N> -out <golden.db>
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/lazypower/continuity/internal/store"
)

func main() {
	bin := flag.String("bin", "", "path to the old released continuity binary")
	schema := flag.Int("schema", 0, "schema version the old binary ships (5, 7, or 8)")
	out := flag.String("out", "", "destination path for the golden .db")
	timeout := flag.Duration("timeout", 60*time.Second, "max time to wait for the binary to migrate")
	flag.Parse()

	if *bin == "" || *schema == 0 || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: genfixtures -bin <binary> -schema <N> -out <path>")
		os.Exit(2)
	}

	if err := run(*bin, *schema, *out, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "genfixtures: %v\n", err)
		os.Exit(1)
	}
}

func run(bin string, schema int, out string, timeout time.Duration) error {
	absBin, err := filepath.Abs(bin)
	if err != nil {
		return fmt.Errorf("resolve binary path: %w", err)
	}
	if _, err := os.Stat(absBin); err != nil {
		return fmt.Errorf("binary not found: %w", err)
	}

	// Isolated HOME → the old binary writes to <home>/.continuity/continuity.db
	// (all shipped versions resolve the DB via os.UserHomeDir; none honor an env
	// override). A throwaway dir keeps generation from ever touching a real DB.
	home, err := os.MkdirTemp("", "genfixtures-home-")
	if err != nil {
		return fmt.Errorf("temp home: %w", err)
	}
	defer os.RemoveAll(home)
	dbPath := filepath.Join(home, ".continuity", "continuity.db")

	if err := bootAndMigrate(absBin, home, dbPath, schema, timeout); err != nil {
		return err
	}

	// Seed through the shared seeder using the RAW driver so we do not migrate
	// the image forward. The current engine's store.Open would bump it to head.
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open minted db: %w", err)
	}
	defer rawDB.Close()
	if err := store.SeedSchemaVersion(rawDB, schema); err != nil {
		return fmt.Errorf("seed schema v%d: %w", schema, err)
	}

	// VACUUM INTO yields a single self-contained file (folds in the WAL, drops
	// -wal/-shm). Destination must not exist.
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}
	_ = os.Remove(out)
	if _, err := rawDB.Exec("VACUUM INTO ?", out); err != nil {
		return fmt.Errorf("vacuum into %s: %w", out, err)
	}

	fmt.Fprintf(os.Stderr, "genfixtures: wrote %s (schema v%d, seeded)\n", out, schema)
	return nil
}

// bootAndMigrate spawns the old binary's `serve` with an isolated HOME, waits
// until the database reports the expected schema version, then stops it.
func bootAndMigrate(bin, home, dbPath string, schema int, timeout time.Duration) error {
	cmd := exec.Command(bin, "serve")
	cmd.Env = append(os.Environ(),
		"HOME="+home,
		// Snapshots are a current-binary concept; old binaries ignore this. Set
		// it anyway so any future-dated binary used as a fixture source stays
		// hands-off. Harmless on the versions we actually mint from.
		store.EnvNoMigrationSnapshot+"=1",
	)
	cmd.Stdout = os.Stderr // serve logs to stderr; keep our stdout clean
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s serve: %w", bin, err)
	}

	// Ensure the process is reaped no matter how we exit.
	stopped := make(chan struct{})
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
	}()
	go func() { _ = cmd.Wait(); close(stopped) }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		v, err := readSchemaVersion(dbPath)
		if err == nil && v == schema {
			return nil
		}
		if err == nil && v > schema {
			return fmt.Errorf("minted schema v%d exceeds expected v%d (wrong binary?)", v, schema)
		}
		// If the process died before migrating, surface that early.
		select {
		case <-stopped:
			if v, err := readSchemaVersion(dbPath); err == nil && v == schema {
				return nil
			}
			return fmt.Errorf("binary exited before reaching schema v%d", schema)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for schema v%d at %s", schema, dbPath)
}

// readSchemaVersion reads MAX(schema_versions.version) from a DB without
// migrating it. Returns 0 if the table/file isn't there yet.
func readSchemaVersion(dbPath string) (int, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return 0, err
	}
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var v int
	err = db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_versions`).Scan(&v)
	if err != nil {
		return 0, err
	}
	return v, nil
}
