package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/lazypower/continuity/internal/store"
	"github.com/spf13/cobra"
)

// The `snapshot` command group manages the path-owned upgrade restore point
// (see internal/store/snapshot.go). status/prune NEVER open the database —
// they derive the sidecar purely from the DB path. restore opens the current
// DB read-only (no migrate) ONLY to recompute the lineage fingerprint.

var (
	snapshotRestoreConfirm bool
	snapshotPruneConfirm   bool
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Manage the migration restore point",
	Long: `Inspect, restore, or prune the upgrade restore point continuity takes
automatically before a destructive (risky) schema migration.

The restore point is a path-owned sidecar next to your database
(<db>.snapshot/), containing a consistent pre-upgrade image plus a small
manifest. It is NOT a general backup system: it is a one-shot rollback for the
specific upgrade window, and it auto-expires after a few successful serve boots.

  continuity snapshot status            Show the current restore point (read-only)
  continuity snapshot restore --confirm Roll the DB back to the restore point
  continuity snapshot prune  --confirm  Remove the restore point now`,
}

var snapshotStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show the current migration restore point (never opens the DB)",
	RunE:  runSnapshotStatus,
}

var snapshotRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Roll the database back to the migration restore point",
	Long: `Replace the current database with the upgrade restore-point snapshot.

This is destructive to the live database, but non-destructive to your data on
disk: the current db / db-wal / db-shm files are renamed aside to timestamped
pre-restore names (never deleted) before the snapshot is moved into place.

Refuses unless: the manifest + snapshot validate, the snapshot passes an
integrity check, the current DB's lineage matches the manifest, the current
schema version is within the restore window, and no live 'continuity serve'
holds the database. Requires --confirm.`,
	RunE: runSnapshotRestore,
}

var snapshotPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Remove the migration restore point now (never opens the DB)",
	Long: `Delete the restore point's snapshot.db and manifest.json.

Refuses (fails closed) if the sidecar is corrupt or partial — continuity never
deletes anything it cannot first prove is its own. Requires --confirm.`,
	RunE: runSnapshotPrune,
}

func init() {
	snapshotRestoreCmd.Flags().BoolVar(&snapshotRestoreConfirm, "confirm", false, "Required: confirm the destructive restore")
	snapshotPruneCmd.Flags().BoolVar(&snapshotPruneConfirm, "confirm", false, "Required: confirm removal of the restore point")

	snapshotCmd.AddCommand(snapshotStatusCmd)
	snapshotCmd.AddCommand(snapshotRestoreCmd)
	snapshotCmd.AddCommand(snapshotPruneCmd)
}

// resolveSnapshotDBPath honors CONTINUITY_DB (the same override serve and the
// other CLI commands use) else falls back to DefaultDBPath. It does NOT open
// the DB — status/prune must stay open-free.
func resolveSnapshotDBPath() (string, error) {
	if v := strings.TrimSpace(os.Getenv("CONTINUITY_DB")); v != "" {
		return v, nil
	}
	return store.DefaultDBPath()
}

func runSnapshotStatus(cmd *cobra.Command, args []string) error {
	dbPath, err := resolveSnapshotDBPath()
	if err != nil {
		return err
	}
	st, err := store.Status(dbPath)
	if err != nil {
		return err
	}

	if st.Sidecar != "" {
		fmt.Printf("db:      %s\n", dbPath)
		fmt.Printf("sidecar: %s\n", st.Sidecar)
	}

	if st.Problem != "" {
		fmt.Printf("status:  PRESENT BUT UNUSABLE\n")
		fmt.Printf("problem: %s\n", st.Problem)
		// Non-zero exit so a fail-closed sidecar is noticed in scripts.
		os.Exit(1)
	}

	if !st.Present || st.Manifest == nil {
		fmt.Println("status:  no restore point")
		return nil
	}

	m := st.Manifest
	fmt.Printf("status:  present\n")
	fmt.Printf("created: %s (%s)\n", m.CreatedAt, m.CreatedByVersion)
	fmt.Printf("schema:  pre v%d → target v%d (first risky v%d)\n",
		m.PreSchemaVersion, m.TargetSchemaVersion, m.FirstRiskySchemaVersion)
	fmt.Printf("size:    %d bytes\n", m.SnapshotSizeBytes)
	fmt.Printf("boots:   %d / %d (expires after threshold)\n",
		m.SuccessfulBoots, m.ExpiresAfterSuccessfulBoots)
	if m.RestoreCount > 0 {
		fmt.Printf("restored: %d time(s), last %v\n", m.RestoreCount, derefStr(m.LastRestoredAt))
	}
	return nil
}

func runSnapshotRestore(cmd *cobra.Command, args []string) error {
	if !snapshotRestoreConfirm {
		return errors.New("restore is destructive; re-run with --confirm")
	}
	dbPath, err := resolveSnapshotDBPath()
	if err != nil {
		return err
	}
	movedAside, err := store.Restore(dbPath)
	if err != nil {
		if errors.Is(err, store.ErrNoRestorePoint) {
			return errors.New("no restore point to restore from")
		}
		return err
	}
	fmt.Printf("restored %s from the upgrade restore point\n", dbPath)
	fmt.Printf("previous database moved aside to: %s{,-wal,-shm}\n", movedAside)
	return nil
}

func runSnapshotPrune(cmd *cobra.Command, args []string) error {
	if !snapshotPruneConfirm {
		return errors.New("prune removes the restore point; re-run with --confirm")
	}
	dbPath, err := resolveSnapshotDBPath()
	if err != nil {
		return err
	}
	if err := store.Prune(dbPath); err != nil {
		if errors.Is(err, store.ErrNoRestorePoint) {
			return errors.New("no restore point to prune")
		}
		return err
	}
	fmt.Println("restore point pruned")
	return nil
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
