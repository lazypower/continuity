package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/lazypower/continuity/internal/store"
)

// snapshotCmd is the parent for `continuity snapshot list / prune`. There is
// NO `restore` subcommand by design — restoration is a manual `cp` and the
// operator must own that decision. The CLI surface exists to make snapshots
// visible and disposable, not to manage them.
var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Inspect or remove migration safety snapshots",
	Long: `Migration safety snapshots are atomic copies of the database taken just
before a risky schema migration runs (e.g., a full-table rebuild). They
exist as a one-shot safety net during the upgrade window — NOT as a
backup system.

Snapshots auto-delete after a small number of successful serve boots;
this command lets you inspect what's currently retained or prune it
explicitly. To restore from a snapshot, stop the server and copy the
snapshot file over the live database manually.`,
}

var snapshotListCmd = &cobra.Command{
	Use:   "list",
	Short: "List retained migration safety snapshots",
	RunE:  runSnapshotList,
}

var snapshotPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Remove all retained migration safety snapshots",
	Long: `Removes every retained migration safety snapshot, deleting the snapshot
files and clearing the tracking table. This is destructive — once pruned,
there is no automated way to roll back the most recent risky migration.`,
	RunE: runSnapshotPrune,
}

func init() {
	snapshotCmd.AddCommand(snapshotListCmd)
	snapshotCmd.AddCommand(snapshotPruneCmd)
}

// openDBForSnapshot opens the configured database (honoring CONTINUITY_DB, like
// openDB) but WITHOUT running migrations. Inspecting or pruning snapshots must
// not trigger a schema upgrade — see store.OpenNoMigrate for why.
func openDBForSnapshot() (*store.DB, error) {
	dbPath := os.Getenv("CONTINUITY_DB")
	if dbPath == "" {
		var err error
		dbPath, err = store.DefaultDBPath()
		if err != nil {
			return nil, err
		}
	}
	return store.OpenNoMigrate(dbPath)
}

func runSnapshotList(cmd *cobra.Command, args []string) error {
	db, err := openDBForSnapshot()
	if err != nil {
		return err
	}
	defer db.Close()

	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		fmt.Println("no migration safety snapshots retained")
		return nil
	}
	fmt.Printf("%d migration safety snapshot(s) retained:\n", len(snaps))
	for _, s := range snaps {
		fmt.Printf("\n  %s\n", s.Path)
		fmt.Printf("    pre-version:    %d\n", s.PreVersion)
		fmt.Printf("    target-version: %d\n", s.TargetVersion)
		fmt.Printf("    created:        %s\n", s.CreatedAt.Format("2006-01-02 15:04:05 MST"))
		fmt.Printf("    boots since:    %d (auto-deletes after %d)\n",
			s.BootsSince, store.SnapshotRetentionBoots)
	}
	fmt.Println()
	fmt.Println("To restore from a snapshot, stop the server and:")
	// Print the DB actually opened, not a hardcoded default — with
	// CONTINUITY_DB set, the default path would point the operator at the
	// wrong file and risk overwriting an unrelated database.
	fmt.Printf("  cp <snapshot-path> %s\n", db.Path)
	return nil
}

func runSnapshotPrune(cmd *cobra.Command, args []string) error {
	db, err := openDBForSnapshot()
	if err != nil {
		return err
	}
	defer db.Close()

	n, err := db.PruneMigrationSnapshots()
	if err != nil {
		return err
	}
	if n == 0 {
		fmt.Println("no snapshots to prune")
		return nil
	}
	fmt.Fprintf(os.Stdout, "removed %d migration safety snapshot(s)\n", n)
	return nil
}
