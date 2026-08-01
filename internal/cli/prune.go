package cli

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

var (
	pruneDryRun     bool
	pruneSkipVacuum bool
)

var pruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Reclaim spent observations and compact the database",
	Long: `Reclaim disk space held by spent observations.

Observations are the raw tool-use records captured during a session. They exist
to serve that session's live context header, so once a session is no longer in
flight they are spent and can be dropped. Memories are never touched.

By default this also runs VACUUM, which is what actually returns freed pages to
the filesystem — pruning alone leaves the file the same size. VACUUM needs free
disk space roughly equal to the current database size, and can take a while on
a large file.

Examples:
  continuity prune --dry-run    # report what would be reclaimed
  continuity prune              # prune and compact
  continuity prune --skip-vacuum`,
	Args: cobra.NoArgs,
	RunE: runPrune,
}

func init() {
	pruneCmd.Flags().BoolVar(&pruneDryRun, "dry-run", false, "Report what would be reclaimed without deleting")
	pruneCmd.Flags().BoolVar(&pruneSkipVacuum, "skip-vacuum", false, "Prune without compacting the database file")
}

type pruneResponse struct {
	Status      string `json:"status"`
	DryRun      bool   `json:"dry_run"`
	Reclaimable int64  `json:"reclaimable"`
	Pruned      int64  `json:"pruned"`
	Vacuumed    bool   `json:"vacuumed"`
	BytesBefore int64  `json:"bytes_before"`
	BytesAfter  int64  `json:"bytes_after"`
}

func runPrune(cmd *cobra.Command, args []string) error {
	client := hooks.NewCLIClient()
	if err := client.CheckHealth(); err != nil {
		return err
	}

	params := url.Values{}
	if pruneDryRun {
		params.Set("dry_run", "true")
	}
	if pruneSkipVacuum {
		params.Set("vacuum", "false")
	}

	if !pruneDryRun {
		fmt.Println("Pruning… (VACUUM on a large database can take a minute)")
	}

	data, err := client.Post("/api/prune?"+params.Encode(), nil)
	if err != nil {
		return client.DescribeError(err)
	}

	var resp pruneResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("decode prune response: %w", err)
	}

	if resp.DryRun {
		fmt.Printf("Reclaimable: %d observation(s)\n", resp.Reclaimable)
		fmt.Printf("Database:    %s\n", humanBytes(resp.BytesBefore))
		if resp.Reclaimable == 0 {
			fmt.Println("Nothing to prune.")
		} else {
			fmt.Println("\nRun `continuity prune` to reclaim.")
		}
		return nil
	}

	fmt.Printf("Pruned:   %d observation(s)\n", resp.Pruned)
	fmt.Printf("Database: %s → %s", humanBytes(resp.BytesBefore), humanBytes(resp.BytesAfter))
	if freed := resp.BytesBefore - resp.BytesAfter; freed > 0 {
		fmt.Printf(" (freed %s)", humanBytes(freed))
	}
	fmt.Println()

	if !resp.Vacuumed && !pruneSkipVacuum {
		fmt.Println("\nNote: VACUUM did not complete — rows were deleted but the file was not " +
			"compacted. This usually means insufficient free disk space. See the server log.")
	}
	if !resp.Vacuumed && pruneSkipVacuum {
		fmt.Println("\nSkipped VACUUM: freed pages are reusable but the file size is unchanged.")
	}
	return nil
}

// humanBytes renders a byte count for a terminal reader.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
