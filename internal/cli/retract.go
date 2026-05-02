package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

var (
	retractURI          string
	retractReason       string
	retractSupersededBy string
)

var retractCmd = &cobra.Command{
	Use:   "retract <uri>",
	Short: "Retract a memory (tombstone or supersession)",
	Long: `Retract a memory you wrote. Memory is preserved as a marker but excluded from
default reads — search, tree, context injection. Use --include-retracted on inspection
commands to see retracted memories.

A reason is required, kept short (one sentence). With --superseded-by, the retraction
becomes a supersession: the new memory is reachable normally and the old is linked to it,
preserving the trail of how understanding evolved.

Examples:
  continuity retract mem://user/events/test-foo \
    --reason "test repro, no ongoing value"

  continuity retract mem://user/preferences/old-style \
    --reason "preference changed after 2026-04 review" \
    --superseded-by mem://user/preferences/new-style`,
	Args: cobra.ExactArgs(1),
	RunE: runRetract,
}

func init() {
	retractCmd.Flags().StringVarP(&retractReason, "reason", "r", "", "Why this memory is being retracted (required, one sentence)")
	retractCmd.Flags().StringVar(&retractSupersededBy, "superseded-by", "", "URI of the memory that supersedes this one (optional)")
	retractCmd.MarkFlagRequired("reason")
}

func runRetract(cmd *cobra.Command, args []string) error {
	retractURI = strings.TrimSpace(args[0])
	if !strings.HasPrefix(retractURI, "mem://") {
		return fmt.Errorf("invalid URI %q: must start with mem://", retractURI)
	}
	if retractSupersededBy != "" && !strings.HasPrefix(retractSupersededBy, "mem://") {
		return fmt.Errorf("invalid superseded-by URI %q: must start with mem://", retractSupersededBy)
	}

	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	payload := map[string]string{
		"uri":    retractURI,
		"reason": retractReason,
	}
	if retractSupersededBy != "" {
		payload["superseded_by"] = retractSupersededBy
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	data, err := client.Post("/api/memories/retract", body)
	if err != nil {
		return fmt.Errorf("retract: %w", err)
	}

	var resp struct {
		Status       string `json:"status"`
		URI          string `json:"uri"`
		SupersededBy string `json:"superseded_by"`
		Error        string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		os.Exit(1)
	}

	if resp.SupersededBy != "" {
		fmt.Printf("%s: %s → %s\n", resp.Status, resp.URI, resp.SupersededBy)
	} else {
		fmt.Printf("%s: %s\n", resp.Status, resp.URI)
	}
	return nil
}
