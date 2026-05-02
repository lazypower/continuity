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
	rememberCategory             string
	rememberName                 string
	rememberSummary              string
	rememberBody                 string
	rememberDetail               string
	rememberSession              string
	rememberAcknowledgeRetracted bool
)

var validCategorySet = map[string]bool{
	"profile": true, "preferences": true, "entities": true,
	"events": true, "patterns": true, "cases": true,
	"moments": true,
}

var rememberCmd = &cobra.Command{
	Use:   "remember",
	Short: "Store a memory directly (no LLM needed)",
	Long: `Store a structured memory directly into the memory tree.
Requires a running server (continuity serve).

Example:
  continuity remember -c preferences -n devbox \
    -s "Always use devbox for development tooling" \
    -b "The project uses devbox shell to provide Go, SQLite tools, and other dev dependencies."`,
	RunE: runRemember,
}

func init() {
	rememberCmd.Flags().StringVarP(&rememberCategory, "category", "c", "", "Memory category (required: profile, preferences, entities, events, patterns, cases, moments)")
	rememberCmd.Flags().StringVarP(&rememberName, "name", "n", "", "URI slug name (required)")
	rememberCmd.Flags().StringVarP(&rememberSummary, "summary", "s", "", "L0 abstract — one sentence, max 200 chars (required)")
	rememberCmd.Flags().StringVarP(&rememberBody, "body", "b", "", "L1 overview — max 2000 chars, compress detail aggressively (required)")
	rememberCmd.Flags().StringVarP(&rememberDetail, "detail", "d", "", "L2 full content — max 40000 chars (optional)")
	rememberCmd.Flags().StringVar(&rememberSession, "session", "", "Session ID for provenance (optional)")
	rememberCmd.Flags().BoolVar(&rememberAcknowledgeRetracted, "acknowledge-retracted", false, "Proceed past a dedup match against retracted memory (use after inspecting with `show --include-retracted`)")

	rememberCmd.MarkFlagRequired("category")
	rememberCmd.MarkFlagRequired("name")
	rememberCmd.MarkFlagRequired("summary")
	rememberCmd.MarkFlagRequired("body")
}

func runRemember(cmd *cobra.Command, args []string) error {
	// Validate category locally before hitting the network
	if !validCategorySet[rememberCategory] {
		valid := make([]string, 0, len(validCategorySet))
		for k := range validCategorySet {
			valid = append(valid, k)
		}
		return fmt.Errorf("invalid category %q (valid: %s)", rememberCategory, strings.Join(valid, ", "))
	}

	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	payload := map[string]any{
		"category": rememberCategory,
		"name":     rememberName,
		"summary":  rememberSummary,
		"body":     rememberBody,
	}
	if rememberDetail != "" {
		payload["detail"] = rememberDetail
	}
	if rememberSession != "" {
		payload["session_id"] = rememberSession
	}
	if rememberAcknowledgeRetracted {
		payload["acknowledge_retracted"] = true
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	data, err := client.Post("/api/memories", body)
	// data may carry a structured response even on non-2xx (e.g. 409 matches_retracted).
	var resp struct {
		Status       string   `json:"status"`
		URI          string   `json:"uri"`
		MatchedURIs  []string `json:"matched_uris"`
		Hint         string   `json:"hint"`
		Error        string   `json:"error"`
	}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &resp)
	}

	if resp.Status == "matches_retracted" {
		fmt.Fprintln(os.Stderr, "matches_retracted: candidate write matches retracted memory")
		for _, u := range resp.MatchedURIs {
			fmt.Fprintf(os.Stderr, "  - %s\n", u)
		}
		if resp.Hint != "" {
			fmt.Fprintln(os.Stderr, resp.Hint)
		}
		os.Exit(2)
	}

	if err != nil {
		return fmt.Errorf("remember: %w", err)
	}

	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		os.Exit(1)
	}

	fmt.Printf("%s: %s [%s]\n", resp.Status, resp.URI, rememberCategory)
	return nil
}
