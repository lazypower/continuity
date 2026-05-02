package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

var (
	showLayer            string
	showJSON             bool
	showIncludeRetracted bool
)

var showCmd = &cobra.Command{
	Use:     "show <uri>",
	Aliases: []string{"get", "cat"},
	Short:   "Show a single memory's full content",
	Long: `Fetch a memory by URI and print its summary (L0), body (L1), and detail (L2).
Requires a running server (continuity serve).

Use this to read a memory's full body before updating it in place, so
appends don't clobber unseen content.

For retracted memories, only metadata (URI, retraction timestamp, supersession
link if any) is shown by default. Pass --include-retracted to see the reason
text and the original content.

Examples:
  continuity show mem://user/preferences/devbox
  continuity show mem://user/preferences/devbox --layer body
  continuity show mem://user/preferences/devbox --json
  continuity show mem://user/events/old-fact --include-retracted

You can also omit the mem:// prefix; it will be added automatically.`,
	Args: cobra.ExactArgs(1),
	RunE: runShow,
}

func init() {
	showCmd.Flags().StringVar(&showLayer, "layer", "all", "Which tier to print: summary, body, detail, or all")
	showCmd.Flags().BoolVar(&showJSON, "json", false, "Emit machine-readable JSON")
	showCmd.Flags().BoolVar(&showIncludeRetracted, "include-retracted", false, "Reveal the reason and original content of a retracted memory")
}

func runShow(cmd *cobra.Command, args []string) error {
	uri := strings.TrimSpace(args[0])
	if uri == "" {
		return fmt.Errorf("uri is required")
	}
	if !strings.HasPrefix(uri, "mem://") {
		uri = "mem://" + strings.TrimPrefix(uri, "/")
	}

	switch showLayer {
	case "all", "summary", "body", "detail":
	default:
		return fmt.Errorf("invalid --layer %q (valid: all, summary, body, detail)", showLayer)
	}

	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	params := url.Values{}
	params.Set("uri", uri)
	if showIncludeRetracted {
		params.Set("include_retracted", "true")
	}
	data, getErr := client.Get("/api/memories?" + params.Encode())

	// hooks.Client.Get returns (body, err) for non-2xx responses so callers can
	// inspect the JSON error.
	var resp struct {
		URI             string `json:"uri"`
		Category        string `json:"category"`
		Summary         string `json:"summary"`
		Body            string `json:"body"`
		Detail          string `json:"detail"`
		Retracted       bool   `json:"retracted"`
		TombstonedAt    int64  `json:"tombstoned_at"`
		TombstoneReason string `json:"tombstone_reason"`
		SupersededBy    string `json:"superseded_by"`
		Error           string `json:"error"`
	}

	if getErr != nil {
		// Error path: best-effort parse for a clean server-side message, fall
		// back to the raw transport error if the body isn't structured JSON.
		if len(data) > 0 {
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil && resp.Error != "" {
				return fmt.Errorf("%s", resp.Error)
			}
		}
		return fmt.Errorf("show: %w", getErr)
	}

	// Success path: a bad decode means the server returned something
	// unexpected (HTML from a proxy, a partial response, etc.) — fail fast
	// rather than printing empty fields.
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}

	if showJSON {
		// Echo the server response shape unchanged — the server already enforces
		// the retracted-without-flag absence-not-empty contract by omitting fields.
		// Reparse and re-emit with indentation; this preserves field absence (not
		// substituting empty strings for missing fields).
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parse json: %w", err)
		}
		// Layer filter still applies for the content-bearing fields.
		if showLayer != "all" {
			filtered := map[string]any{"uri": raw["uri"], "category": raw["category"]}
			if v, ok := raw["retracted"]; ok {
				filtered["retracted"] = v
			}
			if v, ok := raw["tombstoned_at"]; ok {
				filtered["tombstoned_at"] = v
			}
			if v, ok := raw["tombstone_reason"]; ok {
				filtered["tombstone_reason"] = v
			}
			if v, ok := raw["superseded_by"]; ok {
				filtered["superseded_by"] = v
			}
			switch showLayer {
			case "summary":
				if v, ok := raw["summary"]; ok {
					filtered["summary"] = v
				}
			case "body":
				if v, ok := raw["body"]; ok {
					filtered["body"] = v
				}
			case "detail":
				if v, ok := raw["detail"]; ok {
					filtered["detail"] = v
				}
			}
			raw = filtered
		}
		enc, err := json.MarshalIndent(raw, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		fmt.Println(string(enc))
		return nil
	}

	// Retracted-without-flag: text output is metadata only.
	if resp.Retracted && !showIncludeRetracted {
		fmt.Printf("%s [%s] [retracted]\n", resp.URI, resp.Category)
		if resp.SupersededBy != "" {
			fmt.Printf("  superseded by: %s\n", resp.SupersededBy)
		}
		fmt.Fprintln(cmd.ErrOrStderr(), "(reason and original content hidden — pass --include-retracted to reveal)")
		return nil
	}

	// Text output
	switch showLayer {
	case "summary":
		fmt.Println(resp.Summary)
	case "body":
		fmt.Println(resp.Body)
	case "detail":
		if resp.Detail == "" {
			fmt.Fprintln(cmd.ErrOrStderr(), "(no detail tier stored)")
			return nil
		}
		fmt.Println(resp.Detail)
	default:
		header := fmt.Sprintf("%s [%s]", resp.URI, resp.Category)
		if resp.Retracted {
			header += " [retracted]"
		}
		fmt.Println(header)
		fmt.Println()
		if resp.Retracted {
			fmt.Println("## Retraction")
			fmt.Printf("Reason: %s\n", resp.TombstoneReason)
			if resp.SupersededBy != "" {
				fmt.Printf("Superseded by: %s\n", resp.SupersededBy)
			}
			fmt.Println()
		}
		fmt.Println("## Summary")
		fmt.Println(resp.Summary)
		fmt.Println()
		fmt.Println("## Body")
		if resp.Body == "" {
			fmt.Println("(empty)")
		} else {
			fmt.Println(resp.Body)
		}
		if resp.Detail != "" {
			fmt.Println()
			fmt.Println("## Detail")
			fmt.Println(resp.Detail)
		}
	}

	return nil
}
