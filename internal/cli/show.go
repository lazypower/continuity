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
	showLayer string
	showJSON  bool
)

var showCmd = &cobra.Command{
	Use:     "show <uri>",
	Aliases: []string{"get", "cat"},
	Short:   "Show a single memory's full content",
	Long: `Fetch a memory by URI and print its summary (L0), body (L1), and detail (L2).
Requires a running server (continuity serve).

Use this to read a memory's full body before updating it in place, so
appends don't clobber unseen content.

Examples:
  continuity show mem://user/preferences/devbox
  continuity show mem://user/preferences/devbox --layer body
  continuity show mem://user/preferences/devbox --json

You can also omit the mem:// prefix; it will be added automatically.`,
	Args: cobra.ExactArgs(1),
	RunE: runShow,
}

func init() {
	showCmd.Flags().StringVar(&showLayer, "layer", "all", "Which tier to print: summary, body, detail, or all")
	showCmd.Flags().BoolVar(&showJSON, "json", false, "Emit machine-readable JSON")
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
	data, err := client.Get("/api/memories?" + params.Encode())
	if err != nil {
		return fmt.Errorf("show: %w", err)
	}

	var resp struct {
		URI      string `json:"uri"`
		Category string `json:"category"`
		Summary  string `json:"summary"`
		Body     string `json:"body"`
		Detail   string `json:"detail"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}

	if showJSON {
		// Pretty-print what we got back — but filter by layer if requested.
		out := map[string]any{"uri": resp.URI, "category": resp.Category}
		switch showLayer {
		case "summary":
			out["summary"] = resp.Summary
		case "body":
			out["body"] = resp.Body
		case "detail":
			out["detail"] = resp.Detail
		default:
			out["summary"] = resp.Summary
			out["body"] = resp.Body
			out["detail"] = resp.Detail
		}
		enc, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		fmt.Println(string(enc))
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
		fmt.Printf("%s [%s]\n", resp.URI, resp.Category)
		fmt.Println()
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
