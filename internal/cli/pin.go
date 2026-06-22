package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

var pinCmd = &cobra.Command{
	Use:   "pin [uri]",
	Short: "Pin a memory to the cold-boot injection window (declared contract)",
	Long: `Pin a memory you want injected into every session's "Pinned" section, regardless
of recency or relevance. Pins are the declared half of the operating contract: the
things you want the agent to wake up knowing.

A pinned memory rides every cold SessionStart until you unpin it. Retraction wins:
a pinned memory that is later retracted goes silent — it is never injected.

With no argument, lists the current pins (what the agent wakes up with).

Examples:
  continuity pin                                       # list current pins
  continuity pin mem://user/feedback/codex-before-pr   # pin a memory
  continuity unpin mem://user/feedback/codex-before-pr # remove a pin`,
	Args: cobra.MaximumNArgs(1),
	RunE: runPin,
}

var unpinCmd = &cobra.Command{
	Use:   "unpin <uri>",
	Short: "Remove an operator pin",
	Long:  `Clear a pin so the memory no longer rides the cold-boot injection window.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runUnpin,
}

func runPin(cmd *cobra.Command, args []string) error {
	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	// No argument → list current pins.
	if len(args) == 0 {
		return listPins(client)
	}

	uri := strings.TrimSpace(args[0])
	if !strings.HasPrefix(uri, "mem://") {
		return fmt.Errorf("invalid URI %q: must start with mem://", uri)
	}

	warnIfSkewed()

	body, _ := json.Marshal(map[string]string{"uri": uri})
	data, err := client.Post("/api/memories/pin", body)
	if err != nil {
		return fmt.Errorf("pin: %w", err)
	}

	var resp struct {
		Status string `json:"status"`
		URI    string `json:"uri"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("%s: %s\n", resp.Status, resp.URI)
	return nil
}

func runUnpin(cmd *cobra.Command, args []string) error {
	uri := strings.TrimSpace(args[0])
	if !strings.HasPrefix(uri, "mem://") {
		return fmt.Errorf("invalid URI %q: must start with mem://", uri)
	}

	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	warnIfSkewed()

	body, _ := json.Marshal(map[string]string{"uri": uri})
	data, err := client.Post("/api/memories/unpin", body)
	if err != nil {
		return fmt.Errorf("unpin: %w", err)
	}

	var resp struct {
		Status string `json:"status"`
		URI    string `json:"uri"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Printf("%s: %s\n", resp.Status, resp.URI)
	return nil
}

func listPins(client *hooks.Client) error {
	data, err := client.Get("/api/memories/pinned")
	if err != nil {
		return fmt.Errorf("list pins: %w", err)
	}

	var resp struct {
		Count int `json:"count"`
		Pins  []struct {
			URI        string `json:"uri"`
			Category   string `json:"category"`
			L0Abstract string `json:"l0_abstract"`
		} `json:"pins"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		os.Exit(1)
	}

	if resp.Count == 0 {
		fmt.Println("No pinned memories. Pin one with: continuity pin <uri>")
		return nil
	}

	fmt.Printf("Pinned memories (%d) — injected into every cold SessionStart:\n\n", resp.Count)
	for _, p := range resp.Pins {
		fmt.Printf("  📌 [%s] %s\n     %s\n", p.Category, p.URI, p.L0Abstract)
	}
	return nil
}
