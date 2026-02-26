package cli

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/lazypower/continuity/internal/store"
	"github.com/spf13/cobra"
)

// stubRun returns a RunE that prints a not-yet-implemented message to stderr
// and exits 0 (hooks must never crash Claude Code).
func stubRun(name string) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stderr, "%s: not yet implemented\n", name)
	}
}

var hookCmd = &cobra.Command{
	Use:   "hook",
	Short: "Handle Claude Code hook events",
}

var hookStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Handle SessionStart hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("start", os.Stdin)
	},
}

var hookSubmitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Handle UserPromptSubmit hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("submit", os.Stdin)
	},
}

var hookToolCmd = &cobra.Command{
	Use:   "tool",
	Short: "Handle PostToolUse hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("tool", os.Stdin)
	},
}

var hookStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Handle Stop hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("stop", os.Stdin)
	},
}

var hookEndCmd = &cobra.Command{
	Use:   "end",
	Short: "Handle SessionEnd hook",
	Run: func(cmd *cobra.Command, args []string) {
		hooks.Handle("end", os.Stdin)
	},
}

func init() {
	hookCmd.AddCommand(hookStartCmd)
	hookCmd.AddCommand(hookSubmitCmd)
	hookCmd.AddCommand(hookToolCmd)
	hookCmd.AddCommand(hookStopCmd)
	hookCmd.AddCommand(hookEndCmd)

	// Search flags
	searchCmd.Flags().BoolVar(&searchSmart, "smart", false, "Use LLM-assisted search")
	searchCmd.Flags().IntVarP(&searchLimit, "limit", "n", 10, "Maximum number of results")
	searchCmd.Flags().StringVarP(&searchCategory, "category", "c", "", "Filter by category")

	// Profile flags
	profileCmd.Flags().BoolVar(&profileVerbose, "verbose", false, "Show all profile and preference nodes")
}

// openDB is a helper that opens the database for CLI commands.
func openDB() (*store.DB, error) {
	dbPath := os.Getenv("CONTINUITY_DB")
	if dbPath == "" {
		var err error
		dbPath, err = store.DefaultDBPath()
		if err != nil {
			return nil, err
		}
	}
	return store.Open(dbPath)
}

// --- search command ---

var (
	searchSmart    bool
	searchLimit    int
	searchCategory string
)

var searchCmd = &cobra.Command{
	Use:   "search [query]",
	Short: "Search memories",
	Long:  "Search the memory tree via the continuity server. Use --smart for LLM-assisted search. Requires a running server (continuity serve).",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runSearch,
}

func runSearch(cmd *cobra.Command, args []string) error {
	query := strings.Join(args, " ")

	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	// Build query params
	params := url.Values{}
	params.Set("q", query)
	params.Set("limit", strconv.Itoa(searchLimit))
	if searchCategory != "" {
		params.Set("category", searchCategory)
	}
	if searchSmart {
		params.Set("mode", "search")
	}

	data, err := client.Get("/api/search?" + params.Encode())
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}

	var resp struct {
		Query   string `json:"query"`
		Mode    string `json:"mode"`
		Count   int    `json:"count"`
		Results []struct {
			URI        string  `json:"uri"`
			Category   string  `json:"category"`
			L0Abstract string  `json:"l0_abstract"`
			L1Overview string  `json:"l1_overview"`
			Score      float64 `json:"score"`
			Similarity float64 `json:"similarity"`
			Relevance  float64 `json:"relevance"`
		} `json:"results"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	if resp.Count == 0 {
		fmt.Println("No results found.")
		return nil
	}

	for i, r := range resp.Results {
		fmt.Printf("%d. [%.3f] %s\n", i+1, r.Score, r.URI)
		fmt.Printf("   %s [%s]\n", r.L0Abstract, r.Category)
		if r.L1Overview != "" {
			// Show first 200 chars of L1
			overview := r.L1Overview
			if len(overview) > 200 {
				overview = overview[:200] + "..."
			}
			fmt.Printf("   %s\n", overview)
		}
		fmt.Println()
	}

	return nil
}

// --- profile command ---

var profileVerbose bool

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Show relational profile",
	RunE:  runProfile,
}

func runProfile(cmd *cobra.Command, args []string) error {
	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	// Show relational profile
	relProfile, err := db.GetNodeByURI("mem://user/profile/communication")
	if err != nil {
		return fmt.Errorf("get profile: %w", err)
	}

	if relProfile != nil && relProfile.L1Overview != "" {
		fmt.Println("## Relational Profile")
		fmt.Println()
		fmt.Println(relProfile.L1Overview)
		fmt.Println()
	} else {
		fmt.Println("No relational profile found. Run some sessions first.")
	}

	if profileVerbose {
		// Show all profile nodes
		profiles, _ := db.FindByCategory("profile")
		if len(profiles) > 0 {
			fmt.Println("## Profile Nodes")
			for _, n := range profiles {
				if n.URI == "mem://user/profile/communication" {
					continue
				}
				fmt.Printf("- %s: %s\n", n.URI, n.L0Abstract)
			}
			fmt.Println()
		}

		// Show all preference nodes
		prefs, _ := db.FindByCategory("preferences")
		if len(prefs) > 0 {
			fmt.Println("## Preferences")
			for _, n := range prefs {
				fmt.Printf("- %s: %s\n", n.URI, n.L0Abstract)
			}
			fmt.Println()
		}
	}

	return nil
}

// --- tree command ---

var treeCmd = &cobra.Command{
	Use:   "tree [uri]",
	Short: "Browse memory tree",
	Long:  "List memory tree nodes. With no argument, shows root dirs. With a URI, shows children.",
	RunE:  runTree,
}

func runTree(cmd *cobra.Command, args []string) error {
	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	if len(args) > 0 {
		// Show children of the given URI
		uri := args[0]
		children, err := db.GetChildren(uri)
		if err != nil {
			return fmt.Errorf("get children: %w", err)
		}
		if len(children) == 0 {
			fmt.Printf("No children found for %s\n", uri)
			return nil
		}
		fmt.Printf("## %s\n\n", uri)
		for _, c := range children {
			suffix := ""
			if c.NodeType == "dir" {
				count, _ := db.CountChildren(c.URI)
				suffix = fmt.Sprintf(" (%d children)", count)
			}
			if c.L0Abstract != "" {
				fmt.Printf("  %s %s%s\n    %s\n", c.NodeType, c.URI, suffix, c.L0Abstract)
			} else {
				fmt.Printf("  %s %s%s\n", c.NodeType, c.URI, suffix)
			}
		}
		return nil
	}

	// Show roots with child counts
	roots, err := db.ListRoots()
	if err != nil {
		return fmt.Errorf("list roots: %w", err)
	}

	if len(roots) == 0 {
		fmt.Println("Memory tree is empty. Run some sessions first.")
		return nil
	}

	fmt.Println("## Memory Tree")
	fmt.Println()
	for _, r := range roots {
		count, _ := db.CountChildren(r.URI)
		fmt.Printf("  %s (%d children)\n", r.URI, count)
	}

	return nil
}

// --- import command (still a stub) ---

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import from claude-mem database",
	Run:   stubRun("import"),
}
