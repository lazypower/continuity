package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

var (
	extractForce        bool
	extractTranscript   string
	extractBackfillEmpty bool
)

var extractCmd = &cobra.Command{
	Use:   "extract [session-id]",
	Short: "Re-run extraction for a session",
	Long: `Trigger memory extraction for a completed session.

Typical uses:
  continuity extract <session-id>              — re-extract if not already done
  continuity extract <session-id> --force      — re-extract even if marked done
  continuity extract --backfill-empty          — unmark every session that was
                                                 flagged as extracted but has
                                                 no memories attributed to it

When a session-id is given, continuity auto-discovers the transcript at
~/.claude/projects/*/<session-id>.jsonl. Pass --transcript to override.

Requires a running server (continuity serve).`,
	Args: cobra.MaximumNArgs(1),
	RunE: runExtract,
}

func init() {
	extractCmd.Flags().BoolVar(&extractForce, "force", false, "Bypass the idempotency guard (re-extract already-extracted sessions)")
	extractCmd.Flags().StringVar(&extractTranscript, "transcript", "", "Path to transcript JSONL (overrides auto-discovery)")
	extractCmd.Flags().BoolVar(&extractBackfillEmpty, "backfill-empty", false, "Unmark every session marked extracted with zero attributed memories")
}

func runExtract(cmd *cobra.Command, args []string) error {
	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	if extractBackfillEmpty {
		if len(args) > 0 || extractForce || extractTranscript != "" {
			return fmt.Errorf("--backfill-empty cannot be combined with a session-id, --force, or --transcript")
		}
		return runBackfillEmpty(client)
	}

	if len(args) != 1 {
		return fmt.Errorf("session-id is required (or use --backfill-empty)")
	}
	sessionID := strings.TrimSpace(args[0])
	if sessionID == "" {
		return fmt.Errorf("session-id is required")
	}

	transcriptPath := extractTranscript
	if transcriptPath == "" {
		found, err := findTranscript(sessionID)
		if err != nil {
			return err
		}
		transcriptPath = found
	}
	if _, err := os.Stat(transcriptPath); err != nil {
		return fmt.Errorf("transcript not readable: %w", err)
	}

	body, _ := json.Marshal(map[string]any{
		"transcript_path": transcriptPath,
		"force":           extractForce,
	})

	data, err := client.Post("/api/sessions/"+sessionID+"/extract", body)
	if err != nil {
		if len(data) > 0 {
			var resp struct {
				Error string `json:"error"`
			}
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil && resp.Error != "" {
				return fmt.Errorf("%s", resp.Error)
			}
		}
		return fmt.Errorf("extract: %w", err)
	}

	fmt.Printf("extraction queued for %s (transcript: %s, force: %v)\n", sessionID, transcriptPath, extractForce)
	fmt.Println("check serve.log for progress — extraction runs asynchronously")
	return nil
}

func runBackfillEmpty(client *hooks.Client) error {
	data, err := client.Post("/api/sessions/unmark-empty-extractions", nil)
	if err != nil {
		if len(data) > 0 {
			var resp struct {
				Error string `json:"error"`
			}
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil && resp.Error != "" {
				return fmt.Errorf("%s", resp.Error)
			}
		}
		return fmt.Errorf("backfill: %w", err)
	}

	var resp struct {
		Status   string `json:"status"`
		Unmarked int64  `json:"unmarked"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}

	fmt.Printf("unmarked %d session(s) that were extracted with no attributed memories\n", resp.Unmarked)
	if resp.Unmarked > 0 {
		fmt.Println("they will be re-extracted on their next Stop/SessionEnd hook,")
		fmt.Println("or force one now with: continuity extract <session-id> --force")
	}
	return nil
}

// findTranscript searches ~/.claude/projects/*/<session-id>.jsonl for a
// Claude Code transcript matching the given session id. The sessionID is
// validated first — path separators or ".." would let a glob pattern escape
// ~/.claude/projects, which is surprising for "auto-discovery". Callers who
// genuinely need to point at a transcript outside that tree should pass
// --transcript explicitly.
func findTranscript(sessionID string) (string, error) {
	if err := validateSessionIDForGlob(sessionID); err != nil {
		return "", fmt.Errorf("%w — pass --transcript to point at a specific file", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	pattern := filepath.Join(home, ".claude", "projects", "*", sessionID+".jsonl")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("glob transcripts: %w", err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no transcript found for session %s (looked in %s)", sessionID, pattern)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("multiple transcripts found for %s — pass --transcript to disambiguate:\n  %s", sessionID, strings.Join(matches, "\n  "))
	}
	return matches[0], nil
}

// validateSessionIDForGlob rejects session IDs that would let the
// auto-discovery glob escape ~/.claude/projects. Real Claude Code session
// IDs are UUIDs, but continuity imports from other sources so we don't
// require that — we just refuse anything that would traverse the filesystem.
func validateSessionIDForGlob(sessionID string) error {
	if sessionID == "" {
		return fmt.Errorf("session-id is empty")
	}
	if strings.ContainsAny(sessionID, `/\`) {
		return fmt.Errorf("session-id %q contains a path separator", sessionID)
	}
	if sessionID == "." || sessionID == ".." || strings.Contains(sessionID, "..") {
		return fmt.Errorf("session-id %q contains path traversal", sessionID)
	}
	return nil
}
