package llm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// ClaudeCLI calls the Claude CLI (`claude -p`) as a subprocess.
type ClaudeCLI struct {
	model   string
	timeout time.Duration
}

// NewClaudeCLI creates a new Claude CLI client.
func NewClaudeCLI(model string) *ClaudeCLI {
	return &ClaudeCLI{
		model:   model,
		timeout: 120 * time.Second,
	}
}

// Complete sends a prompt to the Claude CLI and returns the response.
func (c *ClaudeCLI) Complete(ctx context.Context, prompt string) (*Response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "claude", "-p", "--model", c.model, "--max-turns", "1")
	cmd.Stdin = strings.NewReader(prompt)

	// Strip CLAUDE_* env vars to prevent recursive hook triggering
	cmd.Env = filterEnv(os.Environ())

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("claude cli: %w (stderr: %s)", err, stderr.String())
	}

	return &Response{
		Content:  strings.TrimSpace(stdout.String()),
		Provider: "claude-cli",
	}, nil
}

// filterEnv removes CLAUDE_* environment variables to prevent recursive hooks.
func filterEnv(env []string) []string {
	filtered := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, "CLAUDE_") {
			filtered = append(filtered, e)
		}
	}
	return filtered
}
