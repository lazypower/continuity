package llm

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

// maxLLMResponse caps how much output any LLM backend may return, so a runaway
// or malformed response cannot OOM the server (M7 in the audit). 10 MiB is far
// above any real extraction or merge response.
const maxLLMResponse = 10 << 20

// cappedWriter buffers up to maxLLMResponse bytes and drops the rest. It always
// reports a full write so the child process is never blocked or errored by the
// cap; overflow is discarded and flagged.
type cappedWriter struct {
	buf     bytes.Buffer
	dropped bool
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	if room := maxLLMResponse - w.buf.Len(); room > 0 {
		if len(p) > room {
			w.buf.Write(p[:room])
			w.dropped = true
		} else {
			w.buf.Write(p)
		}
	} else if len(p) > 0 {
		w.dropped = true
	}
	return len(p), nil
}

func (w *cappedWriter) String() string { return w.buf.String() }

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

	stdout := &cappedWriter{}
	var stderr bytes.Buffer
	cmd.Stdout = stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("claude cli: %w (stderr: %s)", err, stderr.String())
	}
	if stdout.dropped {
		log.Printf("claude cli: response exceeded %d bytes; truncated", maxLLMResponse)
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
