package hooks

import (
	"encoding/json"
	"fmt"
	"strings"
)

// promptGate runs the prompt-gate round-trip for one user prompt
// (ADR-001 §4, #80). Contract, not hope: the gate call is its own request
// with its own short budget (gateTimeout), and EVERY failure — non-200,
// timeout, transport error, malformed body — resolves to silence. This
// function never errors, never writes stderr, never exits: the user's prompt
// passes through unmodified and the hook exits 0 exactly as if the gate did
// not exist. It runs only after session init succeeded; a failed init keeps
// its own non-blocking error path and the gate simply never fires.
func promptGate(client *Client, input *HookInput, project string) {
	if input.Prompt == "" {
		return
	}

	body, err := json.Marshal(map[string]string{
		"session_id": input.SessionID,
		"project":    project,
		"prompt":     input.Prompt,
	})
	if err != nil {
		return
	}

	data, err := client.Post("/api/gate", body)
	if err != nil {
		return // silence: the gate may never block, delay, or fail the prompt
	}

	var resp struct {
		Mode   string `json:"mode"`
		Inject []struct {
			URI        string `json:"uri"`
			L0Abstract string `json:"l0_abstract"`
		} `json:"inject"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return // malformed body → silence
	}

	// "Shadow never injects" is enforced on BOTH sides of the wire: the server
	// returns an empty inject list in shadow mode, and the hook independently
	// refuses inject items unless the response says mode "on" — so a buggy or
	// version-skewed server cannot turn shadow into injection (Codex round 1).
	if resp.Mode != "on" {
		return
	}

	// Every prompt under τ returns an empty inject list. Median prompt injects
	// zero — silence is the default, and no output at all is how
	// UserPromptSubmit says "unchanged".
	if len(resp.Inject) == 0 {
		return
	}

	// L0 + mem:// URI only — pointers, never payloads. The agent deepens by
	// fetching the URI, which is the use event ADR-001 §2 counts.
	var b strings.Builder
	b.WriteString("<continuity-recall>\nMemory relevant to this prompt (fetch the URI for detail):\n")
	wrote := false
	for _, h := range resp.Inject {
		if h.URI == "" {
			continue
		}
		fmt.Fprintf(&b, "- %s (%s)\n", h.L0Abstract, h.URI)
		wrote = true
	}
	b.WriteString("</continuity-recall>")
	if !wrote {
		return
	}
	WriteUserPromptSubmitOutput(b.String())
}
