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
func promptGate(client *Client, input *HookInput) {
	if input.Prompt == "" {
		return
	}

	body, err := json.Marshal(map[string]string{
		"session_id": input.SessionID,
		"project":    projectIdentity(input.CWD),
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
		Inject []struct {
			URI        string `json:"uri"`
			L0Abstract string `json:"l0_abstract"`
		} `json:"inject"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return // malformed body → silence
	}

	// Shadow mode (the default) always returns an empty inject list; so does
	// every prompt under τ. Median prompt injects zero — silence is the
	// default, and no output at all is how UserPromptSubmit says "unchanged".
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
