package hooks

import (
	"encoding/json"
	"strings"
)

// internalSentinel is the prefix added to all Continuity extraction prompts.
// When claude -p is used for LLM calls, it spawns a new Claude Code session
// that fires hooks — including UserPromptSubmit. This sentinel lets the hook
// handler recognize and skip prompts that originated from Continuity itself,
// preventing recursive signal amplification.
//
// Must match llm.InternalSentinel exactly.
const internalSentinel = "[continuity-internal]"

// signalTriggers are phrases that indicate the user wants something remembered immediately.
var signalTriggers = []string{
	"remember this", "don't forget",
	"always use", "never use", "always do", "never do",
	"architecture decision", "we decided",
	"this pattern", "the trick is",
	"bug was", "root cause", "the fix was",
}

// isInternalPrompt returns true if the prompt is a Continuity extraction prompt,
// not a real user message. Checks for sentinel prefix only — the sentinel must
// be at the start of the prompt to prevent false matches on user messages that
// happen to contain the string.
func isInternalPrompt(prompt string) bool {
	return strings.HasPrefix(prompt, internalSentinel)
}

// hasSignal returns true if the prompt contains any signal trigger phrase.
func hasSignal(prompt string) bool {
	lower := strings.ToLower(prompt)
	for _, trigger := range signalTriggers {
		if strings.Contains(lower, trigger) {
			return true
		}
	}
	return false
}

func handleSubmit(client *Client, input *HookInput) {
	// Guard: skip prompts from Continuity's own LLM calls to prevent recursion.
	// When the server calls claude -p for extraction, that spawns a new session
	// whose hooks fire back into us. The sentinel prefix lets us bail early.
	if isInternalPrompt(input.Prompt) {
		return
	}

	// Initialize/resume session on first user prompt
	body, err := json.Marshal(map[string]string{
		"session_id": input.SessionID,
		"project":    input.CWD,
	})
	if err != nil {
		ExitError(err)
		return
	}

	if _, err := client.Post("/api/sessions/init", body); err != nil {
		ExitError(err)
		return
	}

	// Check for signal keywords — fire and forget
	if input.Prompt != "" && hasSignal(input.Prompt) {
		signalBody, err := json.Marshal(map[string]string{
			"prompt": input.Prompt,
		})
		if err != nil {
			return // non-critical, don't block
		}
		// POST to signal endpoint — ignore errors (async on server side)
		client.Post("/api/sessions/"+input.SessionID+"/signal", signalBody)
	}
}
