package hooks

import (
	"encoding/json"
	"strings"
)

// signalTriggers are phrases that indicate the user wants something remembered immediately.
var signalTriggers = []string{
	"remember this", "don't forget",
	"always use", "never use", "always do", "never do",
	"architecture decision", "we decided",
	"this pattern", "the trick is",
	"bug was", "root cause", "the fix was",
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
