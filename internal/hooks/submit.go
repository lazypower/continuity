package hooks

import "encoding/json"

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
}
