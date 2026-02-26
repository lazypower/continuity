package hooks

import (
	"encoding/json"
	"net/url"
)

func handleStart(client *Client, input *HookInput) {
	// Get context from server
	params := url.Values{}
	if input.SessionID != "" {
		params.Set("session_id", input.SessionID)
	}

	data, err := client.Get("/api/context?" + params.Encode())
	if err != nil {
		// Degrade gracefully — return empty context
		WriteSessionStartOutput("")
		return
	}

	var resp struct {
		Context string `json:"context"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		WriteSessionStartOutput("")
		return
	}

	WriteSessionStartOutput(resp.Context)
}
