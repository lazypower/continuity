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
	// Forward project identity at t=0 (#79): the sessions row doesn't exist
	// until the first UserPromptSubmit, but SessionStart already carries cwd —
	// without this, the corpus index and Recent Sessions can't be
	// project-scoped at boot.
	if project := projectIdentity(input.CWD); project != "" {
		params.Set("project", project)
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
