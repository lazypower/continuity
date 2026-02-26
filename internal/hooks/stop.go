package hooks

import "encoding/json"

func handleStop(client *Client, input *HookInput) {
	if _, err := client.Post("/api/sessions/"+input.SessionID+"/complete", nil); err != nil {
		ExitError(err)
		return
	}

	// Trigger async extraction with transcript path
	if input.TranscriptPath != "" {
		body, _ := json.Marshal(map[string]string{
			"transcript_path": input.TranscriptPath,
		})
		// Fire and forget — extraction is async (202 Accepted)
		client.Post("/api/sessions/"+input.SessionID+"/extract", body)
	}
}
