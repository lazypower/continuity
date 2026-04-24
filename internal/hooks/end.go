package hooks

import "encoding/json"

func handleEnd(client *Client, input *HookInput) {
	if _, err := client.Post("/api/sessions/"+input.SessionID+"/end", nil); err != nil {
		ExitError(err)
		return
	}

	// Belt-and-suspenders: also trigger extraction on SessionEnd. Stop fires
	// per-turn and handles the common case, but SessionEnd is our last chance
	// for sessions where Stop didn't run (e.g. terminal killed) or where the
	// final turn pushed content across the threshold after the prior Stop.
	// The server's idempotency guard + content gate make this safe to call
	// even when Stop already extracted.
	if input.TranscriptPath != "" {
		body, _ := json.Marshal(map[string]string{
			"transcript_path": input.TranscriptPath,
		})
		client.Post("/api/sessions/"+input.SessionID+"/extract", body)
	}
}
