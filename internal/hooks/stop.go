package hooks

import (
	"encoding/json"

	"github.com/lazypower/continuity/internal/transcript"
)

func handleStop(client *Client, input *HookInput) {
	if _, err := client.Post("/api/sessions/"+input.SessionID+"/complete", nil); err != nil {
		ExitError(err)
		return
	}

	// Trigger async extraction only when the transcript has enough content
	// to be worth extracting. Stop fires per-turn, so most early turns will
	// skip here. The server applies the same gate as belt-and-suspenders,
	// but doing the cheap parse locally avoids per-turn HTTP round-trips.
	if input.TranscriptPath != "" && shouldExtract(input.TranscriptPath) {
		body, _ := json.Marshal(map[string]string{
			"transcript_path": input.TranscriptPath,
		})
		// Fire and forget — extraction is async (202 Accepted)
		client.Post("/api/sessions/"+input.SessionID+"/extract", body)
	}
}

// shouldExtract mirrors engine.hasEnoughContent so Stop can skip the HTTP
// call on turns that wouldn't pass the server-side gate anyway.
func shouldExtract(transcriptPath string) bool {
	entries, err := transcript.ParseFile(transcriptPath)
	if err != nil {
		// If we can't read the transcript, let SessionEnd sort it out.
		return false
	}
	if transcript.CountUserMessages(entries) < 3 {
		return false
	}
	if len(transcript.Condense(entries)) < 100 {
		return false
	}
	return true
}
