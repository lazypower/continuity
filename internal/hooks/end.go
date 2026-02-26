package hooks

func handleEnd(client *Client, input *HookInput) {
	if _, err := client.Post("/api/sessions/"+input.SessionID+"/end", nil); err != nil {
		ExitError(err)
		return
	}
}
