package hooks

func handleStop(client *Client, input *HookInput) {
	if _, err := client.Post("/api/sessions/"+input.SessionID+"/complete", nil); err != nil {
		ExitError(err)
		return
	}
}
