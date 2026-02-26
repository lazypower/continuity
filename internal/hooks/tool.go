package hooks

import "encoding/json"

func handleTool(client *Client, input *HookInput) {
	if input.ShouldSkipTool() {
		return
	}

	// Serialize tool_input and tool_response to strings for storage
	toolInput := string(input.ToolInput)
	toolResponse := string(input.ToolResponse)

	body, err := json.Marshal(map[string]string{
		"tool_name":     input.ToolName,
		"tool_input":    toolInput,
		"tool_response": toolResponse,
	})
	if err != nil {
		ExitError(err)
		return
	}

	if _, err := client.Post("/api/sessions/"+input.SessionID+"/observations", body); err != nil {
		ExitError(err)
		return
	}
}
