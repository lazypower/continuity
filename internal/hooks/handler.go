package hooks

import (
	"encoding/json"
	"fmt"
	"io"
)

// Handle reads HookInput from the given reader, dispatches to the appropriate
// handler based on the event argument, and writes output to stdout.
func Handle(event string, stdin io.Reader) {
	var input HookInput
	if err := json.NewDecoder(stdin).Decode(&input); err != nil {
		// Stdin may be empty for some events — degrade gracefully
		if event == "start" {
			WriteSessionStartOutput("")
			return
		}
		ExitError(fmt.Errorf("decode stdin: %w", err))
		return
	}

	client := NewClient()

	// Check server health — degrade gracefully if down
	if !client.Healthy() {
		if event == "start" {
			WriteSessionStartOutput("")
			return
		}
		return // silent exit for other events
	}

	switch event {
	case "start":
		handleStart(client, &input)
	case "submit":
		handleSubmit(client, &input)
	case "tool":
		handleTool(client, &input)
	case "stop":
		handleStop(client, &input)
	case "end":
		handleEnd(client, &input)
	default:
		ExitError(fmt.Errorf("unknown hook event: %s", event))
	}
}
