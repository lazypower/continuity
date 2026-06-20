package hooks

import (
	"encoding/json"
	"fmt"
	"io"
)

const maxHookInputSize = 10 << 20 // 10MB

// Handle reads HookInput from the given reader, dispatches to the appropriate
// handler based on the event argument, and writes output to stdout.
func Handle(event string, stdin io.Reader) {
	var input HookInput
	if err := json.NewDecoder(io.LimitReader(stdin, maxHookInputSize)).Decode(&input); err != nil {
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
			if TryAutostart() {
				// Server now healthy — fall through to handleStart
			} else {
				WriteSessionStartOutput("")
				return
			}
		} else {
			return // silent exit for other events
		}
	} else if event == "start" {
		// Server is already running and healthy. Surface (and optionally bounce)
		// a stale post-upgrade server so it can't hide. Best-effort and strictly
		// non-fatal — never blocks the session.
		surfaceServerSkew(client)
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
