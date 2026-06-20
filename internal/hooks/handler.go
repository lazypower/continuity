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

	if event == "start" {
		// On the start path use a SINGLE /api/health round-trip for both liveness
		// AND skew surfacing (previously Healthy() + Status() = two 5s-timeout
		// trips). Status() error => treat as not-healthy and run the existing
		// autostart logic; success => surface any stale-server skew from the same
		// payload. Strictly non-fatal: never blocks the session.
		hs, err := client.Status()
		if err != nil || hs == nil || hs.Status != "ok" {
			if TryAutostart() {
				// Server now healthy — fall through to handleStart.
			} else {
				WriteSessionStartOutput("")
				return
			}
		} else {
			surfaceServerSkewFromHealth(client, hs)
		}
	} else {
		// Non-start events: liveness only; degrade silently if down.
		if !client.Healthy() {
			return
		}
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
