package hooks

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
		// Non-start events: liveness only; degrade if down — but say so once per
		// session so a silent capture loss becomes a visible, fixable condition (M5).
		if !client.Healthy() {
			warnServerUnreachableOnce(input.SessionID)
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

// warnServerUnreachableOnce prints a single stderr notice per session when a hook
// cannot reach the server, converting a silent capture loss into a visible,
// fixable condition (M5). It dedups across a session's many hook invocations with
// a best-effort marker file — hooks are separate processes, so a per-process
// guard would warn on every tool call.
func warnServerUnreachableOnce(sessionID string) {
	marker := ""
	if sessionID != "" {
		marker = filepath.Join(os.TempDir(), "continuity-unreachable-"+sanitizeSessionID(sessionID))
		if _, err := os.Stat(marker); err == nil {
			return // already warned this session
		}
	}
	fmt.Fprintln(os.Stderr, "continuity: server unreachable — this session is not being captured "+
		"(run `continuity serve` or check the service)")
	if marker != "" {
		if f, err := os.Create(marker); err == nil { // best-effort; worst case the notice repeats
			f.Close()
		}
	}
}

// sanitizeSessionID reduces a session id to a filesystem-safe marker suffix.
func sanitizeSessionID(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}
