package hooks

import (
	"encoding/json"
	"errors"
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
	if sessionID != "" {
		marker := filepath.Join(os.TempDir(), "continuity-unreachable-"+sanitizeSessionID(sessionID))
		// Atomic claim: only the process that CREATES the marker warns. O_EXCL
		// closes the check-then-create race, so concurrent hooks for the same
		// session don't both print. An existing marker means another hook already
		// warned (stay silent); any other error (e.g. unwritable tmp) falls through
		// and warns best-effort rather than going silent.
		f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		switch {
		case err == nil:
			f.Close()
		case errors.Is(err, os.ErrExist):
			return
		}
	}
	fmt.Fprintln(os.Stderr, "continuity: server unreachable — this session is not being captured "+
		"(run `continuity serve` or check the service)")
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
