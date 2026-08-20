package hooks

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// internalSentinel is the prefix added to all Continuity extraction prompts.
// When claude -p is used for LLM calls, it spawns a new Claude Code session
// that fires hooks — including UserPromptSubmit. This sentinel lets the hook
// handler recognize and skip prompts that originated from Continuity itself,
// preventing recursive signal amplification.
//
// Must match llm.InternalSentinel exactly.
const internalSentinel = "[continuity-internal]"

// signalTriggers are phrases that indicate the user wants something remembered immediately.
// Keep this list tight — only explicit memory requests and strong decision signals.
// Broad phrases like "this pattern" or "the trick is" fire on normal conversation.
var signalTriggers = []string{
	"remember this", "don't forget",
	"always use", "never use", "always do", "never do",
	"architecture decision",
	"root cause was", "the fix was",
}

// isInternalPrompt returns true if the prompt is a Continuity extraction prompt,
// not a real user message. Checks for sentinel prefix only — the sentinel must
// be at the start of the prompt to prevent false matches on user messages that
// happen to contain the string.
func isInternalPrompt(prompt string) bool {
	return strings.HasPrefix(prompt, internalSentinel)
}

const (
	// maxSignalPromptLen bounds the whole prompt: a trigger phrase buried in a
	// large paste is not the operator asking to remember something — it's
	// third-party content that happened to transit the session. Signals only fire
	// on plausibly-human messages (H5 memory-poisoning defense).
	maxSignalPromptLen = 2000
	// maxSignalTriggerOffset requires the trigger near the START of the message,
	// so a paste whose body contains "always use X" deep inside cannot self-author
	// an attacker-controlled memory.
	maxSignalTriggerOffset = 500
)

// hasSignal reports whether the prompt is a plausibly-human, explicit
// memory-flagging message: short enough to be a real instruction, with a trigger
// phrase near its start. A trigger buried in a large paste does NOT qualify —
// that is the drive-by memory-poisoning vector we refuse (H5).
func hasSignal(prompt string) bool {
	if len(prompt) > maxSignalPromptLen {
		return false
	}
	lower := strings.ToLower(prompt)
	for _, trigger := range signalTriggers {
		if idx := strings.Index(lower, trigger); idx >= 0 && idx <= maxSignalTriggerOffset {
			return true
		}
	}
	return false
}

// signalGatedByLength reports whether the prompt carries an up-front memory cue
// that hasSignal rejected SOLELY because the message exceeds maxSignalPromptLen
// (i.e. a deliberate but long "remember this: ..."). It deliberately does not
// fire for a cue buried past maxSignalTriggerOffset — that's a paste, not a
// gated instruction — so the visibility note only surfaces the real over-block.
func signalGatedByLength(prompt string) bool {
	if len(prompt) <= maxSignalPromptLen {
		return false
	}
	lower := strings.ToLower(prompt)
	for _, trigger := range signalTriggers {
		if idx := strings.Index(lower, trigger); idx >= 0 && idx <= maxSignalTriggerOffset {
			return true
		}
	}
	return false
}

func handleSubmit(client *Client, input *HookInput) {
	// Guard: skip prompts from Continuity's own LLM calls to prevent recursion.
	// When the server calls claude -p for extraction, that spawns a new session
	// whose hooks fire back into us. The sentinel prefix lets us bail early.
	if isInternalPrompt(input.Prompt) {
		return
	}

	// Initialize/resume session on first user prompt. Project is the
	// normalized repository identity, not the raw cwd (#79) — see
	// projectIdentity for why normalization is hook-side.
	body, err := json.Marshal(map[string]string{
		"session_id": input.SessionID,
		"project":    projectIdentity(input.CWD),
	})
	if err != nil {
		ExitError(err)
		return
	}

	if _, err := client.Post("/api/sessions/init", body); err != nil {
		ExitError(err)
		return
	}

	// Check for signal keywords — fire and forget
	if input.Prompt != "" {
		if hasSignal(input.Prompt) {
			signalBody, err := json.Marshal(map[string]string{
				"prompt": input.Prompt,
			})
			if err != nil {
				return // non-critical, don't block
			}
			// POST to signal endpoint — ignore errors (async on server side)
			client.Post("/api/sessions/"+input.SessionID+"/signal", signalBody)
		} else if signalGatedByLength(input.Prompt) {
			// A deliberate, up-front cue that was simply too long to fire as an
			// immediate signal — say so rather than skip silently. Session-end
			// extraction still captures it (visibility for the H5 length gate).
			fmt.Fprintln(os.Stderr, "continuity: memory cue found but the message is too long to fire as an immediate signal — it will still be considered at session end")
		}
	}
}
