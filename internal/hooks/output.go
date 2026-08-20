package hooks

import (
	"encoding/json"
	"fmt"
	"os"
)

// SessionStartOutput is the JSON structure Claude Code expects on stdout
// from the SessionStart hook.
type SessionStartOutput struct {
	HookSpecificOutput struct {
		HookEventName     string `json:"hookEventName"`
		AdditionalContext string `json:"additionalContext"`
	} `json:"hookSpecificOutput"`
}

// WriteSessionStartOutput writes the SessionStart response to stdout.
func WriteSessionStartOutput(context string) error {
	out := SessionStartOutput{}
	out.HookSpecificOutput.HookEventName = "SessionStart"
	out.HookSpecificOutput.AdditionalContext = context
	return json.NewEncoder(os.Stdout).Encode(out)
}

// UserPromptSubmitOutput is the JSON structure Claude Code expects on stdout
// from the UserPromptSubmit hook when it injects additional context (#80).
type UserPromptSubmitOutput struct {
	HookSpecificOutput struct {
		HookEventName     string `json:"hookEventName"`
		AdditionalContext string `json:"additionalContext"`
	} `json:"hookSpecificOutput"`
}

// WriteUserPromptSubmitOutput writes a UserPromptSubmit response to stdout.
// Only the gate's injection path calls this; every other submit outcome —
// including every gate failure — writes nothing (silence is the default).
func WriteUserPromptSubmitOutput(context string) error {
	out := UserPromptSubmitOutput{}
	out.HookSpecificOutput.HookEventName = "UserPromptSubmit"
	out.HookSpecificOutput.AdditionalContext = context
	return json.NewEncoder(os.Stdout).Encode(out)
}

// ExitSilent exits with code 0, no stdout. Used by all hooks except SessionStart.
func ExitSilent() {
	os.Exit(0)
}

// ExitError logs to stderr and exits 0 (hooks must never crash Claude Code).
func ExitError(err error) {
	fmt.Fprintf(os.Stderr, "continuity hook: %v\n", err)
	os.Exit(0)
}
