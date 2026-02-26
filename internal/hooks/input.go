package hooks

import "encoding/json"

// HookInput represents the JSON that Claude Code sends on stdin to hook handlers.
// All fields are optional — different events populate different subsets.
type HookInput struct {
	SessionID      string `json:"session_id"`
	TranscriptPath string `json:"transcript_path"`
	CWD            string `json:"cwd"`
	HookEventName  string `json:"hook_event_name"`

	// SessionStart
	Source string `json:"source,omitempty"`
	Model  string `json:"model,omitempty"`

	// UserPromptSubmit
	Prompt string `json:"prompt,omitempty"`

	// PostToolUse
	ToolName     string          `json:"tool_name,omitempty"`
	ToolUseID    string          `json:"tool_use_id,omitempty"`
	ToolInput    json.RawMessage `json:"tool_input,omitempty"`
	ToolResponse json.RawMessage `json:"tool_response,omitempty"`

	// Stop
	StopHookActive       bool   `json:"stop_hook_active,omitempty"`
	LastAssistantMessage string `json:"last_assistant_message,omitempty"`

	// SessionEnd
	Reason string `json:"reason,omitempty"`
}

// skipTools are meta-tools that generate noise, not useful observations.
var skipTools = map[string]bool{
	"TodoRead":  true,
	"TodoWrite": true,
	"Thinking":  true,
	"TaskList":     true,
	"TaskCreate":   true,
	"TaskGet":      true,
	"TaskUpdate":   true,
}

// ShouldSkipTool returns true if this tool should not be recorded as an observation.
func (h *HookInput) ShouldSkipTool() bool {
	return skipTools[h.ToolName]
}
