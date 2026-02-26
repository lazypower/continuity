package transcript

import (
	"strings"
)

const (
	firstLastAssistantMax = 1000
	midAssistantMax       = 200
)

// Condense reduces transcript entries to essential content.
// Proven rules from the predecessor:
// - ALL user messages (relational signal gold)
// - First + last assistant: up to 1000 chars
// - Mid assistant: up to 200 chars + "..."
// - Drop tool_use/tool_result blocks (already filtered by extractText)
// - Strip <system-reminder> tags (done in parsing)
// - Skip entries < 5 chars or starting with `{` (done in parsing)
func Condense(entries []ParsedEntry) string {
	if len(entries) == 0 {
		return ""
	}

	// Separate user and assistant messages
	var userMsgs []ParsedEntry
	var assistantMsgs []ParsedEntry
	for _, e := range entries {
		switch e.Type {
		case "user":
			userMsgs = append(userMsgs, e)
		case "assistant":
			assistantMsgs = append(assistantMsgs, e)
		}
	}

	var b strings.Builder

	// All user messages
	for _, u := range userMsgs {
		b.WriteString("[USER] ")
		b.WriteString(u.Text)
		b.WriteString("\n\n")
	}

	// Assistant messages: first + last at 1000 chars, mid at 200
	for i, a := range assistantMsgs {
		b.WriteString("[ASSISTANT] ")
		if i == 0 || i == len(assistantMsgs)-1 {
			// First or last
			if len(a.Text) > firstLastAssistantMax {
				b.WriteString(a.Text[:firstLastAssistantMax])
				b.WriteString("...")
			} else {
				b.WriteString(a.Text)
			}
		} else {
			// Mid
			if len(a.Text) > midAssistantMax {
				b.WriteString(a.Text[:midAssistantMax])
				b.WriteString("...")
			} else {
				b.WriteString(a.Text)
			}
		}
		b.WriteString("\n\n")
	}

	return strings.TrimSpace(b.String())
}
