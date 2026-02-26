package transcript

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Entry represents a single line in a Claude Code JSONL transcript.
type Entry struct {
	Type    string          `json:"type"` // "user", "assistant", "system"
	Message json.RawMessage `json:"message"`
}

// Message is the parsed message content.
type Message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"` // string or []ContentItem
}

// ContentItem represents a single content block (text, tool_use, tool_result).
type ContentItem struct {
	Type string `json:"type"` // "text", "tool_use", "tool_result"
	Text string `json:"text,omitempty"`
}

// ParsedEntry holds a fully parsed transcript entry.
type ParsedEntry struct {
	Type string // "user", "assistant", "system"
	Role string
	Text string // extracted plain text
}

var systemReminderRe = regexp.MustCompile(`<system-reminder>[\s\S]*?</system-reminder>`)

// ParseFile reads a JSONL transcript file and returns parsed entries.
func ParseFile(path string) ([]ParsedEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open transcript: %w", err)
	}
	defer f.Close()

	var entries []ParsedEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB line buffer

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		entry, err := parseLine(line)
		if err != nil {
			continue // skip malformed lines
		}
		if entry != nil {
			entries = append(entries, *entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan transcript: %w", err)
	}

	return entries, nil
}

// ParseLines parses transcript content from a string (for testing).
func ParseLines(content string) ([]ParsedEntry, error) {
	var entries []ParsedEntry
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		entry, err := parseLine([]byte(line))
		if err != nil {
			continue
		}
		if entry != nil {
			entries = append(entries, *entry)
		}
	}
	return entries, nil
}

func parseLine(line []byte) (*ParsedEntry, error) {
	var entry Entry
	if err := json.Unmarshal(line, &entry); err != nil {
		return nil, err
	}

	if entry.Type == "" || entry.Message == nil {
		return nil, nil
	}

	var msg Message
	if err := json.Unmarshal(entry.Message, &msg); err != nil {
		return nil, err
	}

	text := extractText(msg.Content)
	text = systemReminderRe.ReplaceAllString(text, "")
	text = strings.TrimSpace(text)

	if len(text) < 5 {
		return nil, nil
	}
	if strings.HasPrefix(text, "{") {
		return nil, nil
	}

	return &ParsedEntry{
		Type: entry.Type,
		Role: msg.Role,
		Text: text,
	}, nil
}

// extractText handles the polymorphic content field.
// It may be a plain string or an array of ContentItem.
func extractText(raw json.RawMessage) string {
	// Try as string first
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}

	// Try as array of content items
	var items []ContentItem
	if err := json.Unmarshal(raw, &items); err == nil {
		var texts []string
		for _, item := range items {
			if item.Type == "text" && item.Text != "" {
				texts = append(texts, item.Text)
			}
		}
		return strings.Join(texts, "\n")
	}

	return ""
}

// CountUserMessages returns the number of user messages in the entries.
func CountUserMessages(entries []ParsedEntry) int {
	count := 0
	for _, e := range entries {
		if e.Type == "user" {
			count++
		}
	}
	return count
}
