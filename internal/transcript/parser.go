package transcript

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

// maxTranscriptLine bounds how many bytes ParseFile retains for a single JSONL
// line. A real Claude Code transcript line is a few KB; a multi-megabyte line
// means a large paste landed in one message. bufio.Scanner used to abort the
// ENTIRE file on such a line (ErrTooLong), silently losing a whole session's
// memory — and transcripts are ephemeral, so that loss is permanent. We now
// truncate the oversized line (it then fails JSON parse and is skipped) and keep
// scanning the rest of the session. 4MB sits far above any real line while
// capping per-line memory against a pathological paste.
const maxTranscriptLine = 4 * 1024 * 1024

// ParseFile reads a JSONL transcript file and returns parsed entries. A single
// oversized or malformed line never aborts the parse — it is skipped so the rest
// of the session still extracts.
func ParseFile(path string) ([]ParsedEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open transcript: %w", err)
	}
	defer f.Close()

	var entries []ParsedEntry
	r := bufio.NewReaderSize(f, 64*1024)

	for {
		line, readErr := readLine(r, maxTranscriptLine)
		if len(line) > 0 {
			if entry, err := parseLine(line); err == nil && entry != nil {
				entries = append(entries, *entry)
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return entries, nil
			}
			return nil, fmt.Errorf("read transcript: %w", readErr)
		}
	}
}

// readLine reads one '\n'-delimited line from r, retaining at most max bytes.
// Bytes past max — and the remainder of an oversized line — are read and
// discarded, so a huge line truncates instead of aborting the scan or growing
// memory without bound. The returned slice has a trailing "\r\n" or "\n"
// stripped. It returns io.EOF when the stream ends, alongside any final
// newline-less line.
func readLine(r *bufio.Reader, max int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if room := max - len(line); room > 0 {
			if len(chunk) > room {
				line = append(line, chunk[:room]...)
			} else {
				line = append(line, chunk...)
			}
		}
		if err == bufio.ErrBufferFull {
			continue // line exceeds bufio's buffer; keep draining it
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})
		return line, err
	}
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
