package transcript

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLines(t *testing.T) {
	lines := `{"type":"user","message":{"role":"user","content":"Hello, help me with Go code"}}
{"type":"assistant","message":{"role":"assistant","content":"Sure, I can help with Go."}}
{"type":"user","message":{"role":"user","content":"Write a function to sort a slice"}}
{"type":"assistant","message":{"role":"assistant","content":"Here is a sort function for you."}}`

	entries, err := ParseLines(lines)
	if err != nil {
		t.Fatalf("ParseLines: %v", err)
	}

	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}

	if entries[0].Type != "user" {
		t.Errorf("entry[0].Type = %q, want user", entries[0].Type)
	}
	if entries[0].Text != "Hello, help me with Go code" {
		t.Errorf("entry[0].Text = %q", entries[0].Text)
	}
	if entries[1].Type != "assistant" {
		t.Errorf("entry[1].Type = %q, want assistant", entries[1].Type)
	}
}

func TestParseLinesContentArray(t *testing.T) {
	lines := `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Here is the code:"},{"type":"tool_use","id":"tu_1","name":"Write"}]}}`

	entries, err := ParseLines(lines)
	if err != nil {
		t.Fatalf("ParseLines: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Text != "Here is the code:" {
		t.Errorf("text = %q, want 'Here is the code:'", entries[0].Text)
	}
}

func TestParseLinesSkipsShort(t *testing.T) {
	lines := `{"type":"user","message":{"role":"user","content":"ok"}}
{"type":"user","message":{"role":"user","content":"yes"}}
{"type":"user","message":{"role":"user","content":"This is a real message"}}`

	entries, err := ParseLines(lines)
	if err != nil {
		t.Fatalf("ParseLines: %v", err)
	}

	// "ok" and "yes" are < 5 chars, should be skipped
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry (skipping short), got %d", len(entries))
	}
}

func TestParseLinesSkipsJSON(t *testing.T) {
	lines := `{"type":"user","message":{"role":"user","content":"{\"json\":\"data\"}"}}
{"type":"user","message":{"role":"user","content":"Real user message here"}}`

	entries, err := ParseLines(lines)
	if err != nil {
		t.Fatalf("ParseLines: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry (skipping JSON-like), got %d", len(entries))
	}
}

func TestParseLinesStripsSystemReminder(t *testing.T) {
	lines := `{"type":"user","message":{"role":"user","content":"Do something <system-reminder>ignore this</system-reminder> please help"}}`

	entries, err := ParseLines(lines)
	if err != nil {
		t.Fatalf("ParseLines: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if strings.Contains(entries[0].Text, "system-reminder") {
		t.Errorf("system-reminder not stripped: %q", entries[0].Text)
	}
	if entries[0].Text != "Do something  please help" {
		t.Errorf("text = %q, want 'Do something  please help'", entries[0].Text)
	}
}

func TestParseLinesMalformed(t *testing.T) {
	lines := `not json at all
{"type":"user","message":{"role":"user","content":"Valid message here"}}
{broken json`

	entries, err := ParseLines(lines)
	if err != nil {
		t.Fatalf("ParseLines: %v", err)
	}

	// Should skip malformed, keep valid
	if len(entries) != 1 {
		t.Fatalf("expected 1 valid entry, got %d", len(entries))
	}
}

func TestReadLineTruncatesOversizedLine(t *testing.T) {
	// A tiny reader buffer plus a small cap forces the ErrBufferFull drain path:
	// the oversized middle line must truncate to the cap, and the line after it
	// must still read intact.
	input := "short\n" + strings.Repeat("x", 50) + "\nafter"
	r := bufio.NewReaderSize(strings.NewReader(input), 16)

	var got []string
	for {
		line, err := readLine(r, 10)
		got = append(got, string(line))
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("readLine: %v", err)
		}
	}

	want := []string{"short", strings.Repeat("x", 10), "after"}
	if len(got) != len(want) {
		t.Fatalf("got %d lines %q, want %q", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseFileSkipsOversizedLine(t *testing.T) {
	valid1 := `{"type":"user","message":{"role":"user","content":"first real message here"}}`
	// One line larger than maxTranscriptLine simulates a huge paste. It gets
	// truncated (then fails JSON parse and is skipped) instead of aborting the
	// whole file the way bufio.Scanner's ErrTooLong used to.
	huge := `{"type":"user","message":{"role":"user","content":"` + strings.Repeat("x", maxTranscriptLine+1024) + `"}}`
	valid2 := `{"type":"assistant","message":{"role":"assistant","content":"second real message here"}}`

	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	if err := os.WriteFile(path, []byte(valid1+"\n"+huge+"\n"+valid2+"\n"), 0o600); err != nil {
		t.Fatalf("write temp transcript: %v", err)
	}

	entries, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile must not error on an oversized line: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (oversized line skipped), got %d", len(entries))
	}
	if entries[0].Text != "first real message here" {
		t.Errorf("entry[0].Text = %q", entries[0].Text)
	}
	if entries[1].Text != "second real message here" {
		t.Errorf("entry[1].Text = %q", entries[1].Text)
	}
}

func TestCountUserMessages(t *testing.T) {
	entries := []ParsedEntry{
		{Type: "user", Text: "hello"},
		{Type: "assistant", Text: "hi"},
		{Type: "user", Text: "world"},
	}

	if count := CountUserMessages(entries); count != 2 {
		t.Errorf("CountUserMessages = %d, want 2", count)
	}
}

func TestCondense(t *testing.T) {
	entries := []ParsedEntry{
		{Type: "user", Text: "Help me write Go code"},
		{Type: "assistant", Text: "Sure, I can help."},
		{Type: "assistant", Text: "Here is some middle content."},
		{Type: "assistant", Text: "Final answer here."},
		{Type: "user", Text: "Thanks that works"},
	}

	result := Condense(entries)

	// Check user messages are included
	if !strings.Contains(result, "[USER] Help me write Go code") {
		t.Error("missing first user message")
	}
	if !strings.Contains(result, "[USER] Thanks that works") {
		t.Error("missing second user message")
	}

	// Check assistant messages are included
	if !strings.Contains(result, "[ASSISTANT] Sure, I can help.") {
		t.Error("missing first assistant message")
	}
	if !strings.Contains(result, "[ASSISTANT] Final answer here.") {
		t.Error("missing last assistant message")
	}
}

func TestCondenseTruncation(t *testing.T) {
	longText := strings.Repeat("x", 2000)

	entries := []ParsedEntry{
		{Type: "assistant", Text: longText}, // first → 1000
		{Type: "assistant", Text: longText}, // mid → 200
		{Type: "assistant", Text: longText}, // last → 1000
	}

	result := Condense(entries)

	// Count occurrences of "..." which indicate truncation
	parts := strings.Split(result, "...")
	if len(parts) < 4 { // 3 truncations = at least 4 parts
		t.Errorf("expected 3 truncated messages, got result len %d", len(result))
	}
}

func TestCondenseEmpty(t *testing.T) {
	if result := Condense(nil); result != "" {
		t.Errorf("expected empty string for nil, got %q", result)
	}
	if result := Condense([]ParsedEntry{}); result != "" {
		t.Errorf("expected empty string for empty, got %q", result)
	}
}
