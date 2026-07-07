package llm

import (
	"strings"
	"testing"
)

// TestCappedWriterCapsAtLimit proves the M7 cap: a runaway subprocess writing
// past maxLLMResponse retains only the cap, but every Write still reports a full
// write so the child is never blocked or errored by us.
func TestCappedWriterCapsAtLimit(t *testing.T) {
	w := &cappedWriter{}
	chunk := strings.Repeat("x", 1<<20) // 1 MiB
	for i := 0; i < 12; i++ {           // 12 MiB total, cap is 10 MiB
		n, err := w.Write([]byte(chunk))
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(chunk) {
			t.Fatalf("Write reported %d, want full %d (child must not see a short write)", n, len(chunk))
		}
	}
	if got := len(w.String()); got != maxLLMResponse {
		t.Errorf("retained %d bytes, want cap %d", got, maxLLMResponse)
	}
	if !w.dropped {
		t.Error("dropped flag not set after exceeding the cap")
	}
}

// TestCappedWriterUnderLimit leaves small output untouched and unflagged.
func TestCappedWriterUnderLimit(t *testing.T) {
	w := &cappedWriter{}
	msg := "small output"
	n, err := w.Write([]byte(msg))
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if n != len(msg) || w.String() != msg || w.dropped {
		t.Errorf("under-limit write mismatch: n=%d s=%q dropped=%v", n, w.String(), w.dropped)
	}
}
