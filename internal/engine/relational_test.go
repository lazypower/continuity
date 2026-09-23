package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
	"github.com/lazypower/continuity/internal/transcript"
)

const relationalProfileResp = `## 1. FEEDBACK CALIBRATION
Direct corrections with reasoning; prefers measured evidence over theory.
## 2. WORKING DYNAMIC
Delegates tactical decisions and expects verification before claims.`

// lastEntryUUID returns the uuid the relational mark would record for a
// transcript: its last parsed entry.
func lastEntryUUID(t *testing.T, path string) string {
	t.Helper()
	entries, err := transcript.ParseFile(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return lastUUID(entries)
}

// userTurns builds n user/assistant exchanges whose text carries tag, with
// uuids offset by start so an appended batch continues the same transcript.
func userTurns(tag string, start, n int) []map[string]any {
	var out []map[string]any
	for i := 0; i < n; i++ {
		k := start + 2*i
		out = append(out,
			map[string]any{"type": "user", "uuid": fmt.Sprintf("u-%d", k), "message": map[string]any{"role": "user", "content": fmt.Sprintf("%s user message number %d about the build", tag, i)}},
			map[string]any{"type": "assistant", "uuid": fmt.Sprintf("u-%d", k+1), "message": map[string]any{"role": "assistant", "content": fmt.Sprintf("%s assistant reply number %d with some detail", tag, i)}},
		)
	}
	return out
}

func relationalSession(t *testing.T, db *store.DB, id string) {
	t.Helper()
	if _, err := db.InitSession(id, "proj"); err != nil {
		t.Fatalf("InitSession: %v", err)
	}
}

func markOf(t *testing.T, db *store.DB, id string) string {
	t.Helper()
	m, _, err := db.RelationalMark(id)
	if err != nil {
		t.Fatal(err)
	}
	return m.String
}

// A growing session merges only what it said since the last merge, and a run
// with nothing new makes no LLM call (#83).
func TestRelationalMergesOnlyNewEvidence(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "grow")
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp, Provider: "mock"}}

	first := userTurns("EARLY", 0, 3)
	path := writeTranscript(t, first)
	if err := extractRelational(db, mock, "grow", path); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 1 || markOf(t, db, "grow") != "u-5" {
		t.Fatalf("first run: calls=%d mark=%q, want 1 call and mark u-5", len(mock.Calls), markOf(t, db, "grow"))
	}

	// Nothing new: no LLM call.
	if err := extractRelational(db, mock, "grow", path); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 1 {
		t.Fatalf("rerun with no new entries made %d calls, want 1 total", len(mock.Calls))
	}

	// The session grows by one exchange: that exchange is the whole delta,
	// even though it alone would not pass the three-message session gate.
	grown := writeTranscript(t, append(first, userTurns("LATE", 6, 1)...))
	if err := extractRelational(db, mock, "grow", grown); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 2 {
		t.Fatalf("calls = %d after growth, want 2", len(mock.Calls))
	}
	prompt := mock.Calls[1]
	if !strings.Contains(prompt, "LATE user message") || strings.Contains(prompt, "EARLY user message") {
		t.Errorf("second merge must carry only the new exchange; prompt:\n%s", prompt)
	}
	if markOf(t, db, "grow") != "u-7" {
		t.Errorf("mark = %q, want u-7", markOf(t, db, "grow"))
	}
}

// Another session writing the profile in between does not make this session
// replay its merged evidence (the defect the old latest-writer guard had).
func TestRelationalOtherWriterDoesNotReplay(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "a")
	relationalSession(t, db, "b")
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp, Provider: "mock"}}

	pathA := writeTranscript(t, userTurns("A", 0, 3))
	pathB := writeTranscript(t, userTurns("B", 0, 3))
	for _, run := range []struct{ id, path string }{{"a", pathA}, {"b", pathB}, {"a", pathA}} {
		if err := extractRelational(db, mock, run.id, run.path); err != nil {
			t.Fatal(err)
		}
	}
	if len(mock.Calls) != 2 {
		t.Fatalf("calls = %d, want 2 (a's rerun after b wrote has nothing new)", len(mock.Calls))
	}
}

// Deliberate rejections consume the delta; a transport error does not.
func TestRelationalMarkAdvancesOnlyOnDeliberateOutcome(t *testing.T) {
	db := testDB(t)
	path := writeTranscript(t, userTurns("X", 0, 3))

	relationalSession(t, db, "noupdate")
	if err := extractRelational(db, &llm.MockClient{Response: &llm.Response{Content: "NO_UPDATE"}}, "noupdate", path); err != nil {
		t.Fatal(err)
	}
	if markOf(t, db, "noupdate") != "u-5" {
		t.Errorf("NO_UPDATE must advance the mark, got %q", markOf(t, db, "noupdate"))
	}

	relationalSession(t, db, "llm-down")
	err := extractRelational(db, &llm.MockClient{Err: errors.New("llm down")}, "llm-down", path)
	if err == nil {
		t.Fatal("transport error must surface so the durable job retries")
	}
	if got := markOf(t, db, "llm-down"); got != "" {
		t.Errorf("transport error advanced the mark to %q", got)
	}
}

// A session that wrote the profile before marks existed is marked to its end
// without an LLM call, instead of replaying its whole transcript.
func TestRelationalLegacySessionMarksWithoutReplay(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "legacy")
	// Sessions that predate migration 18 carry a NULL mark.
	if _, err := db.Exec(`UPDATE sessions SET relational_mark = NULL WHERE session_id = ?`, "legacy"); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertNode(&store.MemNode{URI: relationalURI, NodeType: "leaf", Category: "profile",
		L0Abstract: "profile", L1Overview: relationalProfileResp, SourceSession: "legacy"}); err != nil {
		t.Fatal(err)
	}
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp}}
	path := writeTranscript(t, userTurns("OLD", 0, 3))

	if err := extractRelational(db, mock, "legacy", path); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 0 || markOf(t, db, "legacy") != "u-5" {
		t.Fatalf("calls=%d mark=%q, want 0 calls and mark u-5", len(mock.Calls), markOf(t, db, "legacy"))
	}
}

// A mark that no longer appears in the transcript (the file was rewritten)
// merges from the start rather than skip evidence it cannot place.
func TestRelationalMissingMarkMergesFromStart(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "rewritten")
	if err := db.SetRelationalMark("rewritten", "gone-uuid"); err != nil {
		t.Fatal(err)
	}
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp}}
	path := writeTranscript(t, userTurns("R", 0, 3))

	if err := extractRelational(db, mock, "rewritten", path); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 1 || !strings.Contains(mock.Calls[0], "R user message number 0") {
		t.Fatalf("want one merge carrying the whole transcript, calls=%d", len(mock.Calls))
	}
}

// Without a session row there is nowhere to record progress; merging would
// repeat the whole transcript on every Stop, so the session is skipped.
func TestRelationalSkipsSessionWithoutRow(t *testing.T) {
	db := testDB(t)
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp}}
	path := writeTranscript(t, userTurns("N", 0, 3))
	if err := extractRelational(db, mock, "no-row", path); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 0 {
		t.Fatalf("calls = %d, want 0 for a session without a row", len(mock.Calls))
	}
	if n, _ := db.GetNodeByURI(relationalURI); n != nil {
		t.Fatal("profile written for a session without a row")
	}
}

// With auto extraction on, a session already marked extracted still merges its
// new relational evidence on later Stops.
func TestExtractSessionAlreadyExtractedStillMergesRelational(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "auto")
	if err := db.MarkExtracted("auto"); err != nil {
		t.Fatal(err)
	}
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp}}
	eng := New(db, mock)
	if err := eng.ExtractSession("auto", writeTranscript(t, userTurns("AUTO", 0, 3))); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 1 {
		t.Fatalf("calls = %d, want 1 (relational only; memory extraction already ran)", len(mock.Calls))
	}
	if n, _ := db.GetNodeByURI(relationalURI); n == nil {
		t.Fatal("expected the relational profile to be written")
	}
}

// A new session whose first merge wrote the profile but whose mark write failed
// must not take the legacy branch: the retry merges from the start, including
// turns added since, instead of marking to the end and skipping them.
func TestRelationalFailedFirstMarkDoesNotSkipLaterTurns(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "fresh")
	initial, _, err := db.RelationalMark("fresh")
	if err != nil {
		t.Fatal(err)
	}
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp}}
	first := userTurns("FIRST", 0, 3)
	if err := extractRelational(db, mock, "fresh", writeTranscript(t, first)); err != nil {
		t.Fatal(err)
	}
	// Simulate the mark write having failed after the profile upsert: the
	// session keeps the mark InitSession gave it.
	if _, err := db.Exec(`UPDATE sessions SET relational_mark = ? WHERE session_id = ?`, initial, "fresh"); err != nil {
		t.Fatal(err)
	}
	grown := writeTranscript(t, append(first, userTurns("AFTER", 6, 1)...))
	if err := extractRelational(db, mock, "fresh", grown); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 2 || !strings.Contains(mock.Calls[1], "AFTER user message") {
		t.Fatalf("retry must merge the later turns; calls=%d", len(mock.Calls))
	}
}

// Trailing entries without a uuid wait for a later entry that has one, rather
// than being re-sent on every run.
func TestRelationalUUIDlessTailWaits(t *testing.T) {
	db := testDB(t)
	relationalSession(t, db, "tail")
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalProfileResp}}
	base := userTurns("BASE", 0, 3)
	noID := map[string]any{"type": "user", "uuid": "", "message": map[string]any{"role": "user", "content": "TAIL message that has no uuid yet"}}
	withTail := append(append([]map[string]any{}, base...), noID)

	path := writeTranscript(t, withTail)
	for i := 0; i < 2; i++ {
		if err := extractRelational(db, mock, "tail", path); err != nil {
			t.Fatal(err)
		}
	}
	if len(mock.Calls) != 1 || strings.Contains(mock.Calls[0], "TAIL message") || markOf(t, db, "tail") != "u-5" {
		t.Fatalf("calls=%d mark=%q: the uuid-less tail must wait, and only once", len(mock.Calls), markOf(t, db, "tail"))
	}

	later := writeTranscript(t, append(withTail, userTurns("NEXT", 6, 1)...))
	if err := extractRelational(db, mock, "tail", later); err != nil {
		t.Fatal(err)
	}
	if len(mock.Calls) != 2 || !strings.Contains(mock.Calls[1], "TAIL message") || !strings.Contains(mock.Calls[1], "NEXT user message") {
		t.Fatalf("the waiting tail must merge with the next uuid'd entry; calls=%d", len(mock.Calls))
	}
}
