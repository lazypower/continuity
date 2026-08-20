package server

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
)

// workerTestServer builds a Server backed by a file DB (not :memory:, whose
// per-connection isolation would hide the worker goroutine's writes from the
// test goroutine). Engine is nil; tests inject s.runJob directly.
func workerTestServer(t *testing.T) *Server {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return New(db, nil, "test")
}

func waitForPending(t *testing.T, db *store.DB, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := db.PendingExtractions()
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		if n == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n, _ := db.PendingExtractions()
	t.Fatalf("timeout waiting for pending=%d (got %d)", want, n)
}

// TestExtractionWorkerDrainsAndDeletes: the worker drains queued jobs in FIFO
// order and deletes each row only after its job succeeds.
func TestExtractionWorkerDrainsAndDeletes(t *testing.T) {
	s := workerTestServer(t)
	var mu sync.Mutex
	var ran []int64
	s.runJob = func(j *store.ExtractionJob) error {
		mu.Lock()
		ran = append(ran, j.ID)
		mu.Unlock()
		return nil
	}

	for i := 0; i < 3; i++ {
		if err := s.db.EnqueueExtraction("sess", "session", "/p", false); err != nil {
			t.Fatal(err)
		}
	}

	s.StartExtractionWorker()
	s.wakeExtractionWorker()
	waitForPending(t, s.db, 0, 3*time.Second)
	s.StopExtractionWorker(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 3 {
		t.Fatalf("ran %d jobs, want 3: %v", len(ran), ran)
	}
	if !(ran[0] < ran[1] && ran[1] < ran[2]) {
		t.Errorf("jobs not processed FIFO: %v", ran)
	}
}

// TestExtractionWorkerRetainsFailedJob: a failing job is NOT deleted — its row
// stays (attempts bumped) so it replays instead of vanishing.
func TestExtractionWorkerRetainsFailedJob(t *testing.T) {
	s := workerTestServer(t)
	var calls int32
	s.runJob = func(j *store.ExtractionJob) error {
		atomic.AddInt32(&calls, 1)
		return fmt.Errorf("boom")
	}
	if err := s.db.EnqueueExtraction("sess", "session", "/p", false); err != nil {
		t.Fatal(err)
	}

	s.StartExtractionWorker()
	s.wakeExtractionWorker()

	// Wait until the worker has attempted the job at least once.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if job, _ := s.db.NextExtraction(maxExtractionAttempts); job != nil && job.Attempts >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.StopExtractionWorker(2 * time.Second)

	if n, _ := s.db.PendingExtractions(); n != 1 {
		t.Fatalf("failed job must remain queued, pending=%d", n)
	}
	job, _ := s.db.NextExtraction(maxExtractionAttempts)
	if job == nil || job.Attempts < 1 {
		t.Fatalf("attempts not bumped: %+v", job)
	}
	if atomic.LoadInt32(&calls) < 1 {
		t.Fatal("runJob was never invoked")
	}
}

// writeWorkerTranscript writes a JSONL transcript big enough to pass the
// relational extractor's content gates (>=3 user messages, >=100 condensed chars).
func writeWorkerTranscript(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	entries := []map[string]any{
		{"type": "user", "message": map[string]any{"role": "user", "content": "Help me build a Go CLI tool with cobra for task management"}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "I'll set up the project structure with go mod init and a root command."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "I prefer minimal dependencies. Use the standard library where possible."}},
		{"type": "assistant", "message": map[string]any{"role": "assistant", "content": "Understood — standard library for HTTP, JSON, and file operations."}},
		{"type": "user", "message": map[string]any{"role": "user", "content": "Remember to always use WAL mode for SQLite in production."}},
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create transcript: %v", err)
	}
	defer f.Close()
	for _, e := range entries {
		data, _ := json.Marshal(e)
		f.Write(data)
		f.Write([]byte("\n"))
	}
	return path
}

// TestRelationalOnlyPipelineWithDefaultConfig is the #78 acceptance test:
// with default gating (autoExtract off, relationalAuto on), a non-force
// /extract — the Stop/SessionEnd hook path — updates the relational profile
// through the durable queue while writing ZERO memory nodes and leaving the
// session unmarked, so a later `extract --force` still works.
func TestRelationalOnlyPipelineWithDefaultConfig(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	relationalResp := "## 1. FEEDBACK CALIBRATION\nDirect, specific feedback.\n\n" +
		"## 2. WORKING DYNAMIC\nHigh-level direction, autonomous execution.\n\n" +
		"## 3. CORRECTIONS RECEIVED\n- Always use WAL mode\n\n" +
		"## 4. EARNED SIGNALS\nTrusts agent with code generation."
	mock := &llm.MockClient{Response: &llm.Response{Content: relationalResp, Provider: "mock"}}
	eng := engine.New(db, mock)
	s := New(db, eng, "test") // autoExtract off, relationalAuto on — the defaults

	db.InitSession("rel-e2e", "proj")
	transcriptPath := writeWorkerTranscript(t)

	s.StartExtractionWorker()
	t.Cleanup(func() { s.StopExtractionWorker(2 * time.Second) })

	body := fmt.Sprintf(`{"transcript_path":%q}`, transcriptPath)
	req := newTestRequest("POST", "/api/sessions/rel-e2e/extract", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status = %d; body: %s", w.Code, w.Body.String())
	}

	waitForPending(t, db, 0, 3*time.Second)

	// The relational profile was written from this session...
	node, err := db.GetNodeByURI("mem://user/profile/communication")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if node == nil {
		t.Fatal("expected relational profile node")
	}
	if node.SourceSession != "rel-e2e" {
		t.Errorf("source_session = %q, want rel-e2e", node.SourceSession)
	}

	// ...and it is the ONLY node: memory extraction stayed gated off.
	leaves, err := db.ListLeaves()
	if err != nil {
		t.Fatalf("ListLeaves: %v", err)
	}
	if len(leaves) != 1 || leaves[0].URI != "mem://user/profile/communication" {
		t.Fatalf("leaves = %+v, want only the relational profile", leaves)
	}

	// The session is NOT marked extracted — `extract --force` remains available.
	sess, err := db.GetSession("rel-e2e")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.ExtractedAt != nil {
		t.Error("relational-only job must not mark the session extracted")
	}
}

// TestExtractionWorkerReplaysExistingQueue: jobs already in the queue when the
// worker starts (i.e. left by a prior crash) drain on start with no enqueue-wake
// — the crash-recovery guarantee.
func TestExtractionWorkerReplaysExistingQueue(t *testing.T) {
	s := workerTestServer(t)
	for i := 0; i < 2; i++ {
		if err := s.db.EnqueueExtraction("sess", "session", "/p", false); err != nil {
			t.Fatal(err)
		}
	}
	s.runJob = func(j *store.ExtractionJob) error { return nil }

	s.StartExtractionWorker() // no wake — the initial drain must pick up existing rows
	waitForPending(t, s.db, 0, 3*time.Second)
	s.StopExtractionWorker(2 * time.Second)
}
