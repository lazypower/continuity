package server

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		if job, _ := s.db.NextExtraction(); job != nil && job.Attempts >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.StopExtractionWorker(2 * time.Second)

	if n, _ := s.db.PendingExtractions(); n != 1 {
		t.Fatalf("failed job must remain queued, pending=%d", n)
	}
	job, _ := s.db.NextExtraction()
	if job == nil || job.Attempts < 1 {
		t.Fatalf("attempts not bumped: %+v", job)
	}
	if atomic.LoadInt32(&calls) < 1 {
		t.Fatal("runJob was never invoked")
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
