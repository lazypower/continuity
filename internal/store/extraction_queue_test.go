package store

import "testing"

const testMaxAttempts = 20

// TestExtractionQueueLifecycle pins the durable-queue contract: fresh-first /
// fewer-failed ordering (head-of-line guard), persisted attempts, and
// delete-on-success.
func TestExtractionQueueLifecycle(t *testing.T) {
	db := testDB(t)

	if n, err := db.PendingExtractions(); err != nil || n != 0 {
		t.Fatalf("empty queue: n=%d err=%v", n, err)
	}
	if j, err := db.NextExtraction(testMaxAttempts); err != nil || j != nil {
		t.Fatalf("NextExtraction on empty: j=%+v err=%v", j, err)
	}

	if err := db.EnqueueExtraction("s1", "session", "/transcript/one", false); err != nil {
		t.Fatal(err)
	}
	if err := db.EnqueueExtraction("s2", "signal", "remember this", true); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.PendingExtractions(); n != 2 {
		t.Fatalf("pending=%d want 2", n)
	}

	// Two fresh jobs (attempts 0): the older id comes first.
	j, err := db.NextExtraction(testMaxAttempts)
	if err != nil || j == nil {
		t.Fatalf("next: %v %+v", err, j)
	}
	if j.SessionID != "s1" || j.Kind != "session" || j.Payload != "/transcript/one" || j.Force {
		t.Fatalf("unexpected first job: %+v", j)
	}

	// Bump s1 → attempts 1; it now sinks BELOW the fresh s2, so the next eligible
	// job is s2 (a repeatedly-failing job can't head-of-line-block fresh work).
	if att, err := db.BumpExtractionAttempts(j.ID); err != nil || att != 1 {
		t.Fatalf("bump: att=%d err=%v", att, err)
	}
	j2, _ := db.NextExtraction(testMaxAttempts)
	if j2 == nil || j2.SessionID != "s2" || j2.Kind != "signal" || !j2.Force {
		t.Fatalf("failed job should sink below fresh work; got %+v", j2)
	}

	// Delete s2 → only s1 (attempts 1) remains and is returned, attempts intact.
	if err := db.DeleteExtraction(j2.ID); err != nil {
		t.Fatal(err)
	}
	j3, _ := db.NextExtraction(testMaxAttempts)
	if j3 == nil || j3.SessionID != "s1" || j3.Attempts != 1 {
		t.Fatalf("s1 not returned with persisted attempts: %+v", j3)
	}

	if err := db.DeleteExtraction(j3.ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.PendingExtractions(); n != 0 {
		t.Fatalf("pending=%d want 0", n)
	}

	// Bumping a vanished row is a safe no-op, not an error.
	if att, err := db.BumpExtractionAttempts(99999); err != nil || att != 0 {
		t.Fatalf("bump missing row: att=%d err=%v", att, err)
	}
}

// TestExtractionQueueParksExhaustedJob: a job that reaches the attempt cap is
// PARKED — excluded from NextExtraction so it can't wedge the queue — but NEVER
// deleted, so the capture is not silently lost and stays visible in the backlog.
func TestExtractionQueueParksExhaustedJob(t *testing.T) {
	db := testDB(t)
	if err := db.EnqueueExtraction("s", "session", "/p", false); err != nil {
		t.Fatal(err)
	}
	j, _ := db.NextExtraction(testMaxAttempts)
	if j == nil {
		t.Fatal("expected a claimable job")
	}
	for i := 0; i < testMaxAttempts; i++ {
		if _, err := db.BumpExtractionAttempts(j.ID); err != nil {
			t.Fatal(err)
		}
	}
	// Parked: no longer claimable...
	if pj, err := db.NextExtraction(testMaxAttempts); err != nil || pj != nil {
		t.Fatalf("exhausted job must be parked (unclaimable): %+v err=%v", pj, err)
	}
	// ...but still present — not lost — and counted in the backlog.
	if n, _ := db.PendingExtractions(); n != 1 {
		t.Fatalf("parked job must remain queued, pending=%d want 1", n)
	}
}
