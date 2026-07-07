package store

import "testing"

// TestExtractionQueueLifecycle pins the durable-queue contract H1 relies on:
// FIFO ordering, persisted attempts, delete-on-success, and a safe no-op bump on
// a row that has already been removed.
func TestExtractionQueueLifecycle(t *testing.T) {
	db := testDB(t)

	if n, err := db.PendingExtractions(); err != nil || n != 0 {
		t.Fatalf("empty queue: n=%d err=%v", n, err)
	}
	if j, err := db.NextExtraction(); err != nil || j != nil {
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

	// FIFO: the first enqueued job comes out first, fields intact.
	j, err := db.NextExtraction()
	if err != nil || j == nil {
		t.Fatalf("next: %v %+v", err, j)
	}
	if j.SessionID != "s1" || j.Kind != "session" || j.Payload != "/transcript/one" || j.Force {
		t.Fatalf("unexpected first job: %+v", j)
	}

	// Attempts persist and don't change position (still FIFO front).
	att, err := db.BumpExtractionAttempts(j.ID)
	if err != nil || att != 1 {
		t.Fatalf("bump: att=%d err=%v", att, err)
	}
	if j2, _ := db.NextExtraction(); j2 == nil || j2.ID != j.ID || j2.Attempts != 1 {
		t.Fatalf("attempts not persisted / order changed: %+v", j2)
	}

	// Delete the front → the signal job (force=true) surfaces next.
	if err := db.DeleteExtraction(j.ID); err != nil {
		t.Fatal(err)
	}
	j3, _ := db.NextExtraction()
	if j3 == nil || j3.SessionID != "s2" || j3.Kind != "signal" || j3.Payload != "remember this" || !j3.Force {
		t.Fatalf("second job wrong: %+v", j3)
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
