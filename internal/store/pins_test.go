package store

import (
	"strings"
	"testing"
)

func TestPinNode_SetsPinnedAndIdempotent(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/feedback/codex-before-pr", "feedback", "codex review before every PR")

	newly, err := db.PinNode("mem://user/feedback/codex-before-pr")
	if err != nil {
		t.Fatalf("PinNode: %v", err)
	}
	if !newly {
		t.Errorf("newly = false, want true on first pin")
	}

	got, err := db.GetNodeByURI("mem://user/feedback/codex-before-pr")
	if err != nil || got == nil {
		t.Fatalf("GetNodeByURI returned nil/err: %v", err)
	}
	if !got.IsPinned() {
		t.Errorf("IsPinned() = false after PinNode")
	}
	firstStamp := *got.PinnedAt

	// Idempotent: second pin is a no-op and preserves the original timestamp.
	newly, err = db.PinNode("mem://user/feedback/codex-before-pr")
	if err != nil {
		t.Fatalf("PinNode (second): %v", err)
	}
	if newly {
		t.Errorf("newly = true on repeat pin, want false (idempotent)")
	}
	got, _ = db.GetNodeByURI("mem://user/feedback/codex-before-pr")
	if *got.PinnedAt != firstStamp {
		t.Errorf("pinned_at changed on repeat pin: %d != %d", *got.PinnedAt, firstStamp)
	}
}

func TestUnpinNode_ClearsAndIdempotent(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/feedback/foo", "feedback", "foo")
	if _, err := db.PinNode("mem://user/feedback/foo"); err != nil {
		t.Fatalf("PinNode: %v", err)
	}

	newly, err := db.UnpinNode("mem://user/feedback/foo")
	if err != nil {
		t.Fatalf("UnpinNode: %v", err)
	}
	if !newly {
		t.Errorf("newly = false, want true on first unpin")
	}
	got, _ := db.GetNodeByURI("mem://user/feedback/foo")
	if got.IsPinned() {
		t.Errorf("IsPinned() = true after UnpinNode")
	}

	// Idempotent unpin of an unpinned node.
	newly, err = db.UnpinNode("mem://user/feedback/foo")
	if err != nil {
		t.Fatalf("UnpinNode (second): %v", err)
	}
	if newly {
		t.Errorf("newly = true on repeat unpin, want false")
	}
}

func TestPinNode_RejectsMissingDirAndRetracted(t *testing.T) {
	db := testDB(t)

	// Missing.
	if _, err := db.PinNode("mem://user/feedback/nope"); err == nil {
		t.Errorf("PinNode on missing URI: want error, got nil")
	} else if !isPinValidation(err) {
		t.Errorf("missing URI error not a PinValidationError: %v", err)
	}

	// Retracted: cannot pin an unlearned memory.
	seedNode(t, db, "mem://user/events/gone", "events", "gone")
	if _, err := db.RetractNode("mem://user/events/gone", "test", ""); err != nil {
		t.Fatalf("RetractNode: %v", err)
	}
	_, err := db.PinNode("mem://user/events/gone")
	if err == nil {
		t.Errorf("PinNode on retracted URI: want error, got nil")
	} else if !strings.Contains(err.Error(), "retracted") {
		t.Errorf("retracted-pin error = %q, want mention of 'retracted'", err.Error())
	}
}

// TestListPinned_ExcludesRetracted is the load-bearing safety test: a memory
// that is pinned and then retracted MUST NOT appear in ListPinned, because that
// list is the cold-boot Pinned section. Retraction is an unlearning — it wins.
func TestListPinned_ExcludesRetracted(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/feedback/live", "feedback", "live pin")
	seedNode(t, db, "mem://user/feedback/doomed", "feedback", "doomed pin")
	if _, err := db.PinNode("mem://user/feedback/live"); err != nil {
		t.Fatalf("pin live: %v", err)
	}
	if _, err := db.PinNode("mem://user/feedback/doomed"); err != nil {
		t.Fatalf("pin doomed: %v", err)
	}

	// Retract one of the two pins.
	if _, err := db.RetractNode("mem://user/feedback/doomed", "no longer accurate", ""); err != nil {
		t.Fatalf("retract doomed: %v", err)
	}

	pins, err := db.ListPinned()
	if err != nil {
		t.Fatalf("ListPinned: %v", err)
	}
	if len(pins) != 1 {
		t.Fatalf("ListPinned returned %d pins, want 1 (retracted pin must be excluded)", len(pins))
	}
	if pins[0].URI != "mem://user/feedback/live" {
		t.Errorf("surviving pin = %s, want mem://user/feedback/live", pins[0].URI)
	}
}

func TestListPinned_OrdersByPinTime(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/feedback/a", "feedback", "a")
	seedNode(t, db, "mem://user/feedback/b", "feedback", "b")
	// Pin a, then b. Oldest pin first.
	if _, err := db.PinNode("mem://user/feedback/a"); err != nil {
		t.Fatalf("pin a: %v", err)
	}
	if _, err := db.PinNode("mem://user/feedback/b"); err != nil {
		t.Fatalf("pin b: %v", err)
	}
	pins, err := db.ListPinned()
	if err != nil {
		t.Fatalf("ListPinned: %v", err)
	}
	if len(pins) != 2 {
		t.Fatalf("want 2 pins, got %d", len(pins))
	}
	// a was pinned first; with ASC ordering it should come first. (Both stamps
	// could collide at ms resolution; tolerate that by only asserting a is not
	// after b — i.e. a's stamp <= b's stamp, which the seed order guarantees.)
	if *pins[0].PinnedAt > *pins[1].PinnedAt {
		t.Errorf("pins not ordered by pinned_at ASC: %d > %d", *pins[0].PinnedAt, *pins[1].PinnedAt)
	}
}

// TestPinNode_EnforcesCap verifies the write-time cap: the (MaxPins+1)th pin is
// rejected, so the accepted contract never exceeds the injectable one. Re-pinning
// an already-pinned node does not count against the cap.
func TestPinNode_EnforcesCap(t *testing.T) {
	db := testDB(t)

	// Pin exactly MaxPins memories — all should succeed.
	for i := 0; i < MaxPins; i++ {
		uri := "mem://user/feedback/cap-" + string(rune('a'+i))
		seedNode(t, db, uri, "feedback", "cap test")
		if _, err := db.PinNode(uri); err != nil {
			t.Fatalf("pin %d/%d: %v", i+1, MaxPins, err)
		}
	}

	// Re-pinning an existing pin is idempotent and must NOT be blocked by the cap.
	if _, err := db.PinNode("mem://user/feedback/cap-a"); err != nil {
		t.Errorf("re-pin of existing pin blocked by cap: %v", err)
	}

	// The next NEW pin must be rejected.
	seedNode(t, db, "mem://user/feedback/one-too-many", "feedback", "over cap")
	_, err := db.PinNode("mem://user/feedback/one-too-many")
	if err == nil {
		t.Fatalf("pin beyond cap: want error, got nil")
	}
	if !isPinValidation(err) {
		t.Errorf("over-cap error not a PinValidationError: %v", err)
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("over-cap error = %q, want mention of the limit", err.Error())
	}

	// After unpinning one, a new pin fits again — the cap counts live pins.
	if _, err := db.UnpinNode("mem://user/feedback/cap-a"); err != nil {
		t.Fatalf("unpin: %v", err)
	}
	if _, err := db.PinNode("mem://user/feedback/one-too-many"); err != nil {
		t.Errorf("pin after freeing a slot: %v", err)
	}
}

func isPinValidation(err error) bool {
	_, ok := err.(*PinValidationError)
	return ok
}
