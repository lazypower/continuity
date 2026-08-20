package store

import (
	"fmt"
	"testing"
	"time"
)

// The calibration table is bounded AT WRITE TIME (#72 rule: telemetry must
// never outgrow the corpus). These tests pin both halves of the retention
// rule — the row cap and the age-out — at the only write site.

func TestInsertGateCalibration_RowCapHolds(t *testing.T) {
	db := testDB(t)

	// Pin the trim logic with a small explicit cap (the production wrapper
	// passes GateCalibrationMaxRows through the same code path). Every insert
	// enforces the bound, so the table can never be observed above it — and
	// the survivors are the NEWEST rows.
	const cap = 50
	for i := 0; i < cap+25; i++ {
		if err := db.insertGateCalibrationBounded(GateCalibration{
			SessionID: "sess", MaxSim: float64(i),
		}, cap, GateCalibrationMaxAge); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	n, err := db.CountGateCalibration()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != cap {
		t.Errorf("rows = %d, want cap %d", n, cap)
	}
	sims, err := db.GateCalibrationMaxSims()
	if err != nil {
		t.Fatalf("sims: %v", err)
	}
	if sims[0] != 25 { // rows 0..24 trimmed; oldest survivor is row 25
		t.Errorf("oldest surviving row = %v, want 25 (newest survive, oldest trimmed)", sims[0])
	}
}

func TestInsertGateCalibration_AgesOut(t *testing.T) {
	db := testDB(t)

	old := time.Now().Add(-GateCalibrationMaxAge - time.Hour).UnixMilli()
	if err := db.InsertGateCalibration(GateCalibration{SessionID: "old", MaxSim: 0.9, CreatedAt: old}); err != nil {
		t.Fatalf("insert old: %v", err)
	}
	if err := db.InsertGateCalibration(GateCalibration{SessionID: "new", MaxSim: 0.2}); err != nil {
		t.Fatalf("insert new: %v", err)
	}
	sims, err := db.GateCalibrationMaxSims()
	if err != nil {
		t.Fatalf("sims: %v", err)
	}
	if len(sims) != 1 || sims[0] != 0.2 {
		t.Errorf("sims = %v, want only the fresh row [0.2]", sims)
	}
}

func TestGateCalibrationMaxSims_Ascending(t *testing.T) {
	db := testDB(t)
	for _, s := range []float64{0.7, 0.1, 0.4} {
		if err := db.InsertGateCalibration(GateCalibration{MaxSim: s}); err != nil {
			t.Fatal(err)
		}
	}
	sims, err := db.GateCalibrationMaxSims()
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{0.1, 0.4, 0.7}
	if fmt.Sprint(sims) != fmt.Sprint(want) {
		t.Errorf("sims = %v, want %v", sims, want)
	}
}

func TestShownURIsForSession(t *testing.T) {
	db := testDB(t)
	rows := []MemEvent{
		{NodeURI: "mem://a", Event: "shown", Surface: "tray", SessionID: "s1"},
		{NodeURI: "mem://b", Event: "shown", Surface: "index", SessionID: "s1"},
		{NodeURI: "mem://b", Event: "shown", Surface: "gate", SessionID: "s1"}, // duplicate URI
		{NodeURI: "mem://c", Event: "shown", Surface: "tray", SessionID: "s2"}, // other session
		{NodeURI: "mem://d", Event: "deepened", Surface: "", SessionID: "s1"},  // not shown
	}
	for _, e := range rows {
		if err := db.InsertEvent(e); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.ShownURIsForSession("s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got["mem://a"] || !got["mem://b"] {
		t.Errorf("shown URIs = %v, want {mem://a, mem://b}", got)
	}
	empty, err := db.ShownURIsForSession("")
	if err != nil || len(empty) != 0 {
		t.Errorf("empty session id: got %v, %v", empty, err)
	}
}
