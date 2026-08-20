package server

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lazypower/continuity/internal/store"
)

// eventRecorder decouples surfacing telemetry from read paths.
//
// ADR-001 §5: SQLite is single-writer, so a `shown` insert queued behind an
// extraction write could spend a synchronous surface's latency budget. Writes
// are therefore buffered and fire-and-forget: a full buffer DROPS the event
// and counts the drop. Telemetry is allowed to lose an event; the surfacing
// that triggered it is never allowed to wait for one.
//
// Shutdown uses a stop signal rather than closing the buffer channel: record()
// may race Close() (a late request during shutdown), and a send on a closed
// channel panics. With the stop pattern the late event is simply dropped —
// which is the telemetry contract anyway.
// The channel carries both journal rows (store.MemEvent) and gate calibration
// rows (store.GateCalibration, #80) — two tables, one write contract: buffered,
// fire-and-forget, droppable. Giving calibration its own goroutine would
// duplicate the shutdown/drop machinery without changing any guarantee.
type eventRecorder struct {
	db       *store.DB
	ch       chan any
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	dropped  atomic.Int64
}

const eventBuffer = 256

func newEventRecorder(db *store.DB) *eventRecorder {
	r := &eventRecorder{
		db:   db,
		ch:   make(chan any, eventBuffer),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go r.drain()
	return r
}

func (r *eventRecorder) drain() {
	defer close(r.done)
	for {
		select {
		case e := <-r.ch:
			r.insert(e)
		case <-r.stop:
			// Flush whatever is already buffered, then exit.
			for {
				select {
				case e := <-r.ch:
					r.insert(e)
				default:
					return
				}
			}
		}
	}
}

func (r *eventRecorder) insert(item any) {
	switch e := item.(type) {
	case store.MemEvent:
		if err := r.db.InsertEvent(e); err != nil {
			// Best-effort by contract: log and move on. A persistent failure
			// here (e.g. schema drift) surfaces in doctor, not in surfacing.
			log.Printf("events: write failed for %s/%s %s: %v", e.Event, e.Surface, e.NodeURI, err)
		}
	case store.GateCalibration:
		// InsertGateCalibration enforces the #72 retention bound in the same
		// call, so every drained write leaves the table bounded.
		if err := r.db.InsertGateCalibration(e); err != nil {
			log.Printf("events: gate calibration write failed: %v", err)
		}
	}
}

// enqueue pushes one telemetry item without blocking. Full buffer or shutdown = drop.
func (r *eventRecorder) enqueue(item any) {
	select {
	case <-r.stop:
		return // shutting down; a late telemetry event is droppable by contract
	default:
	}
	select {
	case r.ch <- item:
	default:
		n := r.dropped.Add(1)
		if n == 1 || n%100 == 0 {
			log.Printf("events: buffer full — %d telemetry event(s) dropped so far (by contract, not by accident)", n)
		}
	}
}

// record enqueues one journal event without blocking.
func (r *eventRecorder) record(event, surface, uri, sessionID string) {
	r.enqueue(store.MemEvent{
		NodeURI:   uri,
		Event:     event,
		Surface:   surface,
		SessionID: sessionID,
		CreatedAt: time.Now().UnixMilli(),
	})
}

// recordCalibration enqueues one gate calibration row (#80). Same droppable
// contract as record: the prompt never waits for its own telemetry.
func (r *eventRecorder) recordCalibration(e store.GateCalibration) {
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().UnixMilli()
	}
	r.enqueue(e)
}

// Close stops accepting events and flushes what's buffered, bounded by a
// timeout — telemetry is droppable, so shutdown never hangs on it.
// Idempotent; tests also use it as a deterministic drain barrier.
func (r *eventRecorder) Close() {
	r.stopOnce.Do(func() { close(r.stop) })
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		log.Printf("events: shutdown flush timed out — remaining buffered events dropped")
	}
}
