package server

import (
	"log"
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
type eventRecorder struct {
	db      *store.DB
	ch      chan store.MemEvent
	done    chan struct{}
	dropped atomic.Int64
}

const eventBuffer = 256

func newEventRecorder(db *store.DB) *eventRecorder {
	r := &eventRecorder{
		db:   db,
		ch:   make(chan store.MemEvent, eventBuffer),
		done: make(chan struct{}),
	}
	go r.drain()
	return r
}

func (r *eventRecorder) drain() {
	defer close(r.done)
	for e := range r.ch {
		if err := r.db.InsertEvent(e); err != nil {
			// Best-effort by contract: log and move on. A persistent failure
			// here (e.g. schema drift) surfaces in doctor, not in surfacing.
			log.Printf("events: write failed for %s/%s %s: %v", e.Event, e.Surface, e.NodeURI, err)
		}
	}
}

// record enqueues one event without blocking. Full buffer = drop + count.
func (r *eventRecorder) record(event, surface, uri, sessionID string) {
	e := store.MemEvent{
		NodeURI:   uri,
		Event:     event,
		Surface:   surface,
		SessionID: sessionID,
		CreatedAt: time.Now().UnixMilli(),
	}
	select {
	case r.ch <- e:
	default:
		n := r.dropped.Add(1)
		if n == 1 || n%100 == 0 {
			log.Printf("events: buffer full — %d telemetry event(s) dropped so far (by contract, not by accident)", n)
		}
	}
}

// Close stops accepting events and flushes what's buffered, bounded by a
// timeout — telemetry is droppable, so shutdown never hangs on it.
func (r *eventRecorder) Close() {
	close(r.ch)
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		log.Printf("events: shutdown flush timed out — remaining buffered events dropped")
	}
}
