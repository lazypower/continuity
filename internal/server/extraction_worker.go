package server

import (
	"context"
	"log"
	"time"

	"github.com/lazypower/continuity/internal/store"
)

// maxExtractionAttempts bounds retries of a single queued job. It is set high on
// purpose: a transient outage (LLM down, Ollama restarting) must not drop real
// memories — only a genuinely poison job (one that fails ~every retry for hours)
// is abandoned, and loudly. Losing a memory is worse than a lingering queue row.
const maxExtractionAttempts = 20

// extractionRetryInterval is the safety-net cadence. It replays jobs a crash
// left behind and retries transient failures even when no new enqueue arrives to
// wake the worker.
const extractionRetryInterval = 30 * time.Second

// StartExtractionWorker launches the single serial drain worker. On start it
// replays any jobs a prior crash left in the queue, then processes new enqueues
// as they arrive. No-op (and immediately "done") when extraction is disabled
// (nil engine), so shutdown never blocks waiting on a worker that never ran.
func (s *Server) StartExtractionWorker() {
	if s.runJob == nil {
		close(s.extractDone)
		return
	}
	go s.extractionLoop()
}

func (s *Server) extractionLoop() {
	defer close(s.extractDone)
	ticker := time.NewTicker(extractionRetryInterval)
	defer ticker.Stop()
	for {
		s.drainExtractionQueue()
		select {
		case <-s.extractStop:
			return
		case <-s.extractWake:
		case <-ticker.C:
		}
	}
}

// drainExtractionQueue processes pending jobs FIFO until the queue empties or a
// stop is requested. Each row is deleted only on success; a failure bumps
// attempts and ends the pass so the ticker retries later (no tight spin), and a
// job that exhausts maxExtractionAttempts is dropped with a loud log.
func (s *Server) drainExtractionQueue() {
	// If the corpus vector identity is locked, extraction is DEFERRED, not failed:
	// the engine returns nil in that state (see ExtractSignal / extractSession),
	// which we must not treat as "done" — deleting the row would lose the capture.
	// Skip the whole pass; the operator repairs and the ticker retries. Guarded on
	// a live engine (tests inject runJob with a nil engine).
	if s.engine != nil {
		if locked, _ := s.engine.VectorIdentityLocked(); locked {
			log.Printf("extraction worker: vector identity locked — deferring drain (run `continuity doctor --repair-vectors`)")
			return
		}
	}

	for {
		select {
		case <-s.extractStop:
			return
		default:
		}

		job, err := s.db.NextExtraction(maxExtractionAttempts)
		if err != nil {
			log.Printf("extraction worker: read queue: %v", err)
			return
		}
		if job == nil {
			return // nothing eligible (queue empty or only parked jobs remain)
		}

		if err := s.runJob(job); err != nil {
			attempts, bumpErr := s.db.BumpExtractionAttempts(job.ID)
			if bumpErr != nil {
				log.Printf("extraction worker: %v", bumpErr)
				return
			}
			// Failure phrasing is kept kind-specific and stable: the hook E2E suite
			// treats these exact lines ("signal extraction failed for X" /
			// "extraction failed for X") as proof the route reached the extractor.
			if job.Kind == "signal" {
				log.Printf("signal extraction failed for %s (attempt %d/%d): %v",
					job.SessionID, attempts, maxExtractionAttempts, err)
			} else {
				log.Printf("extraction failed for %s (attempt %d/%d): %v",
					job.SessionID, attempts, maxExtractionAttempts, err)
			}
			if attempts >= maxExtractionAttempts {
				// PARK, never delete: the row stays (NextExtraction excludes it) so
				// the capture is not silently lost — it surfaces in /api/health's
				// queue depth and can be retried once the cause is fixed.
				log.Printf("extraction worker: PARKING %s job for %s after %d failed attempts — "+
					"kept in queue for inspection/retry, NOT captured", job.Kind, job.SessionID, attempts)
			}
			// End the pass rather than spin; the ticker/wake retries eligible jobs.
			return
		}

		if err := s.db.DeleteExtraction(job.ID); err != nil {
			log.Printf("extraction worker: delete completed job %d: %v", job.ID, err)
			return // avoid reprocessing the same row in a loop
		}
	}
}

func (s *Server) runExtractionJob(job *store.ExtractionJob) error {
	switch job.Kind {
	case "session":
		if job.Force {
			return s.engine.ExtractSessionForce(job.SessionID, job.Payload)
		}
		return s.engine.ExtractSession(job.SessionID, job.Payload)
	case "signal":
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		return s.engine.ExtractSignal(ctx, job.SessionID, job.Payload)
	case "relational":
		// Relational-only job (#78): enqueued by handleExtractSession while
		// autoExtract is off, so the profile keeps learning without the memory
		// pipeline. Runs no memory extraction and never marks the session extracted.
		//
		// Honor the kill switch at execution time too, not just at enqueue: a job
		// queued before CONTINUITY_RELATIONAL_AUTO=false took effect must not
		// replay after a restart and write to a profile the operator froze.
		// Dropping (nil ⇒ deleted) is the freeze doing its job, not data loss.
		if !s.relationalAuto {
			log.Printf("extraction worker: dropping queued relational job for %s — relational auto is disabled", job.SessionID)
			return nil
		}
		return s.engine.ExtractRelational(job.SessionID, job.Payload)
	default:
		// Unknown kind: drop it (nil error ⇒ deleted) rather than retry forever.
		log.Printf("extraction worker: unknown job kind %q (job %d) — dropping", job.Kind, job.ID)
		return nil
	}
}

// wakeExtractionWorker nudges the worker to drain now. Non-blocking: a pending
// wake already covers an in-flight drain.
func (s *Server) wakeExtractionWorker() {
	select {
	case s.extractWake <- struct{}{}:
	default:
	}
}

// StopExtractionWorker signals the worker to stop and waits up to timeout for it
// to finish its current job. Abandonment is safe: an unfinished job's row stays
// in the queue and replays on the next boot, so a slow shutdown never loses it.
func (s *Server) StopExtractionWorker(timeout time.Duration) {
	s.extractStopOnce.Do(func() { close(s.extractStop) })
	select {
	case <-s.extractDone:
	case <-time.After(timeout):
		log.Printf("extraction worker: drain timed out after %s; unfinished job (if any) replays next boot", timeout)
	}
}
