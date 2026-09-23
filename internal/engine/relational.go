package engine

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/llm"
	"github.com/lazypower/continuity/internal/store"
	"github.com/lazypower/continuity/internal/transcript"
)

const relationalURI = "mem://user/profile/communication"

// maxRelationalChars caps the stored relational profile.
// This is separate from maxL1Chars because the relational profile is a structured
// document with 4 sections, not a typical memory overview. The output budget in
// buildContext further caps what gets injected into session context.
const maxRelationalChars = 1200

// extractRelational merges a session's new relational evidence into the
// profile. It is incremental (#83): the session row carries a high-water mark,
// the uuid of the last transcript entry already merged or deliberately
// rejected, and each run sends only the entries after it. A long session keeps
// contributing as it grows, and nothing is merged twice.
//
// The mark advances only when the LLM's answer was applied or deliberately
// rejected. A transport error leaves it in place and the durable job retries.
// A crash between the profile write and the mark update re-merges that one
// delta on retry; the relational merge is built to absorb a restatement
// (NO_UPDATE), and the exposure is one delta, once.
func extractRelational(db *store.DB, client llm.Client, sessionID, transcriptPath string) error {
	entries, err := transcript.ParseFile(transcriptPath)
	if err != nil {
		return err
	}

	// The session must be substantive enough to say anything about how the
	// user works. The gate is on the whole session, so the last turns of a
	// session that already passed it still merge.
	if transcript.CountUserMessages(entries) < 3 {
		return nil
	}

	mark, found, err := db.RelationalMark(sessionID)
	if err != nil {
		return err
	}
	if !found {
		// No session row, so no place to record progress: merging would repeat
		// the whole transcript on every Stop. Such sessions never ran
		// SessionStart through continuity.
		log.Printf("relational: skipping %s — no session row to record progress against", sessionID)
		return nil
	}

	node, err := db.GetNodeByURI(relationalURI)
	if err != nil {
		return err
	}
	existing := ""
	if node != nil {
		existing = node.L1Overview
	}

	start := 0
	switch {
	case !mark.Valid:
		// The session predates the mark, so its merge history is unknown. If it
		// already wrote the profile, its transcript was merged: record the end
		// rather than replay it. New sessions start at '' and never take this
		// branch, so a failed mark write after their first merge cannot skip
		// the turns that follow.
		if node != nil && node.SourceSession == sessionID {
			if last := lastUUID(entries); last != "" {
				log.Printf("relational: %s already merged before marks existed — marking to end", sessionID)
				return db.SetRelationalMark(sessionID, last)
			}
		}
	case mark.String != "":
		if i := indexOfUUID(entries, mark.String); i >= 0 {
			start = i + 1
		} else {
			log.Printf("relational: mark for %s not found in transcript — merging it from the start", sessionID)
		}
	}

	// The delta ends at the last entry with a uuid, the only kind of entry the
	// mark can point at. Entries after it wait for a later entry that carries
	// one, instead of being re-sent on every run.
	delta := entries[start:]
	end := -1
	for i := len(delta) - 1; i >= 0; i-- {
		if delta[i].UUID != "" {
			end = i
			break
		}
	}
	if end < 0 {
		return nil
	}
	delta = delta[:end+1]
	if transcript.CountUserMessages(delta) == 0 {
		return nil
	}
	newMark := delta[end].UUID
	condensed := transcript.Condense(delta)

	prompt := llm.RelationalPrompt(existing, condensed)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	resp, err := client.Complete(ctx, prompt)
	if err != nil {
		return err
	}

	content := strings.TrimSpace(resp.Content)

	// No update signal — catch both exact match and embedded in a longer response
	if strings.Contains(content, "NO_UPDATE") {
		log.Printf("relational: no update for %s", sessionID)
		return markMerged(db, sessionID, newMark)
	}
	if len(content) < 20 {
		log.Printf("relational: response too short for %s (%d chars)", sessionID, len(content))
		return markMerged(db, sessionID, newMark)
	}

	// Reject meta-descriptions — if it reads like commentary about the profile
	// rather than the profile itself, it's not useful
	metaPhrases := []string{
		"this transcript reinforces",
		"the interaction confirms",
		"no new signal",
		"already captured in the profile",
		"introduces no new",
	}
	contentLower := strings.ToLower(content)
	for _, phrase := range metaPhrases {
		if strings.Contains(contentLower, phrase) {
			log.Printf("relational: rejecting meta-description for %s", sessionID)
			return markMerged(db, sessionID, newMark)
		}
	}

	// Regression guard: reject absurdly short content that would clobber a richer profile
	if len(content) < 50 {
		log.Printf("relational: rejecting update for %s — content too short (%d chars)", sessionID, len(content))
		return markMerged(db, sessionID, newMark)
	}

	// Size ceiling: truncate if unreasonably large
	if len(content) > maxRelationalChars {
		log.Printf("relational: truncating profile content (%d → %d chars)", len(content), maxRelationalChars)
		content = truncateClean(content, maxRelationalChars)
	}

	// Upsert the relational profile node
	profileNode := &store.MemNode{
		URI:           relationalURI,
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Relational profile: communication style, feedback patterns, working dynamic",
		L1Overview:    content,
		L2Content:     content,
		SourceSession: sessionID,
	}

	// No retraction-resurrection gate here, by construction: relational only ever
	// writes the single fixed relationalURI (mem://user/profile/communication),
	// which is system-owned and un-retractable (store.systemOwnedURIs; see
	// TestRetractNode_RefusesSystemOwnedURIs). So this write can never overwrite a
	// tombstone — and UpsertNode now refuses retracted targets atomically anyway
	// (ErrRetractedTarget). It also never creates arbitrary nodes, and its L0 is a
	// constant, so there is no per-candidate content for the L0-based gate to act on.
	if err := db.UpsertNode(profileNode); err != nil {
		return err
	}

	log.Printf("relational: updated profile from session %s", sessionID)
	return markMerged(db, sessionID, newMark)
}

// markMerged records that the session's evidence through mark has been merged
// or deliberately rejected.
func markMerged(db *store.DB, sessionID, mark string) error {
	return db.SetRelationalMark(sessionID, mark)
}

// lastUUID returns the uuid of the last entry that has one.
func lastUUID(entries []transcript.ParsedEntry) string {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].UUID != "" {
			return entries[i].UUID
		}
	}
	return ""
}

// indexOfUUID returns the index of the first entry with the given uuid, or -1.
// Searching from the start means a reused uuid can only make a run re-merge
// entries, never skip them.
func indexOfUUID(entries []transcript.ParsedEntry, uuid string) int {
	for i := range entries {
		if entries[i].UUID == uuid {
			return i
		}
	}
	return -1
}
