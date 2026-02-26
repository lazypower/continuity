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

// extractRelational runs the relational profiling pipeline.
// It extracts how the user works, communicates, and gives feedback.
func extractRelational(db *store.DB, client llm.Client, sessionID, transcriptPath string) error {
	entries, err := transcript.ParseFile(transcriptPath)
	if err != nil {
		return err
	}

	if transcript.CountUserMessages(entries) < 3 {
		return nil
	}

	condensed := transcript.Condense(entries)
	if len(condensed) < 100 {
		return nil
	}

	// Get existing relational profile
	existing := ""
	node, err := db.GetNodeByURI(relationalURI)
	if err != nil {
		return err
	}
	if node != nil {
		existing = node.L1Overview
		// Check if this session was already processed (dedup)
		if node.SourceSession == sessionID {
			log.Printf("relational: skipping %s — already processed", sessionID)
			return nil
		}
	}

	prompt := llm.RelationalPrompt(existing, condensed)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	resp, err := client.Complete(ctx, prompt)
	if err != nil {
		return err
	}

	content := strings.TrimSpace(resp.Content)

	// No update signal
	if content == "NO_UPDATE" || len(content) < 20 {
		log.Printf("relational: no update for %s", sessionID)
		return nil
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

	if err := db.UpsertNode(profileNode); err != nil {
		return err
	}

	log.Printf("relational: updated profile from session %s", sessionID)
	return nil
}
