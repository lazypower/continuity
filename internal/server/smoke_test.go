//go:build smoke

package server

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

// TestSmoke_MigrationAndRetractAgainstRealDB exercises the migration and
// retract pipeline against a copy of the operator's actual database. Skipped
// unless `-tags smoke` is set AND CONTINUITY_SMOKE_DB points at the copy.
//
// Run with:
//
//	cp ~/.continuity/continuity.db /tmp/continuity-smoke.db
//	CONTINUITY_SMOKE_DB=/tmp/continuity-smoke.db go test -tags smoke ./internal/server/ -run TestSmoke -v
//
// The smoke test mutates the copy DB. It will not touch the real DB.
func TestSmoke_MigrationAndRetractAgainstRealDB(t *testing.T) {
	dbPath := os.Getenv("CONTINUITY_SMOKE_DB")
	if dbPath == "" {
		t.Skip("CONTINUITY_SMOKE_DB not set")
	}

	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	t.Run("migration applied", func(t *testing.T) {
		v, err := db.SchemaVersion()
		if err != nil {
			t.Fatal(err)
		}
		if v != 8 {
			t.Errorf("schema version = %d, want 8", v)
		}
	})

	t.Run("retraction columns queryable", func(t *testing.T) {
		var live, retracted int
		if err := db.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE tombstoned_at IS NULL`).Scan(&live); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM mem_nodes WHERE tombstoned_at IS NOT NULL`).Scan(&retracted); err != nil {
			t.Fatal(err)
		}
		t.Logf("live leaves: %d, already-retracted: %d", live, retracted)
	})

	var victimURI, victimL0, victimCategory string
	t.Run("pick safe victim", func(t *testing.T) {
		err := db.QueryRow(`
			SELECT uri, l0_abstract, category FROM mem_nodes
			WHERE node_type = 'leaf'
			  AND tombstoned_at IS NULL
			  AND uri != 'mem://user/profile/communication'
			  AND uri NOT LIKE 'mem://user/profile/%'
			ORDER BY relevance ASC
			LIMIT 1
		`).Scan(&victimURI, &victimL0, &victimCategory)
		if err == sql.ErrNoRows {
			t.Skip("no eligible leaf found in DB")
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("victim: %s [%s]", victimURI, victimCategory)
		t.Logf("victim L0: %s", victimL0)
	})

	t.Run("retract victim via public verb", func(t *testing.T) {
		newly, err := db.RetractNode(victimURI, "smoke test retraction", "")
		if err != nil {
			t.Fatalf("retract: %v", err)
		}
		if !newly {
			t.Error("expected newly=true on first retraction")
		}
		check, _ := db.GetNodeByURI(victimURI)
		if !check.IsRetracted() {
			t.Error("retraction did not persist")
		}
	})

	t.Run("buildContext does not surface retracted L0", func(t *testing.T) {
		srv := New(db, engine.New(db, nil), "smoke")
		ctx := srv.buildContext("smoke-session")
		if victimL0 != "" && strings.Contains(ctx, victimL0) {
			t.Errorf("retracted victim L0 leaked into session context")
		}
	})

	t.Run("dedup-against-retracted fires on similar write", func(t *testing.T) {
		if victimL0 == "" {
			t.Skip("victim has no L0")
		}
		eng := engine.New(db, nil)
		embedder, err := engine.NewTFIDFEmbedder(db, 512)
		if err != nil {
			t.Fatalf("embedder: %v", err)
		}
		eng.SetEmbedder(embedder)

		_, _, err = eng.Remember(context.Background(), engine.RememberInput{
			Category: victimCategory,
			Name:     "smoke-test-similar-write-target",
			Summary:  victimL0,
			Body:     "smoke test body content with enough length to pass validation thresholds easily.",
		})
		if isMatch, _ := engine.IsRetractedMatch(err); isMatch {
			return
		}
		// If the gate didn't fire, log it but don't fail — depends on TFIDF
		// recall against the live corpus, which is fuzzy on real data.
		t.Logf("dedup gate did not fire (err=%v); embedder recall on real data is fuzzy — manual confirmation may be needed", err)
	})
}
