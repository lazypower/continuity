package store

import (
	"database/sql"
	"fmt"
	"time"
)

// MetaVectorIdentity is the mem_meta key under which the corpus's declared
// vector identity ("model:dims") is stored. The active embedder is reconciled
// against this at startup so it cannot be silently switched by environment.
const MetaVectorIdentity = "vector_identity"

// GetMeta returns the value for a mem_meta key. ok is false when the key is
// absent (distinct from an empty-string value).
func (db *DB) GetMeta(key string) (value string, ok bool, err error) {
	err = db.QueryRow(`SELECT value FROM mem_meta WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get meta %q: %w", key, err)
	}
	return value, true, nil
}

// SetMeta inserts or replaces a mem_meta key/value.
func (db *DB) SetMeta(key, value string) error {
	now := time.Now().UnixMilli()
	_, err := db.Exec(`
		INSERT INTO mem_meta (key, value, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = ?
	`, key, value, now, value, now)
	if err != nil {
		return fmt.Errorf("set meta %q: %w", key, err)
	}
	return nil
}

// VectorIdentity returns the corpus's declared vector identity, or ok=false if
// none has been declared yet (a fresh or pre-identity corpus).
func (db *DB) VectorIdentity() (identity string, ok bool, err error) {
	return db.GetMeta(MetaVectorIdentity)
}

// SetVectorIdentity declares the corpus's vector identity.
func (db *DB) SetVectorIdentity(identity string) error {
	return db.SetMeta(MetaVectorIdentity, identity)
}

// VectorModelCounts returns the count of stored vectors grouped by their
// "model:dimensions" identity string. It reads only metadata columns (no
// embedding blobs), so it is cheap to call at startup for reconciliation.
func (db *DB) VectorModelCounts() (map[string]int, error) {
	rows, err := db.Query(`SELECT model, dimensions, COUNT(*) FROM mem_vectors GROUP BY model, dimensions`)
	if err != nil {
		return nil, fmt.Errorf("vector model counts: %w", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var model string
		var dims, n int
		if err := rows.Scan(&model, &dims, &n); err != nil {
			return nil, fmt.Errorf("scan vector model count: %w", err)
		}
		counts[fmt.Sprintf("%s:%d", model, dims)] = n
	}
	return counts, rows.Err()
}
