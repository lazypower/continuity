package store

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// VectorRecord holds an embedding for a mem_node.
type VectorRecord struct {
	NodeID     int64
	Embedding  []float64
	Model      string
	Dimensions int
	CreatedAt  int64
}

// encodeEmbedding converts a []float64 to a binary BLOB (8 bytes per float64).
func encodeEmbedding(vec []float64) []byte {
	buf := make([]byte, len(vec)*8)
	for i, v := range vec {
		binary.LittleEndian.PutUint64(buf[i*8:], math.Float64bits(v))
	}
	return buf
}

// decodeEmbedding converts a binary BLOB back to []float64.
func decodeEmbedding(buf []byte) []float64 {
	n := len(buf) / 8
	vec := make([]float64, n)
	for i := 0; i < n; i++ {
		vec[i] = math.Float64frombits(binary.LittleEndian.Uint64(buf[i*8:]))
	}
	return vec
}

// SaveVector stores or replaces the embedding for a node.
func (db *DB) SaveVector(nodeID int64, embedding []float64, model string) error {
	now := time.Now().UnixMilli()
	blob := encodeEmbedding(embedding)

	_, err := db.Exec(`
		INSERT INTO mem_vectors (node_id, embedding, model, dimensions, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(node_id) DO UPDATE SET embedding = ?, model = ?, dimensions = ?, created_at = ?
	`, nodeID, blob, model, len(embedding), now,
		blob, model, len(embedding), now)
	if err != nil {
		return fmt.Errorf("save vector: %w", err)
	}
	return nil
}

// GetVector returns the embedding for a node, or nil if not found.
func (db *DB) GetVector(nodeID int64) (*VectorRecord, error) {
	var v VectorRecord
	var blob []byte

	err := db.QueryRow(`
		SELECT node_id, embedding, model, dimensions, created_at
		FROM mem_vectors WHERE node_id = ?
	`, nodeID).Scan(&v.NodeID, &blob, &v.Model, &v.Dimensions, &v.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get vector: %w", err)
	}
	v.Embedding = decodeEmbedding(blob)
	return &v, nil
}

// AllVectors returns all stored vector records.
func (db *DB) AllVectors() ([]VectorRecord, error) {
	rows, err := db.Query(`
		SELECT node_id, embedding, model, dimensions, created_at
		FROM mem_vectors
	`)
	if err != nil {
		return nil, fmt.Errorf("all vectors: %w", err)
	}
	defer rows.Close()

	var records []VectorRecord
	for rows.Next() {
		var v VectorRecord
		var blob []byte
		if err := rows.Scan(&v.NodeID, &blob, &v.Model, &v.Dimensions, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan vector: %w", err)
		}
		v.Embedding = decodeEmbedding(blob)
		records = append(records, v)
	}
	return records, rows.Err()
}

// DeleteVector removes the embedding for a node.
func (db *DB) DeleteVector(nodeID int64) error {
	_, err := db.Exec("DELETE FROM mem_vectors WHERE node_id = ?", nodeID)
	if err != nil {
		return fmt.Errorf("delete vector: %w", err)
	}
	return nil
}
