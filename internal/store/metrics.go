package store

import (
	"sort"
	"time"
)

// Metrics tuning constants. These define the freshness bands and list sizes for
// the Memory Health dashboard. Single-user scale — tune freely.
const (
	// Relevance bands. A node's *effective* relevance is computed live from
	// timestamps (read-only) rather than read from the stored column, so the
	// dashboard never depends on a decay sweep having run.
	freshThreshold = 0.7 // effective >= this  → fresh
	staleThreshold = 0.4 // effective <  this  → stale
	cliffCeiling   = 0.5 // [staleThreshold, cliffCeiling) → near the decay cliff

	metricsRecentDays  = 30 // window for "recent" retractions
	neverRetrievedDays = 30 // never-retrieved + older than this → needs attention
	metricsTopN        = 10 // list sizes for needs-attention / critical sections

	histogramBins  = 18   // relevance distribution bins across [0.1, 1.0]
	histogramFloor = 0.1  // relevance floor (matches decay floor)
	histogramWidth = 0.05 // (1.0 - 0.1) / 18

	dayMs = int64(24 * 60 * 60 * 1000)
)

// HistBin is one bucket of the relevance distribution. Lo/Hi are the band edges;
// Count is how many active memories fall in it. Drives the D3 decay histogram.
type HistBin struct {
	Lo    float64 `json:"lo"`
	Hi    float64 `json:"hi"`
	Count int     `json:"count"`
}

// MetricNode is a memory surfaced in a dashboard list, carrying the live
// effective relevance (not the stored column) plus the signals a human needs to
// decide whether it deserves attention.
type MetricNode struct {
	URI         string  `json:"uri"`
	Category    string  `json:"category"`
	L0Abstract  string  `json:"l0_abstract"`
	Relevance   float64 `json:"relevance"` // live effective relevance, floored at 0.1
	AccessCount int     `json:"access_count"`
	LastAccess  *int64  `json:"last_access,omitempty"`
	CreatedAt   int64   `json:"created_at"`
	AgeDays     int     `json:"age_days"`
}

// CategoryShare is one category's slice of the active memory base. Sorted desc
// by count, it doubles as the by-category breakdown and the imbalance signal.
type CategoryShare struct {
	Category string  `json:"category"`
	Count    int     `json:"count"`
	Share    float64 `json:"share"` // count / active_total, 0..1
}

// Metrics is the full read-only payload for the Memory Health dashboard.
// Computed live; viewing it never mutates the store.
type Metrics struct {
	GeneratedAt int64 `json:"generated_at"` // unix ms

	Summary struct {
		ActiveTotal       int     `json:"active_total"`
		RetractedTotal    int     `json:"retracted_total"`
		Fresh             int     `json:"fresh"`
		Fading            int     `json:"fading"`
		Stale             int     `json:"stale"`
		NeverRetrieved    int     `json:"never_retrieved"`
		RetractionRate    float64 `json:"retraction_rate"`    // retracted / (active+retracted)
		RecentRetractions int     `json:"recent_retractions"` // tombstoned within metricsRecentDays
	} `json:"summary"`

	Categories []CategoryShare `json:"categories"`

	// Relevance distribution across active memories (fine-grained bins), for the
	// D3 decay histogram. Bands: stale < 0.4, fading < 0.7, fresh >= 0.7.
	Histogram []HistBin `json:"histogram"`

	NeedsAttention struct {
		// Load-bearing but decaying: frequently retrieved yet effective relevance
		// has fallen below fresh. Fix these first.
		StaleHighRetrieval []MetricNode `json:"stale_high_retrieval"`
		// Created but never retrieved, older than neverRetrievedDays. Noise or buried value.
		NeverRetrievedOld []MetricNode `json:"never_retrieved_old"`
		// Approaching the stale threshold but not yet stale — act before they fade.
		NearDecayCliff []MetricNode `json:"near_decay_cliff"`
		// Retracted without a superseded_by successor — untidy cleanup.
		OrphanedTombstones []MetricNode `json:"orphaned_tombstones"`
	} `json:"needs_attention"`

	// Most-retrieved memories, shown with live relevance + last access (not bare count).
	Critical []MetricNode `json:"critical"`

	// Trend over the last metricsDailyWindow days — powers the effectiveness
	// sparkline. Captures are real now; retrievals/buckets accrue from snapshots.
	Daily []DailyPoint `json:"daily"`
}

// ComputeMetrics builds the Memory Health payload in a single pass over all
// leaf nodes. Decay is computed live from timestamps (read-only) — this method
// never writes, so opening the dashboard cannot change what it measures.
func (db *DB) ComputeMetrics() (*Metrics, error) {
	leaves, err := db.ListLeavesIncludingRetracted()
	if err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	m := &Metrics{GeneratedAt: now}

	catCounts := map[string]int{}
	histCounts := make([]int, histogramBins)
	var active, neverRetrieved []MetricNode

	for i := range leaves {
		n := &leaves[i]

		if n.IsRetracted() {
			m.Summary.RetractedTotal++
			if n.TombstonedAt != nil && now-*n.TombstonedAt <= int64(metricsRecentDays)*dayMs {
				m.Summary.RecentRetractions++
			}
			if n.SupersededBy == "" {
				m.NeedsAttention.OrphanedTombstones = append(m.NeedsAttention.OrphanedTombstones, toMetricNode(n, now))
			}
			continue
		}

		// Live (active) node.
		m.Summary.ActiveTotal++
		catCounts[n.Category]++
		eff := effectiveRelevance(n, now)
		mn := toMetricNode(n, now)
		mn.Relevance = eff
		active = append(active, mn)

		bin := int((eff - histogramFloor) / histogramWidth)
		if bin < 0 {
			bin = 0
		}
		if bin >= histogramBins {
			bin = histogramBins - 1
		}
		histCounts[bin]++

		switch {
		case eff >= freshThreshold:
			m.Summary.Fresh++
		case eff < staleThreshold:
			m.Summary.Stale++
		default:
			m.Summary.Fading++
		}

		if eff >= staleThreshold && eff < cliffCeiling {
			m.NeedsAttention.NearDecayCliff = append(m.NeedsAttention.NearDecayCliff, mn)
		}

		if n.AccessCount == 0 {
			m.Summary.NeverRetrieved++
			if now-n.CreatedAt >= int64(neverRetrievedDays)*dayMs {
				neverRetrieved = append(neverRetrieved, mn)
			}
		} else if eff < freshThreshold {
			// Retrieved at least once but no longer fresh — load-bearing and decaying.
			m.NeedsAttention.StaleHighRetrieval = append(m.NeedsAttention.StaleHighRetrieval, mn)
		}
	}

	// Retraction rate over the lifetime of the base.
	if total := m.Summary.ActiveTotal + m.Summary.RetractedTotal; total > 0 {
		m.Summary.RetractionRate = float64(m.Summary.RetractedTotal) / float64(total)
	}

	// Category shares, sorted desc by count (doubles as the imbalance signal).
	m.Categories = make([]CategoryShare, 0, len(catCounts))
	for cat, c := range catCounts {
		share := 0.0
		if m.Summary.ActiveTotal > 0 {
			share = float64(c) / float64(m.Summary.ActiveTotal)
		}
		m.Categories = append(m.Categories, CategoryShare{Category: cat, Count: c, Share: share})
	}
	sort.Slice(m.Categories, func(i, j int) bool {
		if m.Categories[i].Count != m.Categories[j].Count {
			return m.Categories[i].Count > m.Categories[j].Count
		}
		return m.Categories[i].Category < m.Categories[j].Category
	})

	// Relevance histogram bins for the D3 decay distribution.
	m.Histogram = make([]HistBin, histogramBins)
	for i := 0; i < histogramBins; i++ {
		lo := histogramFloor + float64(i)*histogramWidth
		m.Histogram[i] = HistBin{Lo: lo, Hi: lo + histogramWidth, Count: histCounts[i]}
	}

	// Critical: most-retrieved active memories, then by live relevance as tiebreak.
	// Exclude session-injected categories (moments, session): they ride into every
	// session, so their access_count measures injection frequency, not how
	// load-bearing the knowledge is. Critical should surface working knowledge.
	m.Critical = make([]MetricNode, 0, len(active))
	for _, mn := range active {
		if mn.Category == "moments" || mn.Category == "session" {
			continue
		}
		m.Critical = append(m.Critical, mn)
	}
	sort.Slice(m.Critical, func(i, j int) bool {
		if m.Critical[i].AccessCount != m.Critical[j].AccessCount {
			return m.Critical[i].AccessCount > m.Critical[j].AccessCount
		}
		return m.Critical[i].Relevance > m.Critical[j].Relevance
	})
	m.Critical = topMetricNodes(m.Critical, metricsTopN)

	// Order the needs-attention lists by urgency, then cap them. Sort each source
	// slice directly before assigning back.
	na := &m.NeedsAttention
	sort.Slice(na.StaleHighRetrieval, func(i, j int) bool {
		return na.StaleHighRetrieval[i].AccessCount > na.StaleHighRetrieval[j].AccessCount
	})
	sort.Slice(neverRetrieved, func(i, j int) bool {
		return neverRetrieved[i].AgeDays > neverRetrieved[j].AgeDays
	})
	sort.Slice(na.NearDecayCliff, func(i, j int) bool {
		return na.NearDecayCliff[i].Relevance < na.NearDecayCliff[j].Relevance
	})
	sort.Slice(na.OrphanedTombstones, func(i, j int) bool {
		return na.OrphanedTombstones[i].AgeDays > na.OrphanedTombstones[j].AgeDays
	})

	na.NeverRetrievedOld = topMetricNodes(neverRetrieved, metricsTopN)
	na.StaleHighRetrieval = topMetricNodes(na.StaleHighRetrieval, metricsTopN)
	na.NearDecayCliff = topMetricNodes(na.NearDecayCliff, metricsTopN)
	na.OrphanedTombstones = topMetricNodes(na.OrphanedTombstones, metricsTopN)

	daily, err := db.BuildDailySeries(metricsDailyWindow)
	if err != nil {
		return nil, err
	}
	m.Daily = daily

	return m, nil
}

// effectiveRelevance computes a node's current relevance live from its
// timestamps using the same 90-day half-life as DecayAllNodes, floored at 0.1.
// Decay-exempt nodes (relational profile, moments) keep their stored relevance.
func effectiveRelevance(n *MemNode, now int64) float64 {
	if n.URI == "mem://user/profile/communication" || n.Category == "moments" {
		return n.Relevance
	}
	refTime := n.CreatedAt
	if n.LastAccess != nil {
		refTime = *n.LastAccess
	}
	elapsed := float64(now - refTime)
	if elapsed <= 0 {
		return 1.0
	}
	halfLifeMs := float64(90 * 24 * 60 * 60 * 1000)
	v := pow05(elapsed / halfLifeMs)
	if v < 0.1 {
		return 0.1
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}

func toMetricNode(n *MemNode, now int64) MetricNode {
	return MetricNode{
		URI:         n.URI,
		Category:    n.Category,
		L0Abstract:  n.L0Abstract,
		Relevance:   n.Relevance,
		AccessCount: n.AccessCount,
		LastAccess:  n.LastAccess,
		CreatedAt:   n.CreatedAt,
		AgeDays:     int((now - n.CreatedAt) / dayMs),
	}
}

func topMetricNodes(xs []MetricNode, n int) []MetricNode {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}
