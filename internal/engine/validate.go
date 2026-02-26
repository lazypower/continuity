package engine

import (
	"fmt"
	"log"
	"strings"
	"unicode"
)

// Content size limits (approximate token → char conversion: 1 token ≈ 4 chars).
const (
	maxL0Chars = 800   // ~200 tokens
	maxL1Chars = 12000 // ~3K tokens
	maxL2Chars = 40000 // ~10K tokens
	minL1Chars = 20
)

// validURIHintChar returns true if the character is allowed in a URI hint.
// Allowed: lowercase alphanumeric, hyphens, underscores.
func validURIHintChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_'
}

// sanitizeURIHint normalizes a URI hint to [a-z0-9_-].
// Uppercases become lowercase, spaces/dots become hyphens, invalid chars are dropped.
// Returns empty string if the result is empty after sanitization.
func sanitizeURIHint(hint string) string {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return ""
	}

	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(hint) {
		if validURIHintChar(r) {
			b.WriteRune(r)
			prevHyphen = (r == '-')
		} else if r == ' ' || r == '.' || r == '/' {
			// Collapse separators to single hyphen
			if !prevHyphen && b.Len() > 0 {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
		// Other chars silently dropped
	}

	result := strings.Trim(b.String(), "-_")
	return result
}

// validateCandidate checks a memory candidate for obvious garbage.
// Returns a sanitized copy and an error if the candidate should be rejected.
func validateCandidate(c memoryCandidate) (memoryCandidate, error) {
	// Category must be valid
	if !validCategories[c.Category] {
		return c, fmt.Errorf("invalid category %q", c.Category)
	}

	// Sanitize and validate URI hint
	c.URIHint = sanitizeURIHint(c.URIHint)
	if c.URIHint == "" {
		return c, fmt.Errorf("empty URI hint after sanitization")
	}

	// L0 is required
	c.L0 = strings.TrimSpace(c.L0)
	if c.L0 == "" {
		return c, fmt.Errorf("empty L0 abstract")
	}

	// Trim all content tiers
	c.L1 = strings.TrimSpace(c.L1)
	c.L2 = strings.TrimSpace(c.L2)

	// L1 must be non-trivial (it's the primary context injection content)
	if len(c.L1) < minL1Chars {
		return c, fmt.Errorf("L1 too short (%d chars, min %d)", len(c.L1), minL1Chars)
	}

	// Size ceilings — truncate rather than reject, but log it
	if len(c.L0) > maxL0Chars {
		log.Printf("validate: truncating L0 for %s (%d → %d chars)", c.URIHint, len(c.L0), maxL0Chars)
		c.L0 = truncateClean(c.L0, maxL0Chars)
	}
	if len(c.L1) > maxL1Chars {
		log.Printf("validate: truncating L1 for %s (%d → %d chars)", c.URIHint, len(c.L1), maxL1Chars)
		c.L1 = truncateClean(c.L1, maxL1Chars)
	}
	if len(c.L2) > maxL2Chars {
		log.Printf("validate: truncating L2 for %s (%d → %d chars)", c.URIHint, len(c.L2), maxL2Chars)
		c.L2 = truncateClean(c.L2, maxL2Chars)
	}

	return c, nil
}

// truncateClean truncates a string to maxLen, cutting at the last word boundary
// to avoid mid-word breaks.
func truncateClean(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	// Back up to last space
	truncated := s[:maxLen]
	if idx := strings.LastIndexFunc(truncated, unicode.IsSpace); idx > maxLen-200 {
		truncated = truncated[:idx]
	}
	return strings.TrimSpace(truncated)
}
