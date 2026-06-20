package engine

import (
	"errors"
	"fmt"
)

// ValidationError signals that a write was rejected because of bad user/agent
// input — an invalid category, a missing required field, a malformed URI, a
// content tier outside its size bounds, and so on. These messages are safe to
// surface verbatim to the client: they describe the caller's own input, not
// internal state (SQL, filesystem paths, etc.).
//
// Internal failures (DB errors, embed failures, upsert collisions) are NOT
// wrapped in ValidationError and stay generic at the HTTP boundary.
//
// Mirrors the RetractedMatchError pattern: a typed error the boundary layer
// classifies via IsValidationError.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// validationErrorf constructs a *ValidationError with a formatted message.
func validationErrorf(format string, args ...any) error {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}

// IsValidationError reports whether err (or anything it wraps) is a
// *ValidationError, returning its client-safe message when so. The boundary
// layer surfaces this message with HTTP 400; everything else stays generic.
func IsValidationError(err error) (bool, string) {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return true, ve.Message
	}
	return false, ""
}
