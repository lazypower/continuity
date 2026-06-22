package engine

import (
	"errors"
	"log"
	"strings"

	"github.com/lazypower/continuity/internal/store"
)

// Pin marks a memory as an operator-declared pin (the "declared half" of the
// operating contract). A pinned memory is injected into the cold-boot context's
// dedicated Pinned section regardless of recency or relevance — until it is
// unpinned or retracted.
//
// Returns (newly bool, error). newly is true when this call performed the pin;
// false when the memory was already pinned (idempotent, original timestamp kept).
func (e *Engine) Pin(uri string) (bool, error) {
	if !strings.HasPrefix(uri, "mem://") {
		return false, validationErrorf("invalid URI %q: must start with mem://", uri)
	}

	newly, err := e.DB.PinNode(uri)
	if err != nil {
		// Store-level domain rejections (not found, directory node, retracted) are
		// actionable user input — re-wrap as ValidationError so the HTTP boundary
		// surfaces the real reason as 400. Internal failures stay plain and generic.
		var pve *store.PinValidationError
		if errors.As(err, &pve) {
			return false, validationErrorf("%s", pve.Message)
		}
		return false, err
	}

	if newly {
		log.Printf("pin: %s pinned (declared contract)", uri)
	} else {
		log.Printf("pin: %s already pinned (no-op)", uri)
	}
	return newly, nil
}

// Unpin clears an operator pin, removing the memory from the Pinned section.
//
// Returns (newly bool, error). newly is true when this call performed the unpin;
// false when the memory was not pinned (idempotent).
func (e *Engine) Unpin(uri string) (bool, error) {
	if !strings.HasPrefix(uri, "mem://") {
		return false, validationErrorf("invalid URI %q: must start with mem://", uri)
	}

	newly, err := e.DB.UnpinNode(uri)
	if err != nil {
		var pve *store.PinValidationError
		if errors.As(err, &pve) {
			return false, validationErrorf("%s", pve.Message)
		}
		return false, err
	}

	if newly {
		log.Printf("unpin: %s unpinned", uri)
	} else {
		log.Printf("unpin: %s was not pinned (no-op)", uri)
	}
	return newly, nil
}
