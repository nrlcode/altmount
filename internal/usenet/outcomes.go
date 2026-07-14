package usenet

import (
	"context"
	"errors"
	"fmt"

	"github.com/javi11/nntppool/v4"
)

// IncompleteError means an operation did not produce enough conclusive
// results to support a health or import decision. It is deliberately distinct
// from hard article absence: callers must retry or defer instead of treating
// incomplete work as missing content.
type IncompleteError struct {
	Expected  int
	Completed int
	Cause     error
	// Global means the sweep itself was not trustworthy (for example its
	// context ended or it returned unrequested output), so even files whose
	// individual result counts look complete must remain inconclusive.
	Global bool
}

func (e *IncompleteError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause != nil {
		return fmt.Sprintf("incomplete Usenet check (%d/%d conclusive): %v", e.Completed, e.Expected, e.Cause)
	}
	return fmt.Sprintf("incomplete Usenet check (%d/%d conclusive)", e.Completed, e.Expected)
}

func (e *IncompleteError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// IsIncomplete reports whether err represents omitted, cancelled, or otherwise
// inconclusive provider work.
func IsIncomplete(err error) bool {
	var incomplete *IncompleteError
	return errors.As(err, &incomplete)
}

// ClassifyNNTPOutcome converts both the corrected nntppool typed contract and
// its stable sentinel errors into one semantic outcome. Unknown errors remain
// inconclusive and can never become content absence.
func ClassifyNNTPOutcome(err error) nntppool.OutcomeKind {
	if err == nil {
		return nntppool.OutcomeSuccess
	}

	var transportErr *nntppool.TransportError
	if errors.As(err, &transportErr) {
		return transportErr.Kind
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return nntppool.OutcomeCancellation
	case errors.Is(err, nntppool.ErrBodyCorrupt), errors.Is(err, nntppool.ErrCRCMismatch):
		return nntppool.OutcomeCorruptBody
	case errors.Is(err, nntppool.ErrCircuitBreakerOpen):
		return nntppool.OutcomeTemporaryFailure
	case errors.Is(err, nntppool.ErrServiceUnavailable),
		errors.Is(err, nntppool.ErrAuthRequired),
		errors.Is(err, nntppool.ErrAuthRejected),
		errors.Is(err, nntppool.ErrQuotaExceeded),
		errors.Is(err, nntppool.ErrInvalidProviderConfiguration),
		errors.Is(err, nntppool.ErrMaxConnections):
		return nntppool.OutcomeProviderUnavailable
	case errors.Is(err, nntppool.ErrArticleNotFound):
		return nntppool.OutcomeHardArticleAbsence
	default:
		return nntppool.OutcomeInconclusive
	}
}

// IsHardArticleAbsence reports whether the complete eligible-provider result
// conclusively says that the article is absent.
func IsHardArticleAbsence(err error) bool {
	return ClassifyNNTPOutcome(err) == nntppool.OutcomeHardArticleAbsence
}

// IsCorruptBody reports whether transport-level BODY validation failed without
// promoting the result to hard article absence.
func IsCorruptBody(err error) bool {
	return ClassifyNNTPOutcome(err) == nntppool.OutcomeCorruptBody
}

// IsClassifiedNNTPError reports whether an error carries transport-owned or
// AltMount incomplete-work semantics, as opposed to an ordinary local parser
// or archive-format failure. Callers that intentionally isolate local file
// failures must still propagate these errors so temporary or inconclusive
// provider work cannot become partial success.
func IsClassifiedNNTPError(err error) bool {
	if err == nil {
		return false
	}

	var transportErr *nntppool.TransportError
	if errors.As(err, &transportErr) {
		return true
	}
	var incomplete *IncompleteError
	if errors.As(err, &incomplete) {
		return true
	}
	var corruption *DataCorruptionError
	if errors.As(err, &corruption) && corruption.Outcome != "" {
		return true
	}

	return errors.Is(err, nntppool.ErrArticleNotFound) ||
		errors.Is(err, nntppool.ErrBodyCorrupt) ||
		errors.Is(err, nntppool.ErrCRCMismatch) ||
		errors.Is(err, nntppool.ErrCircuitBreakerOpen) ||
		errors.Is(err, nntppool.ErrServiceUnavailable) ||
		errors.Is(err, nntppool.ErrAuthRequired) ||
		errors.Is(err, nntppool.ErrAuthRejected) ||
		errors.Is(err, nntppool.ErrQuotaExceeded) ||
		errors.Is(err, nntppool.ErrInvalidProviderConfiguration) ||
		errors.Is(err, nntppool.ErrMaxConnections)
}
