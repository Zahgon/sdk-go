package backoff

import (
	"context"
	"sync"
	"time"
)

type (
	// Operation to retry
	Operation func() error

	// IsRetryable handler can be used to exclude certain errors during retry
	IsRetryable func(error) bool

	// ConcurrentRetrier is used for client-side throttling. It determines whether to
	// throttle outgoing traffic in case downstream backend server rejects
	// requests due to out-of-quota or server busy errors.
	ConcurrentRetrier struct {
		sync.Mutex
		retrier                 Retrier // Backoff retrier
		secondaryRetrier        Retrier
		failureCount            int64 // Number of consecutive failures seen
		includeSecondaryRetrier bool
	}
)

// Throttle Sleep if there were failures since the last success call. The
// provided done channel provides a way to exit early.
func (c *ConcurrentRetrier) Throttle(doneCh <-chan struct{}) { _ = "STUB: not implemented"; return }

// GetElapsedTime gets the amount of time since that last ConcurrentRetrier.Succeeded call
func (c *ConcurrentRetrier) GetElapsedTime() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func (c *ConcurrentRetrier) throttleInternal(doneCh <-chan struct{}) time.Duration {
	_ = "STUB: not implemented"

	// Check if we have failure count.
	return *new(time.Duration)
}

// If secondary is included, use the greatest of the two (which also means
// if one is "done", which is -1, the one that's not done is chosen)

// Succeeded marks client request succeeded.
func (c *ConcurrentRetrier) Succeeded() { _ = "STUB: not implemented"; return }

// Failed marks client request failed because backend is busy. If
// includeSecondaryRetryPolicy is true, see SetSecondaryRetryPolicy for effects.
func (c *ConcurrentRetrier) Failed(includeSecondaryRetryPolicy bool) {
	_ = "STUB: not implemented"
	return
}

// SetSecondaryRetryPolicy sets a secondary retry policy that, if Failed is
// called with true, will trigger the secondary retry policy in addition to the
// primary and will use the result of the secondary if longer than the primary.
func (c *ConcurrentRetrier) SetSecondaryRetryPolicy(retryPolicy RetryPolicy) {
	_ = "STUB: not implemented"
	return
}

// NewConcurrentRetrier returns an instance of concurrent backoff retrier.
func NewConcurrentRetrier(retryPolicy RetryPolicy) *ConcurrentRetrier {
	_ = "STUB: not implemented"
	return nil
}

// Retry function can be used to wrap any call with retry logic using the passed in policy
func Retry(ctx context.Context, operation Operation, policy RetryPolicy, isRetryable IsRetryable) error {
	_ = "STUB: not implemented"
	return nil
}

// operation completed successfully. No need to retry.

// Usually, after number of retry attempts, last attempt fails with DeadlineExceeded error.
// It is not informative and actual error reason is in the error occurred on previous attempt.
// Therefore, update lastErr only if it is not set (first attempt) or opErr is not a DeadlineExceeded error.
// This lastErr is returned if retry attempts are exhausted.

// Check if the error is retryable

// check if ctx is done

// ctx is not cancellable

// IgnoreErrors can be used as IsRetryable handler for Retry function to exclude certain errors from the retry list
func IgnoreErrors(errorsToExclude []error) func(error) bool { _ = "STUB: not implemented"; return nil }
