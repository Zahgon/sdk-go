package backoff

import (
	"time"

	"go.temporal.io/sdk/internal/common/retry"
)

const (
	done time.Duration = -1
)

type (
	// RetryPolicy is the API which needs to be implemented by various retry policy implementations
	RetryPolicy interface {
		ComputeNextDelay(elapsedTime time.Duration, attempt int) time.Duration
		GrpcRetryConfig() *retry.GrpcRetryConfig
	}

	// Retrier manages the state of retry operation
	Retrier interface {
		GetElapsedTime() time.Duration
		NextBackOff() time.Duration
		Reset()
	}

	// Clock used by ExponentialRetryPolicy implementation to get the current time.  Mainly used for unit testing
	Clock interface {
		Now() time.Time
	}

	// ExponentialRetryPolicy provides the implementation for retry policy using a coefficient to compute the next delay.
	// Formula used to compute the next delay is: initialInterval * math.Pow(backoffCoefficient, currentAttempt)
	ExponentialRetryPolicy struct {
		initialInterval    time.Duration
		backoffCoefficient float64
		maximumInterval    time.Duration
		expirationInterval time.Duration
		maximumAttempts    int
	}

	systemClock struct{}

	retrierImpl struct {
		policy         RetryPolicy
		clock          Clock
		currentAttempt int
		startTime      time.Time
	}
)

// SystemClock implements Clock interface that uses time.Now().
var SystemClock = systemClock{}

// NewExponentialRetryPolicy returns an instance of ExponentialRetryPolicy using the provided initialInterval
func NewExponentialRetryPolicy(initialInterval time.Duration) *ExponentialRetryPolicy {
	_ = "STUB: not implemented"
	return nil
}

// NewRetrier is used for creating a new instance of Retrier
func NewRetrier(policy RetryPolicy, clock Clock) Retrier {
	_ = "STUB: not implemented"
	return *new(Retrier)
}

// SetInitialInterval sets the initial interval used by ExponentialRetryPolicy for the very first retry
// All later retries are computed using the following formula:
// initialInterval * math.Pow(backoffCoefficient, currentAttempt)
func (p *ExponentialRetryPolicy) SetInitialInterval(initialInterval time.Duration) {
	_ = "STUB: not implemented"
	return
}

// SetBackoffCoefficient sets the coefficient used by ExponentialRetryPolicy to compute next delay for each retry
// All retries are computed using the following formula:
// initialInterval * math.Pow(backoffCoefficient, currentAttempt)
func (p *ExponentialRetryPolicy) SetBackoffCoefficient(backoffCoefficient float64) {
	_ = "STUB: not implemented"
	return
}

// SetMaximumInterval sets the maximum interval for each retry
func (p *ExponentialRetryPolicy) SetMaximumInterval(maximumInterval time.Duration) {
	_ = "STUB: not implemented"
	return
}

// SetExpirationInterval sets the absolute expiration interval for all retries
func (p *ExponentialRetryPolicy) SetExpirationInterval(expirationInterval time.Duration) {
	_ = "STUB: not implemented"
	return
}

// SetMaximumAttempts sets the maximum number of retry attempts
func (p *ExponentialRetryPolicy) SetMaximumAttempts(maximumAttempts int) {
	_ = "STUB: not implemented"
	return
}

// ComputeNextDelay returns the next delay interval.  This is used by Retrier to delay calling the operation again
func (p *ExponentialRetryPolicy) ComputeNextDelay(elapsedTime time.Duration, attempt int) time.Duration {
	_ = "STUB: not implemented"
	// Check to see if we ran out of maximum number of attempts
	return *new(time.Duration)
}

// Stop retrying after expiration interval is elapsed

// Disallow retries if initialInterval is negative or nextInterval overflows

// Bail out if the next interval is smaller than initial retry interval

// add jitter to avoid global synchronization

// Prevent overflow

// GrpcRetryConfig converts retry policy into retry config.
func (p *ExponentialRetryPolicy) GrpcRetryConfig() *retry.GrpcRetryConfig {
	_ = "STUB: not implemented"
	return nil
}

// Now returns the current time using the system clock
func (t systemClock) Now() time.Time {
	_ = "STUB: not implemented"

	// Reset will set the Retrier into initial state
	return *new(time.Time)
}

func (r *retrierImpl) Reset() { _ = "STUB: not implemented"; return }

// NextBackOff returns the next delay interval.  This is used by Retry to delay calling the operation again
func (r *retrierImpl) NextBackOff() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

// Now increment the current attempt

// GetElapsedTime returns the amount of time since the retrier was created or the last reset,
// whatever was sooner.
func (r *retrierImpl) GetElapsedTime() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}
