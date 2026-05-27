package retry

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// UnlimitedMaximumAttempts when maximum attempts is set to this special value, then the number of attempts is unlimited.
	UnlimitedMaximumAttempts = 0
	// UnlimitedInterval when maximum interval is set to this special value, then there is no upper bound on the retry delay.
	// Should not be used together with unlimited attempts as resulting retry interval can grow to unreasonable values.
	UnlimitedInterval = 0
	// DefaultBackoffCoefficient is default backOffCoefficient for retryPolicy
	DefaultBackoffCoefficient = 2.0
	// DefaultMaximumInterval is default maximum amount of time for an individual retry.
	DefaultMaximumInterval = 10 * time.Second
	// DefaultExpirationInterval is default expiration time for all retry attempts.
	DefaultExpirationInterval = time.Minute
	// DefaultMaximumAttempts is default maximum number of attempts.
	DefaultMaximumAttempts = UnlimitedMaximumAttempts
	// DefaultJitter is a default jitter applied on the backoff interval for delay randomization.
	DefaultJitter = 0.2
)

type (
	// GrpcRetryConfig defines required configuration for exponential backoff function that is supplied to gRPC retrier.
	GrpcRetryConfig struct {
		initialInterval    time.Duration
		backoffCoefficient float64
		maximumInterval    time.Duration
		expirationInterval time.Duration
		jitter             float64
		maximumAttempts    int
	}

	GrpcMessageTooLargeError struct {
		err    error
		status *status.Status
	}

	contextKey struct{}
)

func (ck contextKey) String() string { _ = "STUB: not implemented"; return "" }

// SetBackoffCoefficient sets rate at which backoff coefficient will change.
func (g *GrpcRetryConfig) SetBackoffCoefficient(backoffCoefficient float64) {
	_ = "STUB: not implemented"
	return
}

// SetMaximumInterval defines maximum amount of time between attempts.
func (g *GrpcRetryConfig) SetMaximumInterval(maximumInterval time.Duration) {
	_ = "STUB: not implemented"
	return
}

// SetExpirationInterval defines total amount of time that can be used for all retry attempts.
// Note that this value is ignored if deadline is set on the context.
func (g *GrpcRetryConfig) SetExpirationInterval(expirationInterval time.Duration) {
	_ = "STUB: not implemented"
	return
}

// SetJitter defines level of randomization for each delay interval. For example 0.2 would mex target +- 20%
func (g *GrpcRetryConfig) SetJitter(jitter float64) {
	_ = "STUB: not implemented"

	// SetMaximumAttempts defines maximum total number of retry attempts.
	return
}

func (g *GrpcRetryConfig) SetMaximumAttempts(maximumAttempts int) {
	_ = "STUB: not implemented"
	return
}

// NewGrpcRetryConfig creates new retry config with specified initial interval and defaults for other parameters.
// Use SetXXX functions on this config in order to customize values.
func NewGrpcRetryConfig(initialInterval time.Duration) *GrpcRetryConfig {
	_ = "STUB: not implemented"
	return nil
}

var (
	// ConfigKey context key for GrpcRetryConfig
	ConfigKey = contextKey{}
	// gRPC response codes that represent retryable errors.
	// The following status codes are never retried by the library:
	//    INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, FAILED_PRECONDITION, ABORTED, OUT_OF_RANGE, DATA_LOSS
	// codes.DeadlineExceeded and codes.Canceled are not here (and shouldn't be here!)
	// because they are coming from go context and "context errors are not retriable based on user settings"
	// by gRPC library.
	// codes.ResourceExhausted is non-retryable if it comes from GrpcMessageTooLargeError, but otherwise is retryable.
	// codes.Internal is not included because it's retryable or non-retryable depending on server capabilities.
	retryableCodesWithoutInternal = []codes.Code{codes.Aborted, codes.ResourceExhausted, codes.Unavailable, codes.Unknown}
)

// NewRetryOptionsInterceptor creates a new gRPC interceptor that populates retry options for each call based on values
// provided in the context. The atomic bool is checked each call to determine whether internals are included in retry.
// If not present or false, internals are assumed to be included.
func NewRetryOptionsInterceptor(excludeInternal *atomic.Bool) grpc.UnaryClientInterceptor {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor)
}

// Populate backoff function, which provides retrier with the delay for each attempt.

// Max attempts is a required parameter in grpc retry interceptor,
// if it's set to zero then no retries will be made.

// Do not retry if retry config is not set.

func IsRetryable(err error, excludeInternalFromRetry *atomic.Bool) bool {
	_ = "STUB: not implemented"
	return false
}

// GrpcMessageTooLargeErrorInterceptor checks if the error is caused by gRPC message being too large and converts it into GrpcMessageTooLargeError.
func GrpcMessageTooLargeErrorInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	_ = "STUB: not implemented"
	return nil
}

func (e *GrpcMessageTooLargeError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *GrpcMessageTooLargeError) Unwrap() error { _ = "STUB: not implemented"; return nil }

func (e *GrpcMessageTooLargeError) GRPCStatus() *status.Status {
	_ = "STUB: not implemented"
	return nil
}

func isGrpcMessageTooLargeStatus(status *status.Status) bool {
	_ = "STUB: not implemented"
	return false
}

// Source: https://github.com/search?q=repo%3Agrpc%2Fgrpc-go+ResourceExhausted&type=code
