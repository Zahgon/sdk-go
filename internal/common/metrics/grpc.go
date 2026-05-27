package metrics

import (
	"time"

	"google.golang.org/grpc"
)

// HandlerContextKey is the context key for a MetricHandler value.
type HandlerContextKey struct{}

// LongPollContextKey is the context key for a boolean stating whether the gRPC
// call is a long poll.
type LongPollContextKey struct{}

// NewGRPCInterceptor creates a new gRPC unary interceptor to record metrics.
func NewGRPCInterceptor(defaultHandler Handler, suffix string, disableRequestFailCodes bool) grpc.UnaryClientInterceptor {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor)
}

// Only take method name after the last slash

// Since this interceptor can be used for clients of different name, we
// attempt to extract the namespace out of the request. All namespace-based
// requests have been confirmed to have a top-level namespace field.

// Capture time, record start, run, and record end

func recordRequestStart(handler Handler, longPoll bool, suffix string) {
	_ = "STUB: not implemented"
	// Count request
	return
}

func recordRequestEnd(handler Handler, longPoll bool, suffix string, start time.Time, err error, disableRequestFailCodes bool) {
	_ = "STUB: not implemented"
	// Record latency
	return
}

// Count failure

// If it's a resource exhausted, extract cause if present and increment
