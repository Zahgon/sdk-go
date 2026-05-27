package internal

// All code in this file is private to the package.

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/sdk/internal/common/metrics"
	"google.golang.org/grpc/metadata"
)

const (
	clientNameHeaderName              = "client-name"
	clientNameHeaderValue             = "temporal-go"
	clientVersionHeaderName           = "client-version"
	supportedServerVersionsHeaderName = "supported-server-versions"

	// defaultRPCTimeout is the default gRPC call timeout.
	defaultRPCTimeout = 10 * time.Second
	// minRPCTimeout is minimum gRPC call timeout allowed.
	minRPCTimeout = 1 * time.Second
	// maxRPCTimeout is maximum gRPC call timeout allowed (should not be less than defaultRPCTimeout).
	maxRPCTimeout = 10 * time.Second

	temporalPrefix      = "__temporal_"
	temporalPrefixError = "__temporal_ is a reserved prefix"
)

// grpcContextBuilder stores all gRPC-specific parameters that will
// be stored inside of a context.
type grpcContextBuilder struct {
	Timeout time.Duration

	// ParentContext to build the new context from. If empty, context.Background() is used.
	// The new (child) context inherits a number of properties from the parent context:
	//   - context fields, accessible via `ctx.Value(key)`
	ParentContext context.Context

	MetricsHandler metrics.Handler

	Headers metadata.MD

	IsLongPoll bool
}

func (cb *grpcContextBuilder) Build() (context.Context, context.CancelFunc) {
	_ = "STUB: not implemented"
	return *new(context.Context), *new(context.CancelFunc)
}

func grpcTimeout(timeout time.Duration) func(builder *grpcContextBuilder) {
	_ = "STUB: not implemented"
	return nil
}

func grpcMetricsHandler(metricsHandler metrics.Handler) func(builder *grpcContextBuilder) {
	_ = "STUB: not implemented"
	return nil
}

func grpcLongPoll(isLongPoll bool) func(builder *grpcContextBuilder) {
	_ = "STUB: not implemented"
	return nil
}

func grpcContextValue(key interface{}, val interface{}) func(builder *grpcContextBuilder) {
	_ = "STUB: not implemented"
	return nil
}

func defaultGrpcRetryParameters(ctx context.Context) func(builder *grpcContextBuilder) {
	_ = "STUB: not implemented"
	return nil
}

// newGRPCContext - Get context for gRPC calls.
func newGRPCContext(ctx context.Context, options ...func(builder *grpcContextBuilder)) (context.Context, context.CancelFunc) {
	_ = "STUB: not implemented"
	return *new(context.Context), *new(context.CancelFunc)
}

// Set rpc timeout less than context timeout to allow for retries when call gets lost

// Make sure to not set rpc timeout lower than minRPCTimeout

// GetWorkerIdentity gets a default identity for the worker.
func getWorkerIdentity(taskqueueName string) string { _ = "STUB: not implemented"; return "" }

func getHostName() string { _ = "STUB: not implemented"; return "" }

func getWorkerTaskQueue(stickyUUID string) string {
	_ = "STUB: not implemented"
	// includes hostname for debuggability, stickyUUID guarantees the uniqueness
	return ""
}

// AwaitWaitGroup calls Wait on the given wait
// Returns true if the Wait() call succeeded before the timeout
// Returns false if the Wait() did not return before the timeout
func awaitWaitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {
	_ = "STUB: not implemented"
	return false
}

// InterruptCh returns channel which will get data when system receives interrupt signal. Pass it to worker.Run() func to stop worker with Ctrl+C.
func InterruptCh() <-chan interface{} { _ = "STUB: not implemented"; return nil }

func getStringID(intID int64) string { _ = "STUB: not implemented"; return "" }

type PollerAutoscaleBehavior struct {
	// Minimum is the minimum number of poll calls that will always be attempted (assuming slots are available).
	//
	// Cannot be less than two for workflow tasks, or one for other tasks.
	Minimum int
	// Maximum is the maximum number of poll calls that will ever be open at once. Must be >= `minimum`.
	Maximum int
	// Initial is the number of polls that will be attempted initially before scaling kicks in. Must be between
	// `minimum` and `maximum`.
	Initial int
}
