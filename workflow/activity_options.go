package workflow

import (
	"time"

	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/temporal"
)

// ActivityOptions stores all activity-specific invocation parameters that will be stored inside of a context.
type ActivityOptions = internal.ActivityOptions

// LocalActivityOptions doc
type LocalActivityOptions = internal.LocalActivityOptions

// WithActivityOptions makes a copy of the context and adds the
// passed in options to the context. If an activity options exists,
// it will be overwritten by the passed in value as a whole.
// So specify all the values in the options as necessary, as values
// in the existing context options will not be carried over.
func WithActivityOptions(ctx Context, options ActivityOptions) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithLocalActivityOptions makes a copy of the context and adds the
// passed in options to the context. If a local activity options exists,
// it will be overwritten by the passed in value.
func WithLocalActivityOptions(ctx Context, options LocalActivityOptions) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithTaskQueue makes a copy of the current context and update the taskQueue
// field in its activity options. An empty activity options will be created
// if it does not exist in the original context.
func WithTaskQueue(ctx Context, name string) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithScheduleToCloseTimeout makes a copy of the current context and update
// the ScheduleToCloseTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Temporal time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithScheduleToCloseTimeout(ctx Context, d time.Duration) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithScheduleToStartTimeout makes a copy of the current context and update
// the ScheduleToStartTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Temporal time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithScheduleToStartTimeout(ctx Context, d time.Duration) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithStartToCloseTimeout makes a copy of the current context and update
// the StartToCloseTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Temporal time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithStartToCloseTimeout(ctx Context, d time.Duration) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithHeartbeatTimeout makes a copy of the current context and update
// the HeartbeatTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Temporal time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithHeartbeatTimeout(ctx Context, d time.Duration) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWaitForCancellation makes a copy of the current context and update
// the WaitForCancellation field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
func WithWaitForCancellation(ctx Context, wait bool) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithRetryPolicy makes a copy of the current context and update
// the RetryPolicy field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
func WithRetryPolicy(ctx Context, retryPolicy temporal.RetryPolicy) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithPriority makes a copy of the current context and updates
// the Priority field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// WARNING: Task queue priority is currently experimental.
func WithPriority(ctx Context, priority temporal.Priority) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// GetActivityOptions returns all activity options present on the context.
func GetActivityOptions(ctx Context) ActivityOptions {
	_ = "STUB: not implemented"
	return *new(ActivityOptions)
}

// GetLocalActivityOptions returns all local activity options present on the context.
func GetLocalActivityOptions(ctx Context) LocalActivityOptions {
	_ = "STUB: not implemented"
	return *new(LocalActivityOptions)
}
