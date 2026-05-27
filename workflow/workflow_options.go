package workflow

import (
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/temporal"
)

// WithChildOptions adds all workflow options to the context.
func WithChildOptions(ctx Context, cwo ChildWorkflowOptions) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWorkflowNamespace adds a namespace to the context.
//
// Deprecated: Cross-namespace operations are disabled by default as of server 1.30.1.
func WithWorkflowNamespace(ctx Context, name string) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWorkflowTaskQueue adds a task queue to the context.
func WithWorkflowTaskQueue(ctx Context, name string) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWorkflowID adds a workflowID to the context.
func WithWorkflowID(ctx Context, workflowID string) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWorkflowRunTimeout adds a run timeout to the context.
// The current timeout resolution implementation is in seconds and uses math.Ceil(d.Seconds()) as the duration. But is
// subjected to change in the future.
func WithWorkflowRunTimeout(ctx Context, d time.Duration) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWorkflowTaskTimeout adds a workflow task timeout to the context.
// The current timeout resolution implementation is in seconds and uses math.Ceil(d.Seconds()) as the duration. But is
// subjected to change in the future.
func WithWorkflowTaskTimeout(ctx Context, d time.Duration) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithDataConverter adds DataConverter to the context.
func WithDataConverter(ctx Context, dc converter.DataConverter) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// WithWorkflowPriority adds a priority to the context.
//
// WARNING: Task queue priority is currently experimental.
func WithWorkflowPriority(ctx Context, priority internal.Priority) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// GetChildWorkflowOptions returns all workflow options present on the context.
func GetChildWorkflowOptions(ctx Context) ChildWorkflowOptions {
	_ = "STUB: not implemented"
	return *new(ChildWorkflowOptions)
}

// WithWorkflowVersioningIntent is used to set the VersioningIntent before constructing a
// ContinueAsNewError with NewContinueAsNewError.
//
// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
func WithWorkflowVersioningIntent(ctx Context, intent temporal.VersioningIntent) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}
