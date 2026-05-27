// Package tracing provides Datadog tracing utilities for Temporal workflows.
//
// # Breaking Changes in v0.5.0
//
// This release upgrades from dd-trace-go v1 to v2. Most users will not be affected,
// but there are two breaking changes:
//
//   - TracerOptions.OnFinish: If you provide a custom OnFinish function, update
//     your import from gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer to
//     github.com/DataDog/dd-trace-go/v2/ddtrace/tracer. The [tracer.FinishOption]
//     type is source-compatible, so no other changes are needed.
//
//   - SpanFromWorkflowContext: The return type changed from ddtrace.Span to
//     *tracer.Span. If you pass the returned span to code that expects the old
//     type, you will need to update that code.
package tracing

import (
	"context"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// TracerOptions are options provided to NewInterceptor
type TracerOptions struct {
	// DisableSignalTracing can be set to disable signal tracing.
	DisableSignalTracing bool

	// DisableQueryTracing can be set to disable query tracing.
	DisableQueryTracing bool

	// DisableUpdateTracing can be set to disable update tracing.
	DisableUpdateTracing bool

	// OnFinish sets finish options.
	// If unset, this will use [tracer.WithError]
	// in case [interceptor.TracerFinishSpanOptions.Error] is non-nil and not [workflow.IsContinueAsNewError].
	OnFinish func(options *interceptor.TracerFinishSpanOptions) []tracer.FinishOption
}

// NewTracingInterceptor convenience method that wraps a NeTracer() with a tracing interceptor
func NewTracingInterceptor(opts TracerOptions) interceptor.Interceptor {
	_ = "STUB: not implemented"
	return *new(interceptor.Interceptor)
}

// NewTracer creates an interceptor for setting on client options
// that implements Datadog tracing for workflows.
func NewTracer(opts TracerOptions) interceptor.Tracer {
	_ = "STUB: not implemented"
	return *new(interceptor.Tracer)
}

type contextKey string

const (
	activeSpanContextKey contextKey = "dd_trace_span"
	headerKey                       = string(activeSpanContextKey)
)

type tracerImpl struct {
	interceptor.BaseTracer
	// DisableSignalTracing can be set to disable signal tracing.
	opts TracerOptions
}

func (t *tracerImpl) Options() interceptor.TracerOptions {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerOptions)
}

func (t *tracerImpl) UnmarshalSpan(m map[string]string) (interceptor.TracerSpanRef, error) {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerSpanRef), nil
}

// If there is no span, return nothing, but don't error out. This is
// a legitimate place where a span does not exist in the headers

func (t *tracerImpl) MarshalSpan(span interceptor.TracerSpan) (map[string]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (t *tracerImpl) SpanFromContext(ctx context.Context) interceptor.TracerSpan {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerSpan)
}

func (t *tracerImpl) ContextWithSpan(ctx context.Context, span interceptor.TracerSpan) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

// SpanFromWorkflowContext extracts the DataDog Span object from the workflow context.
func SpanFromWorkflowContext(ctx workflow.Context) (*tracer.Span, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func genSpanID(idempotencyKey string) uint64 {
	_ = "STUB: not implemented"

	// Write() always writes all bytes and never fails; the count and error result are for implementing io.Writer.
	return 0
}

func (t *tracerImpl) StartSpan(options *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerSpan), nil
}

// Set a deterministic span ID for workflows which are long-running and cross process boundaries

// Add tags to start options.

// Display Temporal tags in a nested group in Datadog APM.

// This should be considered an error, because something unexpected is
// in the place where only a parent trace should be. In this case, we don't
// set up the parent, so we will be creating a new top-level span

func (t *tracerImpl) GetLogger(logger log.Logger, ref interceptor.TracerSpanRef) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (t *tracerImpl) SpanName(options *interceptor.TracerStartSpanOptions) string {
	_ = "STUB: not implemented"
	return ""
}

type tracerSpan struct {
	Span     *tracer.Span
	OnFinish func(options *interceptor.TracerFinishSpanOptions) []tracer.FinishOption
}
type tracerSpanCtx struct {
	*tracer.SpanContext
}

func (t *tracerSpan) SpanID() uint64 { _ = "STUB: not implemented"; return 0 }

func (t *tracerSpan) TraceID() uint64 { _ = "STUB: not implemented"; return 0 }

func (t *tracerSpan) ForeachBaggageItem(handler func(k string, v string) bool) {
	_ = "STUB: not implemented"
	return
}

func (t *tracerSpan) Finish(options *interceptor.TracerFinishSpanOptions) {
	_ = "STUB: not implemented"
	return
}
