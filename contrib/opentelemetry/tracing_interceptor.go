// Package opentelemetry provides OpenTelemetry utilities.
package opentelemetry

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// DefaultTextMapPropagator is the default OpenTelemetry TextMapPropagator used
// by this implementation if not otherwise set in TracerOptions.
var DefaultTextMapPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// TracerOptions are options provided to NewTracingInterceptor or NewTracer.
type TracerOptions struct {
	// Tracer is the tracer to use. If not set, one is obtained from the global
	// tracer provider using the name "temporal-sdk-go".
	Tracer trace.Tracer

	// DisableSignalTracing can be set to disable signal tracing.
	DisableSignalTracing bool

	// DisableQueryTracing can be set to disable query tracing.
	DisableQueryTracing bool

	// DisableUpdateTracing can be set to disable update tracing.
	DisableUpdateTracing bool

	// DisableBaggage can be set to disable baggage propagation.
	DisableBaggage bool

	// AllowInvalidParentSpans will swallow errors interpreting parent
	// spans from headers. Useful when migrating from one tracing library
	// to another, while workflows/activities may be in progress.
	AllowInvalidParentSpans bool

	// TextMapPropagator is the propagator to use for serializing spans. If not
	// set, this uses DefaultTextMapPropagator, not the OpenTelemetry global one.
	// To use the OpenTelemetry global one, set this value to the result of the
	// global call.
	TextMapPropagator propagation.TextMapPropagator

	// SpanContextKey is the context key used for internal span tracking (not to
	// be confused with the context key OpenTelemetry uses internally). If not
	// set, this defaults to an internal key (recommended).
	SpanContextKey interface{}

	// HeaderKey is the Temporal header field key used to serialize spans. If
	// empty, this defaults to the one used by all SDKs (recommended).
	HeaderKey string

	// SpanStarter is a callback to create spans. If not set, this creates normal
	// OpenTelemetry spans calling Tracer.Start.
	SpanStarter func(ctx context.Context, t trace.Tracer, spanName string, opts ...trace.SpanStartOption) trace.Span
}

type spanContextKey struct{}

const defaultHeaderKey = "_tracer-data"

type tracer struct {
	interceptor.BaseTracer
	options *TracerOptions
}

// NewTracer creates a tracer with the given options. Most callers should use
// NewTracingInterceptor instead.
func NewTracer(options TracerOptions) (interceptor.Tracer, error) {
	_ = "STUB: not implemented"
	return *new(interceptor.Tracer), nil
}

// NewTracingInterceptor creates an interceptor for setting on client options
// that implements OpenTelemetry tracing for workflows.
func NewTracingInterceptor(options TracerOptions) (interceptor.Interceptor, error) {
	_ = "STUB: not implemented"
	return *new(interceptor.Interceptor), nil
}

func (t *tracer) Options() interceptor.TracerOptions {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerOptions)
}

func (t *tracer) UnmarshalSpan(m map[string]string) (interceptor.TracerSpanRef, error) {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerSpanRef), nil
}

// If there is no span, return nothing, but don't error out. This is
// a legitimate place where a span does not exist in the headers

func (t *tracer) MarshalSpan(span interceptor.TracerSpan) (map[string]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (t *tracer) SpanFromContext(ctx context.Context) interceptor.TracerSpan {
	_ = "STUB: not implemented"
	return *new(interceptor.TracerSpan)
}

func (t *tracer) ContextWithSpan(ctx context.Context, span interceptor.TracerSpan) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

// SpanFromWorkflowContext extracts an OpenTelemetry span from the given
// workflow context.  If no span is found, a no-op span is returned.
func SpanFromWorkflowContext(ctx workflow.Context) (trace.Span, bool) {
	_ = "STUB: not implemented"
	return *new(trace.Span), false
}

// Fallback to OpenTelemetry span extraction behavior

func (t *tracer) StartSpan(opts *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	_ = "STUB: not implemented"
	// Create context with parent
	return *new(interceptor.TracerSpan), nil
}

// Create span

// Set tags

func (t *tracer) GetLogger(logger log.Logger, ref interceptor.TracerSpanRef) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

type tracerSpanRef struct {
	trace.SpanContext
	baggage.Baggage
}

type tracerSpan struct {
	trace.Span
	baggage.Baggage
}

func (t *tracerSpan) Finish(opts *interceptor.TracerFinishSpanOptions) {
	_ = "STUB: not implemented"
	return
}

func isBenignApplicationError(err error) bool { _ = "STUB: not implemented"; return false }

type textMapCarrier map[string]string

func (t textMapCarrier) Get(key string) string        { _ = "STUB: not implemented"; return "" }
func (t textMapCarrier) Set(key string, value string) { _ = "STUB: not implemented"; return }
func (t textMapCarrier) Keys() []string               { _ = "STUB: not implemented"; return nil }
