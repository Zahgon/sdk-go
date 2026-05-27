// Package opentracing provides OpenTracing utilities.
package opentracing

import (
	"context"

	"github.com/opentracing/opentracing-go"

	"go.temporal.io/sdk/interceptor"
)

// TracerOptions are options provided to NewInterceptor or NewTracer.
type TracerOptions struct {
	// Tracer is the tracer to use. If not set, the global one is used.
	Tracer opentracing.Tracer

	// DisableSignalTracing can be set to disable signal tracing.
	DisableSignalTracing bool

	// DisableQueryTracing can be set to disable query tracing.
	DisableQueryTracing bool

	// SpanContextKey is the context key used for internal span tracking (not to
	// be confused with the context key OpenTracing uses internally). If not set,
	// this defaults to an internal key (recommended).
	SpanContextKey interface{}

	// HeaderKey is the Temporal header field key used to serialize spans. If
	// empty, this defaults to the one used by all SDKs (recommended).
	HeaderKey string

	// SpanStarter is a callback to create spans. If not set, this creates normal
	// OpenTracing spans calling Tracer.StartSpan.
	SpanStarter func(t opentracing.Tracer, operationName string, opts ...opentracing.StartSpanOption) opentracing.Span
}

type spanContextKey struct{}

const defaultHeaderKey = "_tracer-data"

type tracer struct {
	interceptor.BaseTracer
	options *TracerOptions
}

// NewTracer creates a tracer with the given options. Most callers should use
// NewInterceptor instead.
func NewTracer(options TracerOptions) (interceptor.Tracer, error) {
	_ = "STUB: not implemented"
	return *new(interceptor.Tracer), nil
}

// NewTracingInterceptor creates an interceptor for setting on client options
// that implements OpenTracing tracing for workflows.
func NewInterceptor(options TracerOptions) (interceptor.Interceptor, error) {
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

func (t *tracer) StartSpan(opts *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	_ = "STUB: not implemented"
	// Build start options
	return *new(interceptor.TracerSpan), nil
}

// Link parent

// Set tags

// Start

type tracerSpanRef struct{ opentracing.SpanContext }

type tracerSpan struct{ opentracing.Span }

func (t *tracerSpan) Finish(opts *interceptor.TracerFinishSpanOptions) {
	_ = "STUB: not implemented"
	return

	// Standard tag that can be bridged to OpenTelemetry
}
