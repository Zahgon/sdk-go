// Package otel provides convenience helpers for configuring OpenTelemetry
// metrics and tracing on a Temporal client running inside AWS Lambda.
//
// Use [ApplyDefaults] inside a [lambdaworker.RunWorker] configure callback for a
// batteries-included setup that creates OTLP gRPC exporters and an AWS X-Ray ID
// generator, suitable for use with the AWS Distro for OpenTelemetry (ADOT) Lambda layer.
//
// Use [ApplyDefaultsWithProviders] if you need to supply your own MeterProvider and TracerProvider.
package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdkmetric "go.opentelemetry.io/otel/sdk/metric"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"

	"go.temporal.io/sdk/client"
)

// ShutdownRegistrar accepts a function to be called at the end of each Lambda invocation.
// [lambdaworker.Options] implements this interface.
type ShutdownRegistrar interface {
	OnShutdown(func(context.Context) error)
}

// Options configures the behavior of [ApplyDefaults].
type Options struct {
	// MetricExportInterval controls how often metrics are exported. Defaults to 10 seconds.
	MetricExportInterval time.Duration

	// ServiceName sets the OTel service name resource attribute. If empty, defaults to the
	// OTEL_SERVICE_NAME environment variable, then AWS_LAMBDA_FUNCTION_NAME, then
	// "temporal-lambda-worker".
	ServiceName string

	// CollectorEndpoint sets the OTLP gRPC collector endpoint (e.g. "localhost:4317").
	// If empty, defaults to the OTEL_EXPORTER_OTLP_ENDPOINT environment variable, then
	// "localhost:4317".
	CollectorEndpoint string

	// MetricExporterOptions are additional options passed to the OTLP gRPC metric exporter.
	// By default [otlpmetricgrpc.WithInsecure] is prepended; set this to override that
	// default (e.g. to use TLS).
	MetricExporterOptions []otlpmetricgrpc.Option

	// TraceExporterOptions are additional options passed to the OTLP gRPC trace exporter.
	// By default [otlptracegrpc.WithInsecure] is prepended; set this to override that
	// default (e.g. to use TLS).
	TraceExporterOptions []otlptracegrpc.Option
}

// ApplyDefaults configures OTel metrics and tracing on the given client options using AWS Lambda
// defaults. It creates OTLP gRPC exporters (insecure, defaulting to the localhost:4317 endpoint
// expected by the ADOT collector Lambda layer) and an AWS X-Ray compatible trace ID generator.
//
// The collector endpoint and service name can be set via [Options], or fall back to environment
// variables (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME / AWS_LAMBDA_FUNCTION_NAME).
//
// ApplyDefaults registers a per-invocation ForceFlush hook on the given [ShutdownRegistrar] so
// that pending metrics and traces are exported before each Lambda invocation completes. It calls
// only ForceFlush (not Shutdown) so the providers remain usable across warm-start invocations.
// Permanent provider shutdown is unnecessary in Lambda since the runtime terminates the process.
//
// Call this from a [lambdaworker.RunWorker] configure callback, passing the
// [lambdaworker.Options] as the [ShutdownRegistrar].
// If you need more control, see [ApplyDefaultsWithProviders].
func ApplyDefaults(
	ctx ShutdownRegistrar, opts *client.Options, options Options,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Build a shared resource for both providers so that metrics and traces
// carry the same service.name, enabling correlation in backends.

// If anything below fails, shut down the meterProvider to stop its
// periodic reader goroutine and release the underlying gRPC connection.

// Use Background — the invocation context may already be cancelled.

// If ApplyDefaultsWithProviders fails, shut down the tracerProvider too.

// ApplyDefaultsWithProviders configures OTel metrics and tracing on the given client options using
// the provided MeterProvider and TracerProvider. It registers a per-invocation ForceFlush hook on
// the given [ShutdownRegistrar]. Use this instead of [ApplyDefaults] when you need full control
// over the OTel provider configuration.
//
// Call this from a [lambdaworker.RunWorker] configure callback, passing the
// [lambdaworker.Options] as the [ShutdownRegistrar].
func ApplyDefaultsWithProviders(
	ctx ShutdownRegistrar,
	opts *client.Options,
	meterProvider *otelsdkmetric.MeterProvider,
	tracerProvider *otelsdktrace.TracerProvider,
) error {
	_ = "STUB: not implemented"
	return nil
}

// ApplyMetrics configures only OTel metrics (no tracing) on the given client
// options.
func ApplyMetrics(opts *client.Options, meterProvider *otelsdkmetric.MeterProvider) {
	_ = "STUB: not implemented"
	return
}

// ApplyTracing configures only OTel tracing (no metrics) on the given client
// options.
func ApplyTracing(opts *client.Options, tracerProvider *otelsdktrace.TracerProvider) error {
	_ = "STUB: not implemented"
	return nil
}
