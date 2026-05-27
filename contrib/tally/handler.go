// Package tally implements a MetricsHandler backed by [github.com/uber-go/tally].
package tally

import (
	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/client"
)

type metricsHandler struct{ scope tally.Scope }

// NewMetricsHandler returns a [client.MetricsHandler] that is backed by the given Tally
// scope.
//
// Default metrics are Prometheus compatible, but they should be sanitized and
// use the Prometheus naming scope so they are cross-SDK compatible with
// OpenMetrics naming conventions:
//
//	opts := tally.ScopeOptions{
//		SanitizeOptions: &contribtally.PrometheusSanitizeOptions,
//		Separator: "_",
//	}
//	scope, _ := tally.NewRootScope(opts, time.Second)
//	scope = contribtally.NewPrometheusNamingScope(scope)
func NewMetricsHandler(scope tally.Scope) client.MetricsHandler {
	_ = "STUB: not implemented"
	return *new(client.MetricsHandler)
}

// ScopeFromHandler returns the underlying scope of the handler. Callers may
// need to check [go.temporal.io/sdk/workflow.IsReplaying] to avoid recording metrics during
// replay. If this handler was not created via this package, [github.com/uber-go/tally.NoopScope] is
// returned.
//
// Raw use of the scope is discouraged but may be used for Histograms or other
// advanced features. This scope does not skip metrics during replay like the
// metrics handler does. Therefore the caller should check replay state, for
// example:
//
//	scope := tally.NoopScope
//	if !workflow.IsReplaying(ctx) {
//		scope = ScopeFromHandler(workflow.GetMetricsHandler(ctx))
//	}
//	scope.Histogram("my_histogram", nil).RecordDuration(5 * time.Second)
func ScopeFromHandler(handler client.MetricsHandler) tally.Scope {
	_ = "STUB: not implemented"
	// Continually unwrap until we find an instance of our own handler
	return *new(tally.Scope)
}

// If unwrappable, do so, otherwise return noop

func (m metricsHandler) WithTags(tags map[string]string) client.MetricsHandler {
	_ = "STUB: not implemented"
	return *new(client.MetricsHandler)
}

func (m metricsHandler) Counter(name string) client.MetricsCounter {
	_ = "STUB: not implemented"
	return *new(client.MetricsCounter)
}

func (m metricsHandler) Gauge(name string) client.MetricsGauge {
	_ = "STUB: not implemented"
	return *new(client.MetricsGauge)
}

func (m metricsHandler) Timer(name string) client.MetricsTimer {
	_ = "STUB: not implemented"
	return *new(client.MetricsTimer)
}
