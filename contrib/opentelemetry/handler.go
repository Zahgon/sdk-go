package opentelemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/client"
)

var _ client.MetricsHandler = MetricsHandler{}

// MetricsHandler is an implementation of client.MetricsHandler
// for open telemetry.
type MetricsHandler struct {
	meter      metric.Meter
	attributes attribute.Set
	onError    func(error)
}

// MetricsHandlerOptions are options provided to NewMetricsHandler.
type MetricsHandlerOptions struct {
	// Meter is the Meter to use. If not set, one is obtained from the global
	// meter provider using the name "temporal-sdk-go".
	Meter metric.Meter
	// InitialAttributes to set on the handler
	//
	// Optional: Defaults to the empty set.
	InitialAttributes attribute.Set
	// OnError Callback to invoke if the provided meter returns an error.
	//
	// Optional: Defaults to panicking on any error.
	OnError func(error)
}

// NewMetricsHandler returns a client.MetricsHandler that is backed by the given Meter
func NewMetricsHandler(options MetricsHandlerOptions) MetricsHandler {
	_ = "STUB: not implemented"
	return *new(MetricsHandler)
}

// ExtractMetricsHandler gets the underlying Open Telemetry MetricsHandler from a MetricsHandler
// if any is present.
//
// Raw use of the MetricHandler is discouraged but may be used for Histograms or other
// advanced features. This scope does not skip metrics during replay like the
// metrics handler does. Therefore the caller should check replay state.
func ExtractMetricsHandler(handler client.MetricsHandler) *MetricsHandler {
	_ = "STUB: not implemented"
	// Continually unwrap until we find an instance of our own handler
	return nil
}

// If unwrappable, do so, otherwise return noop

// GetMeter returns the meter used by this handler.
func (m MetricsHandler) GetMeter() metric.Meter {
	_ = "STUB: not implemented"

	// GetAttributes returns the attributes set on this handler.
	return *new(metric.Meter)
}

func (m MetricsHandler) GetAttributes() attribute.Set {
	_ = "STUB: not implemented"
	return *new(attribute.Set)
}

func (m MetricsHandler) WithTags(tags map[string]string) client.MetricsHandler {
	_ = "STUB: not implemented"
	return *new(client.MetricsHandler)
}

func (m MetricsHandler) Counter(name string) client.MetricsCounter {
	_ = "STUB: not implemented"
	return *new(client.MetricsCounter)
}

func (m MetricsHandler) Gauge(name string) client.MetricsGauge {
	_ = "STUB: not implemented"
	return *new(client.MetricsGauge)
}

func (m MetricsHandler) Timer(name string) client.MetricsTimer {
	_ = "STUB: not implemented"
	return *new(client.MetricsTimer)
}
