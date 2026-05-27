package metrics

import "time"

// Handler is a handler for metrics emitted by the SDK. This interface is
// intentionally limited to only what the SDK needs to emit metrics and is not
// built to be a general purpose metrics abstraction for all uses.
//
// A common implementation is at
// go.temporal.io/sdk/contrib/tally.NewMetricsHandler. The NopHandler is a noop
// handler. A handler may implement "Unwrap() Handler" if it wraps a handler.
type Handler interface {
	// WithTags returns a new handler with the given tags set for each metric
	// created from it. Old tags from the previous handler are either preserved
	// or overwritten, if an existing key is also present in the new tag set.
	WithTags(map[string]string) Handler

	// Counter obtains a counter for the given name.
	Counter(name string) Counter

	// Gauge obtains a gauge for the given name.
	Gauge(name string) Gauge

	// Timer obtains a timer for the given name.
	Timer(name string) Timer
}

// Counter is an ever-increasing counter.
type Counter interface {
	// Inc increments the counter value.
	Inc(int64)
}

// CounterFunc implements Counter with a single function.
type CounterFunc func(int64)

// Inc implements Counter.Inc.
func (c CounterFunc) Inc(d int64) {
	_ = "STUB: not implemented"

	// Gauge can be set to any float.
	return
}

type Gauge interface {
	// Update updates the gauge value.
	Update(float64)
}

// GaugeFunc implements Gauge with a single function.
type GaugeFunc func(float64)

// Update implements Gauge.Update.
func (g GaugeFunc) Update(d float64) {
	_ = "STUB: not implemented"

	// Timer records time durations.
	return
}

type Timer interface {
	// Record sets the timer value.
	Record(time.Duration)
}

// TimerFunc implements Timer with a single function.
type TimerFunc func(time.Duration)

// Record implements Timer.Record.
func (t TimerFunc) Record(d time.Duration) {
	_ = "STUB: not implemented"

	// NopHandler is a noop handler that does nothing with the metrics.
	return
}

var NopHandler Handler = nopHandler{}

type nopHandler struct{}

func (nopHandler) WithTags(map[string]string) Handler {
	_ = "STUB: not implemented"
	return *new(Handler)
}
func (nopHandler) Counter(string) Counter { _ = "STUB: not implemented"; return *new(Counter) }
func (nopHandler) Gauge(string) Gauge     { _ = "STUB: not implemented"; return *new(Gauge) }
func (nopHandler) Timer(string) Timer     { _ = "STUB: not implemented"; return *new(Timer) }
func (nopHandler) Inc(int64)              { _ = "STUB: not implemented"; return }
func (nopHandler) Update(float64)         { _ = "STUB: not implemented"; return }
func (nopHandler) Record(time.Duration)   { _ = "STUB: not implemented"; return }

type replayAwareHandler struct {
	replay     *bool
	underlying Handler
}

// NewReplayAwareHandler is a handler that will not record any metrics if the
// boolean pointed to by "replay" is true.
func NewReplayAwareHandler(replay *bool, underlying Handler) Handler {
	_ = "STUB: not implemented"
	return *new(Handler)
}

func (r *replayAwareHandler) WithTags(tags map[string]string) Handler {
	_ = "STUB: not implemented"
	return *new(Handler)
}

func (r *replayAwareHandler) Counter(name string) Counter {
	_ = "STUB: not implemented"
	return *new(Counter)
}

func (r *replayAwareHandler) Gauge(name string) Gauge {
	_ = "STUB: not implemented"
	return *new(Gauge)
}

func (r *replayAwareHandler) Timer(name string) Timer {
	_ = "STUB: not implemented"
	return *new(Timer)
}

func (r *replayAwareHandler) Unwrap() Handler { _ = "STUB: not implemented"; return *new(Handler) }
