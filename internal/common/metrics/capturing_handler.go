package metrics

import (
	"sync"
	"time"
)

// This file contains test helpers only. They are not private because they are used by other tests.

type capturedInfo struct {
	sliceLock sync.RWMutex // Only governs slice access, not what's in the slice
	counters  []*CapturedCounter
	gauges    []*CapturedGauge
	timers    []*CapturedTimer
}

// CapturingHandler is a Handler that retains counted values locally.
type CapturingHandler struct {
	*capturedInfo
	// Never changed once created
	tags map[string]string
}

var _ Handler = &CapturingHandler{}

// NewCapturingHandler creates a new CapturingHandler.
func NewCapturingHandler() *CapturingHandler { _ = "STUB: not implemented"; return nil }

// Clear removes all known metrics from the root handler.
func (c *CapturingHandler) Clear() { _ = "STUB: not implemented"; return }

// WithTags implements Handler.WithTags.
func (c *CapturingHandler) WithTags(tags map[string]string) Handler {
	_ = "STUB: not implemented"
	return *new(Handler)
}

// Counter implements Handler.Counter.
func (c *CapturingHandler) Counter(name string) Counter {
	_ = "STUB: not implemented"
	return *new(Counter)
}

// Try to find one or create otherwise

// Counters returns shallow copy of the local counters. New counters will not
// get added here, but the value within the counter may still change.
func (c *CapturingHandler) Counters() []*CapturedCounter { _ = "STUB: not implemented"; return nil }

// Gauge implements Handler.Gauge.
func (c *CapturingHandler) Gauge(name string) Gauge { _ = "STUB: not implemented"; return *new(Gauge) }

// Try to find one or create otherwise

// Gauges returns shallow copy of the local gauges. New gauges will not get
// added here, but the value within the gauge may still change.
func (c *CapturingHandler) Gauges() []*CapturedGauge { _ = "STUB: not implemented"; return nil }

// Timer implements Handler.Timer.
func (c *CapturingHandler) Timer(name string) Timer { _ = "STUB: not implemented"; return *new(Timer) }

// Try to find one or create otherwise

// Timers returns shallow copy of the local timers. New timers will not get
// added here, but the value within the timer may still change.
func (c *CapturingHandler) Timers() []*CapturedTimer { _ = "STUB: not implemented"; return nil }

// CapturedMetricMeta is common information for captured metrics. These fields
// should never by mutated.
type CapturedMetricMeta struct {
	Name string
	Tags map[string]string
}

func (c *CapturedMetricMeta) equalTags(other map[string]string) bool {
	_ = "STUB: not implemented"
	return false
}

// CapturedCounter atomically implements Counter and provides an atomic getter.
type CapturedCounter struct {
	CapturedMetricMeta
	value int64
}

// Inc implements Counter.Inc.
func (c *CapturedCounter) Inc(d int64) { _ = "STUB: not implemented"; return }

// Value atomically returns the current value.
func (c *CapturedCounter) Value() int64 { _ = "STUB: not implemented"; return 0 }

// CapturedGauge atomically implements Gauge and provides an atomic getter.
type CapturedGauge struct {
	CapturedMetricMeta
	value     float64
	valueLock sync.RWMutex
}

// Update implements Gauge.Update.
func (c *CapturedGauge) Update(d float64) { _ = "STUB: not implemented"; return }

// Value atomically returns the current value.
func (c *CapturedGauge) Value() float64 { _ = "STUB: not implemented"; return 0 }

// CapturedTimer atomically implements Timer and provides an atomic getter.
type CapturedTimer struct {
	CapturedMetricMeta
	value int64
	count int64
}

// Record implements Timer.Record.
func (c *CapturedTimer) Record(d time.Duration) { _ = "STUB: not implemented"; return }

// Value atomically returns the current value.
func (c *CapturedTimer) Value() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

// Count atomically returns the current count.
func (c *CapturedTimer) Count() int64 { _ = "STUB: not implemented"; return 0 }
