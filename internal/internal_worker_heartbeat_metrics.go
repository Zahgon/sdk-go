package internal

import (
	"sync"
	"sync/atomic"
	"time"

	workerpb "go.temporal.io/api/worker/v1"

	"go.temporal.io/sdk/internal/common/metrics"
)

// Metrics we capture for heartbeat reporting.
var (
	capturedCounters = map[string]struct{}{
		metrics.StickyCacheHit:                      {},
		metrics.StickyCacheMiss:                     {},
		metrics.WorkflowTaskExecutionFailureCounter: {},
		metrics.ActivityExecutionFailedCounter:      {},
		metrics.LocalActivityExecutionFailedCounter: {},
		metrics.NexusTaskExecutionFailedCounter:     {},
	}

	// Timer recordings are counted (not their latencies) to track tasks processed.
	capturedTimers = map[string]struct{}{
		metrics.WorkflowTaskExecutionLatency:  {},
		metrics.ActivityExecutionLatency:      {},
		metrics.LocalActivityExecutionLatency: {},
		metrics.NexusTaskExecutionLatency:     {},
	}
)

// heartbeatMetricsHandler wraps a metrics handler and captures specific metrics
// in memory for worker heartbeats.
type heartbeatMetricsHandler struct {
	underlying metrics.Handler
	workerType string
	pollerType string

	// Keys are metric names, or "metricName:workerType" / "metricName:pollerType" for typed metrics.
	metrics *sync.Map
}

// newHeartbeatMetricsHandler creates a new handler that captures specific metrics
// for worker heartbeats while passing all metrics to the underlying handler.
func newHeartbeatMetricsHandler(underlying metrics.Handler) *heartbeatMetricsHandler {
	_ = "STUB: not implemented"
	return nil
}

// forWorker creates a new handler that captures metrics specific to a worker type, for worker heartbeating.
// This should be called explicitly before calling WithTags on the returned handler.
func (h *heartbeatMetricsHandler) forWorker(workerType string) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// forPoller creates a new handler that captures metrics specific to a poller type, for worker heartbeating.
// This should be called explicitly before calling WithTags on the returned handler.
func (h *heartbeatMetricsHandler) forPoller(pollerType string) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (h *heartbeatMetricsHandler) WithTags(tags map[string]string) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (h *heartbeatMetricsHandler) Counter(name string) metrics.Counter {
	_ = "STUB: not implemented"
	return *new(metrics.Counter)
}

func (h *heartbeatMetricsHandler) Gauge(name string) metrics.Gauge {
	_ = "STUB: not implemented"
	return *new(metrics.Gauge)
}

func (h *heartbeatMetricsHandler) Timer(name string) metrics.Timer {
	_ = "STUB: not implemented"
	return *new(metrics.Timer)
}

func (h *heartbeatMetricsHandler) getOrCreate(key string) *atomic.Int64 {
	_ = "STUB: not implemented"
	return nil
}

func (h *heartbeatMetricsHandler) get(key string) int64 { _ = "STUB: not implemented"; return 0 }

// populateHeartbeatOptions contains extra information needed to populate heartbeats.
type populateHeartbeatOptions struct {
	workflowSlotSupplierKind      string
	activitySlotSupplierKind      string
	localActivitySlotSupplierKind string
	nexusSlotSupplierKind         string

	workflowPollerBehavior PollerBehavior
	activityPollerBehavior PollerBehavior
	nexusPollerBehavior    PollerBehavior

	// For delta calculations between heartbeats (mutated by PopulateHeartbeat).
	prevWorkflowProcessed      *int64
	prevWorkflowFailed         *int64
	prevActivityProcessed      *int64
	prevActivityFailed         *int64
	prevLocalActivityProcessed *int64
	prevLocalActivityFailed    *int64
	prevNexusProcessed         *int64
	prevNexusFailed            *int64

	pollTimeTracker *pollTimeTracker
}

// PopulateHeartbeat fills in the metrics-related fields of the WorkerHeartbeat proto.
func (h *heartbeatMetricsHandler) PopulateHeartbeat(hb *workerpb.WorkerHeartbeat, opts *populateHeartbeatOptions) {
	_ = "STUB: not implemented"
	return
}

func (h *heartbeatMetricsHandler) Unwrap() metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func buildSlotsInfo(
	supplierKind string,
	slotsAvailable int32,
	slotsUsed int32,
	totalProcessed int64,
	totalFailed int64,
	prevProcessed *int64,
	prevFailed *int64,
) *workerpb.WorkerSlotsInfo {
	_ = "STUB: not implemented"
	return nil
}

func buildPollerInfo(currentPollers int32, lastSuccessfulPollTime time.Time, pollerBehavior PollerBehavior) *workerpb.WorkerPollerInfo {
	_ = "STUB: not implemented"
	return nil
}

// capturingCounter wraps a counter and captures its value in memory.
type capturingCounter struct {
	underlying metrics.Counter
	value      *atomic.Int64
}

func (c *capturingCounter) Inc(delta int64) { _ = "STUB: not implemented"; return }

// capturingGauge wraps a gauge and captures its value in memory.
type capturingGauge struct {
	underlying metrics.Gauge
	value      *atomic.Int64
}

func (g *capturingGauge) Update(f float64) { _ = "STUB: not implemented"; return }

// capturingTimer wraps a timer and increments a counter each time Record is called.
type capturingTimer struct {
	underlying metrics.Timer
	counter    *atomic.Int64
}

func (t *capturingTimer) Record(d time.Duration) { _ = "STUB: not implemented"; return }
