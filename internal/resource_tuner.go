package internal

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

// Metric names emitted by the resource-based tuner
const (
	resourceSlotsCPUUsage = "temporal_resource_slots_cpu_usage"
	resourceSlotsMemUsage = "temporal_resource_slots_mem_usage"
)

// SysInfoProvider implementations provide information about system resources.
//
// Exposed as: [go.temporal.io/sdk/worker.SysInfoProvider]
type SysInfoProvider interface {
	// MemoryUsage returns the current system memory usage as a fraction of total memory between
	// 0 and 1.
	MemoryUsage(infoContext *SysInfoContext) (float64, error)
	// CpuUsage returns the current system CPU usage as a fraction of total CPU usage between 0
	// and 1.
	CpuUsage(infoContext *SysInfoContext) (float64, error)
}

// SysInfoContext provides context for SysInfoProvider calls.
//
// Exposed as: [go.temporal.io/sdk/worker.SysInfoContext]
type SysInfoContext struct {
	Logger log.Logger
}

// HasSysInfoProvider is an optional interface that SlotSupplier implementations can implement
// to expose their SysInfoProvider. This allows the SDK to access system metrics (CPU/memory)
// for features like worker heartbeats without coupling to specific SlotSupplier implementations.
//
// Exposed as: [go.temporal.io/sdk/worker.HasSysInfoProvider]
type HasSysInfoProvider interface {
	SysInfoProvider() SysInfoProvider
}

// ResourceBasedTunerOptions configures a resource-based tuner.
//
// Exposed as: [go.temporal.io/sdk/worker.ResourceBasedTunerOptions]
type ResourceBasedTunerOptions struct {
	// TargetMem is the target overall system memory usage as value 0 and 1 that the controller will
	// attempt to maintain. Must be set nonzero.
	TargetMem float64
	// TargetCpu is the target overall system CPU usage as value 0 and 1 that the controller will
	// attempt to maintain. Must be set nonzero.
	TargetCpu float64
	// InfoSupplier provides CPU and memory usage information. This is required.
	// Use contrib/sysinfo.SysInfoProvider() for a gopsutil-based implementation.
	InfoSupplier SysInfoProvider
	// Passed to ResourceBasedSlotSupplierOptions.RampThrottle for activities.
	// If not set, the default value is 50ms.
	ActivityRampThrottle time.Duration
	// Passed to ResourceBasedSlotSupplierOptions.RampThrottle for workflows.
	// If not set, the default value is 0ms.
	WorkflowRampThrottle time.Duration
}

// NewResourceBasedTuner creates a WorkerTuner that dynamically adjusts the number of slots based
// on system resources. Specify the target CPU and memory usage as a value between 0 and 1.
//
// InfoSupplier is required - use contrib/sysinfo.SysInfoProvider() for a gopsutil-based
// implementation, or provide your own.
//
// Exposed as: [go.temporal.io/sdk/worker.NewResourceBasedTuner]
func NewResourceBasedTuner(opts ResourceBasedTunerOptions) (WorkerTuner, error) {
	_ = "STUB: not implemented"
	return *new(WorkerTuner), nil
}

// ResourceBasedSlotSupplierOptions configures a particular ResourceBasedSlotSupplier.
//
// Exposed as: [go.temporal.io/sdk/worker.ResourceBasedSlotSupplierOptions]
type ResourceBasedSlotSupplierOptions struct {
	// MinSlots is minimum number of slots that will be issued without any resource checks.
	MinSlots int
	// MaxSlots is the maximum number of slots that will ever be issued.
	MaxSlots int
	// RampThrottle is time to wait between slot issuance. This value matters (particularly for
	// activities) because how many resources a task will use cannot be determined ahead of time,
	// and thus the system should wait to see how much resources are used before issuing more slots.
	RampThrottle time.Duration
}

// DefaultWorkflowResourceBasedSlotSupplierOptions returns default options for workflow slot suppliers.
//
// Exposed as: [go.temporal.io/sdk/worker.DefaultWorkflowResourceBasedSlotSupplierOptions]
func DefaultWorkflowResourceBasedSlotSupplierOptions() ResourceBasedSlotSupplierOptions {
	_ = "STUB: not implemented"
	return *new(ResourceBasedSlotSupplierOptions)
}

// DefaultActivityResourceBasedSlotSupplierOptions returns default options for activity slot suppliers.
//
// Exposed as: [go.temporal.io/sdk/worker.DefaultActivityResourceBasedSlotSupplierOptions]
func DefaultActivityResourceBasedSlotSupplierOptions() ResourceBasedSlotSupplierOptions {
	_ = "STUB: not implemented"
	return *new(ResourceBasedSlotSupplierOptions)
}

// ResourceBasedSlotSupplier is a SlotSupplier that issues slots based on system resource usage.
//
// Exposed as: [go.temporal.io/sdk/worker.ResourceBasedSlotSupplier]
type ResourceBasedSlotSupplier struct {
	controller *ResourceController
	options    ResourceBasedSlotSupplierOptions

	lastIssuedMu     sync.Mutex
	lastSlotIssuedAt time.Time
}

// NewResourceBasedSlotSupplier creates a ResourceBasedSlotSupplier given the provided
// ResourceController and ResourceBasedSlotSupplierOptions. All ResourceBasedSlotSupplier instances
// must use the same ResourceController.
//
// Exposed as: [go.temporal.io/sdk/worker.NewResourceBasedSlotSupplier]
func NewResourceBasedSlotSupplier(
	controller *ResourceController,
	options ResourceBasedSlotSupplierOptions,
) (*ResourceBasedSlotSupplier, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (r *ResourceBasedSlotSupplier) ReserveSlot(ctx context.Context, info SlotReservationInfo) (*SlotPermit, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (r *ResourceBasedSlotSupplier) TryReserveSlot(info SlotReservationInfo) *SlotPermit {
	_ = "STUB: not implemented"
	return nil
}

func (r *ResourceBasedSlotSupplier) MarkSlotUsed(SlotMarkUsedInfo) {
	_ = "STUB: not implemented"
	return
}
func (r *ResourceBasedSlotSupplier) ReleaseSlot(SlotReleaseInfo) { _ = "STUB: not implemented"; return }
func (r *ResourceBasedSlotSupplier) MaxSlots() int {
	_ = "STUB: not implemented"

	// GetSysInfoProvider returns the SysInfoProvider used by this slot supplier's controller.
	return 0
}

func (r *ResourceBasedSlotSupplier) SysInfoProvider() SysInfoProvider {
	_ = "STUB: not implemented"
	return *new(SysInfoProvider)
}

// ResourceControllerOptions contains configurable parameters for a ResourceController.
// It is recommended to use DefaultResourceControllerOptions to create a ResourceControllerOptions
// and only modify the mem/cpu target percent fields.
//
// Exposed as: [go.temporal.io/sdk/worker.ResourceControllerOptions]
type ResourceControllerOptions struct {
	// MemTargetPercent is the target overall system memory usage as value 0 and 1 that the
	// controller will attempt to maintain.
	MemTargetPercent float64
	// CpuTargetPercent is the target overall system CPU usage as value 0 and 1 that the controller
	// will attempt to maintain.
	CpuTargetPercent float64
	// InfoSupplier is the supplier that the controller will use to get system resources.
	InfoSupplier SysInfoProvider

	MemOutputThreshold float64
	CpuOutputThreshold float64

	MemPGain float64
	MemIGain float64
	MemDGain float64
	CpuPGain float64
	CpuIGain float64
	CpuDGain float64
}

// DefaultResourceControllerOptions returns a ResourceControllerOptions with default values.
//
// Exposed as: [go.temporal.io/sdk/worker.DefaultResourceControllerOptions]
func DefaultResourceControllerOptions() ResourceControllerOptions {
	_ = "STUB: not implemented"
	return *new(ResourceControllerOptions)
}

// pidController implements a simple PID controller for resource-based tuning.
// This is the standard PID formula: output = Kp*error + Ki*integral + Kd*derivative
type pidController struct {
	pGain, iGain, dGain float64

	prevError     float64
	integral      float64
	controlSignal float64
}

func (c *pidController) update(reference, actual float64, dt time.Duration) {
	_ = "STUB: not implemented"
	return
}

// ResourceController is used by ResourceBasedSlotSupplier to make decisions about whether slots
// should be issued based on system resource usage.
//
// Exposed as: [go.temporal.io/sdk/worker.ResourceController]
type ResourceController struct {
	options ResourceControllerOptions

	mu           sync.Mutex
	infoSupplier SysInfoProvider
	lastRefresh  time.Time
	memPid       *pidController
	cpuPid       *pidController
}

// NewResourceController creates a new ResourceController with the provided options.
// WARNING: It is important that you do not create multiple ResourceController instances. Since
// the controller looks at overall system resources, multiple instances with different configs can
// only conflict with one another.
//
// Exposed as: [go.temporal.io/sdk/worker.NewResourceController]
func NewResourceController(options ResourceControllerOptions) *ResourceController {
	_ = "STUB: not implemented"
	return nil
}

func (rc *ResourceController) pidDecision(logger log.Logger, metricsHandler metrics.Handler) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

// Never allow going over the memory target

// This shouldn't be possible with real implementations, but if the elapsed time is 0 the
// PID controller can produce NaNs.

func (rc *ResourceController) publishResourceMetrics(metricsHandler metrics.Handler, memUsage, cpuUsage float64) {
	_ = "STUB: not implemented"
	return
}
