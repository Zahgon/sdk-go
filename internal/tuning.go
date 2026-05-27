package internal

import (
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"

	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

// WorkerTuner allows for the dynamic customization of some aspects of worker behavior.
// Exposed as: [go.temporal.io/sdk/worker.WorkerTuner]
type WorkerTuner interface {
	// GetWorkflowTaskSlotSupplier returns the SlotSupplier used for workflow tasks.
	GetWorkflowTaskSlotSupplier() SlotSupplier
	// GetActivityTaskSlotSupplier returns the SlotSupplier used for activity tasks.
	GetActivityTaskSlotSupplier() SlotSupplier
	// GetLocalActivitySlotSupplier returns the SlotSupplier used for local activities.
	GetLocalActivitySlotSupplier() SlotSupplier
	// GetNexusSlotSupplier returns the SlotSupplier used for nexus tasks.
	GetNexusSlotSupplier() SlotSupplier
	// GetSessionActivitySlotSupplier returns the SlotSupplier used for activities within sessions.
	GetSessionActivitySlotSupplier() SlotSupplier
}

// SlotPermit is a permit to use a slot.
// Exposed as: [go.temporal.io/sdk/worker.SlotPermit]
type SlotPermit struct {
	// UserData is a field that can be used to store arbitrary on a permit by SlotSupplier
	// implementations.
	UserData any
	// Specifically eager activities need to keep track of their own concurrent max separately and
	// this helps them do that. It can be used for other specific use cases in the future.
	extraReleaseCallback func()
}

// SlotReservationInfo contains information that SlotSupplier instances can use during
// reservation calls. It embeds a standard Context.
//
// Exposed as: [go.temporal.io/sdk/worker.SlotReservationInfo]
type SlotReservationInfo interface {
	// TaskQueue returns the task queue for which a slot is being reserved. In the case of local
	// activities, this is the same as the workflow's task queue.
	TaskQueue() string
	// WorkerBuildId returns the build ID of the worker that is reserving the slot.
	WorkerBuildId() string
	// WorkerBuildId returns the build ID of the worker that is reserving the slot.
	WorkerIdentity() string
	// NumIssuedSlots returns the current number of slots that have already been issued by the
	// supplier. This value may change over the course of the reservation.
	NumIssuedSlots() int
	// Logger returns an appropriately tagged logger.
	Logger() log.Logger
	// MetricsHandler returns an appropriately tagged metrics handler that can be used to record
	// custom metrics.
	MetricsHandler() metrics.Handler
}

// SlotMarkUsedInfo contains information that SlotSupplier instances can use during
// SlotSupplier.MarkSlotUsed calls.
//
// Exposed as: [go.temporal.io/sdk/worker.SlotMarkUsedInfo]
type SlotMarkUsedInfo interface {
	// Permit returns the permit that is being marked as used.
	Permit() *SlotPermit
	// Logger returns an appropriately tagged logger.
	Logger() log.Logger
	// MetricsHandler returns an appropriately tagged metrics handler that can be used to record
	// custom metrics.
	MetricsHandler() metrics.Handler
}

// SlotReleaseReason describes the reason that a slot is being released.
type SlotReleaseReason int

const (
	SlotReleaseReasonTaskProcessed SlotReleaseReason = iota
	SlotReleaseReasonUnused
)

// SlotReleaseInfo contains information that SlotSupplier instances can use during
// SlotSupplier.ReleaseSlot calls.
//
// Exposed as: [go.temporal.io/sdk/worker.SlotReleaseInfo]
type SlotReleaseInfo interface {
	// Permit returns the permit that is being released.
	Permit() *SlotPermit
	// Reason returns the reason that the slot is being released.
	Reason() SlotReleaseReason
	// Logger returns an appropriately tagged logger.
	Logger() log.Logger
	// MetricsHandler returns an appropriately tagged metrics handler that can be used to record
	// custom metrics.
	MetricsHandler() metrics.Handler
}

// SlotSupplier controls how slots are handed out for workflow and activity tasks as well as
// local activities when used in conjunction with a WorkerTuner.
// Exposed as: [go.temporal.io/sdk/worker.SlotSupplier]
type SlotSupplier interface {
	// ReserveSlot is called before polling for new tasks. The implementation should block until
	// a slot is available, then return a permit to use that slot. Implementations must be
	// thread-safe.
	//
	// Any returned error besides context.Canceled will be logged and the function will be retried.
	ReserveSlot(ctx context.Context, info SlotReservationInfo) (*SlotPermit, error)

	// TryReserveSlot is called when attempting to reserve slots for eager workflows and activities.
	// It should return a permit if a slot is available, and nil otherwise. Implementations must be
	// thread-safe.
	TryReserveSlot(info SlotReservationInfo) *SlotPermit

	// MarkSlotUsed is called once a slot is about to be used for actually processing a task.
	// Because slots are reserved before task polling, not all reserved slots will be used.
	// Implementations must be thread-safe.
	MarkSlotUsed(info SlotMarkUsedInfo)

	// ReleaseSlot is called when a slot is no longer needed, which is typically after the task
	// has been processed, but may also be called upon shutdown or other situations where the
	// slot is no longer needed. Implementations must be thread-safe.
	ReleaseSlot(info SlotReleaseInfo)

	// MaxSlots returns the maximum number of slots that this supplier will ever issue.
	// Implementations may return 0 if there is no well-defined upper limit. In such cases the
	// available task slots metric will not be emitted.
	MaxSlots() int
}

func getSlotSupplierKind(s SlotSupplier) string { _ = "STUB: not implemented"; return "" }

// CompositeTuner allows you to build a tuner from multiple slot suppliers.
type CompositeTuner struct {
	workflowSlotSupplier        SlotSupplier
	activitySlotSupplier        SlotSupplier
	localActivitySlotSupplier   SlotSupplier
	nexusSlotSupplier           SlotSupplier
	sessionActivitySlotSupplier SlotSupplier
}

func (c *CompositeTuner) GetWorkflowTaskSlotSupplier() SlotSupplier {
	_ = "STUB: not implemented"
	return *new(SlotSupplier)
}

func (c *CompositeTuner) GetActivityTaskSlotSupplier() SlotSupplier {
	_ = "STUB: not implemented"
	return *new(SlotSupplier)
}

func (c *CompositeTuner) GetLocalActivitySlotSupplier() SlotSupplier {
	_ = "STUB: not implemented"
	return *new(SlotSupplier)
}

func (c *CompositeTuner) GetNexusSlotSupplier() SlotSupplier {
	_ = "STUB: not implemented"
	return *new(SlotSupplier)
}

func (c *CompositeTuner) GetSessionActivitySlotSupplier() SlotSupplier {
	_ = "STUB: not implemented"
	return *new(SlotSupplier)
}

// CompositeTunerOptions are the options used by NewCompositeTuner.
//
// Exposed as: [go.temporal.io/sdk/worker.CompositeTunerOptions]
type CompositeTunerOptions struct {
	// WorkflowSlotSupplier is the SlotSupplier used for workflow tasks.
	WorkflowSlotSupplier SlotSupplier
	// ActivitySlotSupplier is the SlotSupplier used for activity tasks.
	ActivitySlotSupplier SlotSupplier
	// LocalActivitySlotSupplier is the SlotSupplier used for local activities.
	LocalActivitySlotSupplier SlotSupplier
	// NexusSlotSupplier is the SlotSupplier used for nexus tasks.
	NexusSlotSupplier SlotSupplier
	// SessionActivitySlotSupplier is the SlotSupplier used for activities within sessions.
	SessionActivitySlotSupplier SlotSupplier
}

// NewCompositeTuner creates a WorkerTuner that uses a combination of slot suppliers.
// Exposed as: [go.temporal.io/sdk/worker.NewCompositeTuner]
func NewCompositeTuner(options CompositeTunerOptions) (WorkerTuner, error) {
	_ = "STUB: not implemented"
	return *new(WorkerTuner), nil
}

// FixedSizeTunerOptions are the options used by NewFixedSizeTuner.
//
// Exposed as: [go.temporal.io/sdk/worker.FixedSizeTunerOptions]
type FixedSizeTunerOptions struct {
	// NumWorkflowSlots is the number of slots available for workflow tasks.
	NumWorkflowSlots int
	// NumActivitySlots is the number of slots available for activity tasks.
	NumActivitySlots int
	// NumLocalActivitySlots is the number of slots available for local activities.
	NumLocalActivitySlots int
	// NumNexusSlots is the number of slots available for nexus tasks.
	NumNexusSlots int
}

// NewFixedSizeTuner creates a WorkerTuner that uses fixed size slot suppliers.
//
// Exposed as: [go.temporal.io/sdk/worker.NewFixedSizeTuner]
func NewFixedSizeTuner(options FixedSizeTunerOptions) (WorkerTuner, error) {
	_ = "STUB: not implemented"
	return *new(WorkerTuner), nil
}

// FixedSizeSlotSupplier is a slot supplier that will only ever issue at most a fixed number of
// slots.
type FixedSizeSlotSupplier struct {
	numSlots int
	sem      *semaphore.Weighted
}

// NewFixedSizeSlotSupplier creates a new FixedSizeSlotSupplier with the given number of slots.
//
// Exposed as: [go.temporal.io/sdk/worker.NewFixedSizeSlotSupplier]
func NewFixedSizeSlotSupplier(numSlots int) (*FixedSizeSlotSupplier, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (f *FixedSizeSlotSupplier) ReserveSlot(ctx context.Context, _ SlotReservationInfo) (
	*SlotPermit, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (f *FixedSizeSlotSupplier) TryReserveSlot(SlotReservationInfo) *SlotPermit {
	_ = "STUB: not implemented"
	return nil
}

func (f *FixedSizeSlotSupplier) MarkSlotUsed(SlotMarkUsedInfo) { _ = "STUB: not implemented"; return }
func (f *FixedSizeSlotSupplier) ReleaseSlot(SlotReleaseInfo)   { _ = "STUB: not implemented"; return }

func (f *FixedSizeSlotSupplier) MaxSlots() int { _ = "STUB: not implemented"; return 0 }

type slotReservationData struct {
	taskQueue string
}

type slotReserveInfoImpl struct {
	taskQueue      string
	workerBuildId  string
	workerIdentity string
	issuedSlots    *atomic.Int32
	logger         log.Logger
	metrics        metrics.Handler
}

func (s slotReserveInfoImpl) TaskQueue() string { _ = "STUB: not implemented"; return "" }

func (s slotReserveInfoImpl) WorkerBuildId() string { _ = "STUB: not implemented"; return "" }

func (s slotReserveInfoImpl) WorkerIdentity() string { _ = "STUB: not implemented"; return "" }

func (s slotReserveInfoImpl) NumIssuedSlots() int { _ = "STUB: not implemented"; return 0 }

func (s slotReserveInfoImpl) Logger() log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (s slotReserveInfoImpl) MetricsHandler() metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

type slotMarkUsedContextImpl struct {
	permit  *SlotPermit
	logger  log.Logger
	metrics metrics.Handler
}

func (s slotMarkUsedContextImpl) Permit() *SlotPermit { _ = "STUB: not implemented"; return nil }

func (s slotMarkUsedContextImpl) Logger() log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (s slotMarkUsedContextImpl) MetricsHandler() metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

type slotReleaseContextImpl struct {
	permit  *SlotPermit
	reason  SlotReleaseReason
	logger  log.Logger
	metrics metrics.Handler
}

func (s slotReleaseContextImpl) Permit() *SlotPermit { _ = "STUB: not implemented"; return nil }

func (s slotReleaseContextImpl) Reason() SlotReleaseReason {
	_ = "STUB: not implemented"
	return *new(SlotReleaseReason)
}

func (s slotReleaseContextImpl) Logger() log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (s slotReleaseContextImpl) MetricsHandler() metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

type trackingSlotSupplier struct {
	inner          SlotSupplier
	logger         log.Logger
	metrics        metrics.Handler
	workerBuildId  string
	workerIdentity string

	issuedSlotsAtomic atomic.Int32
	slotsMutex        sync.Mutex
	// Values should eventually become slot info types
	usedSlots               map[*SlotPermit]struct{}
	taskSlotsAvailableGauge metrics.Gauge
	taskSlotsUsedGauge      metrics.Gauge
}

type trackingSlotSupplierOptions struct {
	logger         log.Logger
	metricsHandler metrics.Handler
	workerBuildId  string
	workerIdentity string
}

func newTrackingSlotSupplier(inner SlotSupplier, options trackingSlotSupplierOptions) *trackingSlotSupplier {
	_ = "STUB: not implemented"
	return nil
}

func (t *trackingSlotSupplier) ReserveSlot(
	ctx context.Context,
	data *slotReservationData,
) (*SlotPermit, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (t *trackingSlotSupplier) TryReserveSlot(data *slotReservationData) *SlotPermit {
	_ = "STUB: not implemented"
	return nil
}

func (t *trackingSlotSupplier) MarkSlotUsed(permit *SlotPermit) { _ = "STUB: not implemented"; return }

func (t *trackingSlotSupplier) ReleaseSlot(permit *SlotPermit, reason SlotReleaseReason) {
	_ = "STUB: not implemented"
	return
}

func (t *trackingSlotSupplier) publishMetrics(usedSlots int) { _ = "STUB: not implemented"; return }

func (t *trackingSlotSupplier) GetSlotSupplierKind() string { _ = "STUB: not implemented"; return "" }
