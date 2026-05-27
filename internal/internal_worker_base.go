package internal

// All code in this file is private to the package.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"golang.org/x/time/rate"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/backoff"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	retryPollOperationInitialInterval         = 200 * time.Millisecond
	retryPollOperationMaxInterval             = 10 * time.Second
	retryPollResourceExhaustedInitialInterval = time.Second
	retryPollResourceExhaustedMaxInterval     = 10 * time.Second
	// How long the same poll task error can remain suppressed
	lastPollTaskErrSuppressTime     = 1 * time.Minute
	pollerAutoscalingReportInterval = 100 * time.Millisecond
)

var (
	pollOperationRetryPolicy         = createPollRetryPolicy()
	pollResourceExhaustedRetryPolicy = createPollResourceExhaustedRetryPolicy()
	retryLongPollGracePeriod         = 2 * time.Minute
	errStop                          = errors.New("worker stopping")
	// ErrWorkerStopped is returned when the worker is stopped
	//
	// Exposed as: [go.temporal.io/sdk/worker.ErrWorkerShutdown]
	ErrWorkerShutdown = errors.New("worker is now shutdown")
)

type (
	// ResultHandler that returns result
	ResultHandler func(result *commonpb.Payloads, err error)
	// LocalActivityResultHandler that returns local activity result
	LocalActivityResultHandler func(lar *LocalActivityResultWrapper)

	// LocalActivityResultWrapper contains the result of a local activity
	LocalActivityResultWrapper struct {
		Err     error
		Result  *commonpb.Payloads
		Attempt int32
		Backoff time.Duration
	}

	LocalActivityMarkerParams struct {
		Summary string
	}

	ExecuteNexusOperationParams struct {
		client      NexusClient
		operation   string
		input       *commonpb.Payload
		options     NexusOperationOptions
		nexusHeader map[string]string
	}
)

// NewExecuteNexusOperationParams builds a parameters struct for
// WorkflowEnvironment.ExecuteNexusOperation. Exposed so that non-Go SDKs
// (e.g. roadrunner-temporal proxying for PHP) can populate the struct from
// outside the `internal` package — the fields themselves stay unexported to
// keep the SDK free to evolve them.
func NewExecuteNexusOperationParams(
	client NexusClient,
	operation string,
	input *commonpb.Payload,
	options NexusOperationOptions,
	nexusHeader map[string]string,
) ExecuteNexusOperationParams {
	_ = "STUB: not implemented"
	return *new(ExecuteNexusOperationParams)
}

type (

	// WorkflowEnvironment Represents the environment for workflow.
	// Should only be used within the scope of workflow definition.
	WorkflowEnvironment interface {
		AsyncActivityClient
		LocalActivityClient
		WorkflowTimerClient
		SideEffect(f func() (*commonpb.Payloads, error), callback ResultHandler, summary string)
		GetVersion(changeID string, minSupported, maxSupported Version) Version
		WorkflowInfo() *WorkflowInfo
		TypedSearchAttributes() SearchAttributes
		Complete(result *commonpb.Payloads, err error)
		RegisterCancelHandler(handler func())
		RequestCancelChildWorkflow(namespace, workflowID string)
		RequestCancelExternalWorkflow(namespace, workflowID, runID string, callback ResultHandler)
		ExecuteChildWorkflow(params ExecuteWorkflowParams, callback ResultHandler, startedHandler func(r WorkflowExecution, e error))
		ExecuteNexusOperation(params ExecuteNexusOperationParams, callback func(*commonpb.Payload, error), startedHandler func(token string, e error)) int64
		RequestCancelNexusOperation(seq int64)
		GetLogger() log.Logger
		GetMetricsHandler() metrics.Handler
		// Must be called before WorkflowDefinition.Execute returns
		RegisterSignalHandler(
			handler func(name string, input *commonpb.Payloads, header *commonpb.Header) error,
		)
		SignalExternalWorkflow(
			namespace string,
			workflowID string,
			runID string,
			signalName string,
			input *commonpb.Payloads,
			arg interface{},
			header *commonpb.Header,
			childWorkflowOnly bool,
			callback ResultHandler,
		)
		RegisterQueryHandler(
			handler func(queryType string, queryArgs *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error),
		)
		RegisterUpdateHandler(
			handler func(string, string, *commonpb.Payloads, *commonpb.Header, UpdateCallbacks),
		)
		IsReplaying() bool
		MutableSideEffect(id string, f func() interface{}, equals func(a, b interface{}) bool, summary string) converter.EncodedValue
		GetDataConverter() converter.DataConverter
		GetFailureConverter() converter.FailureConverter
		AddSession(sessionInfo *SessionInfo)
		RemoveSession(sessionID string)
		GetContextPropagators() []ContextPropagator
		UpsertSearchAttributes(attributes map[string]interface{}) error
		UpsertTypedSearchAttributes(attributes SearchAttributes) error
		UpsertMemo(memoMap map[string]interface{}) error
		GetRegistry() *registry
		// QueueUpdate request of type name
		QueueUpdate(name string, f func())
		// HandleQueuedUpdates unblocks all queued updates of type name
		HandleQueuedUpdates(name string)
		// DrainUnhandledUpdates unblocks all updates, meant to be used to drain
		// all unhandled updates at the end of a workflow task
		// returns true if any update was unblocked
		DrainUnhandledUpdates() bool
		// TryUse returns true if this flag may currently be used.
		TryUse(flag sdkFlag) bool
		GenerateSequence() int64
	}

	// WorkflowDefinitionFactory factory for creating WorkflowDefinition instances.
	WorkflowDefinitionFactory interface {
		// NewWorkflowDefinition must return a new instance of WorkflowDefinition on each call.
		NewWorkflowDefinition() WorkflowDefinition
	}

	// WorkflowDefinition wraps the code that can execute a workflow.
	WorkflowDefinition interface {
		// Execute implementation must be asynchronous.
		Execute(env WorkflowEnvironment, header *commonpb.Header, input *commonpb.Payloads)
		// OnWorkflowTaskStarted is called for each non timed out startWorkflowTask event.
		// Executed after all history events since the previous commands are applied to WorkflowDefinition
		// Application level code must be executed from this function only.
		// Execute call as well as callbacks called from WorkflowEnvironment functions can only schedule callbacks
		// which can be executed from OnWorkflowTaskStarted().
		OnWorkflowTaskStarted(deadlockDetectionTimeout time.Duration)
		// StackTrace of all coroutines owned by the Dispatcher instance.
		StackTrace() string
		// Close destroys all coroutines without waiting for their completion
		Close()
	}

	scalableTaskPoller struct {
		taskPollerType string
		// pollerCount is the number of pollers tasks to start. There may be less than this
		// due to limited slots, rate limiting, or poller autoscaling.
		pollerCount                  int
		taskPoller                   taskPoller
		pollerAutoscalerReportHandle *pollScalerReportHandle
		pollerSemaphore              *pollerSemaphore
	}

	// baseWorkerOptions options to configure base worker.
	baseWorkerOptions struct {
		pollerRate              int
		slotSupplier            SlotSupplier
		maxTaskPerSecond        float64
		taskPollers             []scalableTaskPoller
		taskProcessor           taskProcessor
		workerType              string
		identity                string
		buildId                 string
		deploymentOptions       WorkerDeploymentOptions
		logger                  log.Logger
		stopTimeout             time.Duration
		fatalErrCb              func(error)
		backgroundContextCancel context.CancelCauseFunc
		metricsHandler          metrics.Handler
		sessionTokenBucket      *sessionTokenBucket
		slotReservationData     slotReservationData
		isInternalWorker        bool
	}

	// baseWorker that wraps worker activities.
	baseWorker struct {
		options              baseWorkerOptions
		isWorkerStarted      bool
		stopCh               chan struct{}  // Channel used to stop the go routines.
		stopWG               sync.WaitGroup // The WaitGroup for stopping existing routines.
		pollLimiter          *rate.Limiter
		taskLimiter          *rate.Limiter
		limiterContext       context.Context
		limiterContextCancel func()
		retrier              *backoff.ConcurrentRetrier // Service errors back off retrier
		logger               log.Logger
		metricsHandler       metrics.Handler

		slotSupplier       *trackingSlotSupplier
		taskQueueCh        chan eagerOrPolledTask
		eagerTaskQueueCh   chan eagerTask
		fatalErrCb         func(error)
		sessionTokenBucket *sessionTokenBucket
		pollerBalancer     *pollerBalancer

		lastPollTaskErrMessage string
		lastPollTaskErrStarted time.Time
		lastPollTaskErrLock    sync.Mutex

		noRepoll atomic.Bool
	}

	eagerOrPolledTask interface {
		getTask() taskForWorker
		getPermit() *SlotPermit
	}

	polledTask struct {
		task   taskForWorker
		permit *SlotPermit
	}

	eagerTask struct {
		// task to process.
		task   taskForWorker
		permit *SlotPermit
	}

	pollScalerReportHandleOptions struct {
		initialPollerCount        int
		maxPollerCount            int
		minPollerCount            int
		logger                    log.Logger
		scaleCallback             func(int)
		serverSupportsAutoscaling *atomic.Bool
	}

	pollScalerReportHandle struct {
		minPollerCount            int
		maxPollerCount            int
		logger                    log.Logger
		target                    atomic.Int64
		scaleCallback             func(int)
		everSawScalingDecision    atomic.Bool
		serverSupportsAutoscaling *atomic.Bool
		ingestedThisPeriod        atomic.Int64
		ingestedLastPeriod        atomic.Int64
		scaleUpAllowed            atomic.Bool
	}

	barrier chan struct{}

	// pollerSemaphore is a semaphore that limits the number of concurrent pollers.
	// it is effectively a resizable semaphore.
	pollerSemaphore struct {
		maxPermits int
		permits    int
		bs         chan barrier
	}

	// pollerBalancer is used to balance the number of poll requests from different poller types
	pollerBalancer struct {
		pollerCount   map[string]int
		pollerBarrier map[string]barrier
		mu            sync.Mutex
	}
)

func (h ResultHandler) wrap(callback ResultHandler) ResultHandler {
	_ = "STUB: not implemented"
	return *new(ResultHandler)
}

func (t *polledTask) getTask() taskForWorker { _ = "STUB: not implemented"; return *new(taskForWorker) }

func (t *polledTask) getPermit() *SlotPermit { _ = "STUB: not implemented"; return nil }

func (t *eagerTask) getTask() taskForWorker { _ = "STUB: not implemented"; return *new(taskForWorker) }

func (t *eagerTask) getPermit() *SlotPermit {
	_ = "STUB: not implemented"

	// SetRetryLongPollGracePeriod sets the amount of time a long poller retries on
	// fatal errors before it actually fails. For test use only,
	// not safe to call with a running worker.
	return nil
}

func SetRetryLongPollGracePeriod(period time.Duration) { _ = "STUB: not implemented"; return }

func getRetryLongPollGracePeriod() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func createPollRetryPolicy() backoff.RetryPolicy {
	_ = "STUB: not implemented"
	return *new(backoff.RetryPolicy)
}

// NOTE: We don't use expiration interval since we don't use retries from retrier class.
// We use it to calculate next backoff. We have additional layer that is built on poller
// in the worker layer for to add some middleware for any poll retry that includes
// (a) rate limiting across pollers (b) back-off across pollers when server is busy
// We don't ever expire

func createPollResourceExhaustedRetryPolicy() backoff.RetryPolicy {
	_ = "STUB: not implemented"
	return *new(backoff.RetryPolicy)
}

func newBaseWorker(
	options baseWorkerOptions,
) *baseWorker {
	_ = "STUB: not implemented"
	return nil
}

// No buffer, so pollers are only able to poll for new tasks after the previous one is
// dispatched.

// Allow enough capacity so that eager dispatch will not block. There's an upper limit of
// 2k pending activities so this channel never needs to be larger than that.

// Set secondary retrier as resource exhausted

// If we have multiple task workers, we need to balance the pollers

// Start starts a fixed set of routines to do the work.
func (bw *baseWorker) Start() { _ = "STUB: not implemented"; return }

func (bw *baseWorker) isStop() bool { _ = "STUB: not implemented"; return false }

func (bw *baseWorker) runPoller(taskWorker scalableTaskPoller) { _ = "STUB: not implemented"; return }

// Note: With poller autoscaling, this metric doesn't make a lot of sense since the number of pollers can go up and down.

// Call the balancer to make sure one poller type doesn't starve the others of slots.

// There was an error reserving a slot
// Avoid spamming reserve hard in the event it's constantly failing

func (bw *baseWorker) tryReserveSlot() *SlotPermit { _ = "STUB: not implemented"; return nil }

func (bw *baseWorker) releaseSlot(permit *SlotPermit, reason SlotReleaseReason) {
	_ = "STUB: not implemented"
	return
}

func (bw *baseWorker) pushEagerTask(task eagerTask) {
	_ = "STUB: not implemented"
	// Should always be non-blocking. Slots are reserved before requesting eager tasks.
	return
}

func (bw *baseWorker) getDeploymentOptions() WorkerDeploymentOptions {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentOptions)
}

func (bw *baseWorker) processTaskAsync(eagerOrPolled eagerOrPolledTask) {
	_ = "STUB: not implemented"
	return
}

func (bw *baseWorker) runTaskDispatcher() { _ = "STUB: not implemented"; return }

// wait for new task or worker stop

// Currently we can drop any tasks received when closing.
// https://github.com/temporalio/sdk-go/issues/1197

// for non-polled-task (local activity result as task or eager task), we don't need to rate limit

func (bw *baseWorker) runEagerTaskDispatcher() { _ = "STUB: not implemented"; return }

// drain eager dispatch queue

func (bw *baseWorker) pollTask(taskWorker scalableTaskPoller, slotPermit *SlotPermit) {
	_ = "STUB: not implemented"
	return
}

// We retry "non retriable" errors while long polling for a while, because some proxies return
// unexpected values causing unnecessary downtime.

// We use the secondary retrier on resource exhausted

func (bw *baseWorker) logPollTaskError(err error) {
	_ = "STUB: not implemented"
	// We do not want to log any errors after we were explicitly stopped
	return
}

// No error means reset the message and time

// Ignore connection loss on server shutdown. This helps with quiescing spurious error messages
// upon server shutdown (where server is using the SDK).

// Log the error as warn if it doesn't match the last error seen or its over
// the time since

func isNonRetriableError(err error) bool { _ = "STUB: not implemented"; return false }

// Stop is a blocking call and cleans up all the resources associated with worker.
func (bw *baseWorker) Stop() { _ = "STUB: not implemented"; return }

// Close context

func newPollScalerReportHandle(options pollScalerReportHandleOptions) *pollScalerReportHandle {
	_ = "STUB: not implemented"
	return nil
}

func (prh *pollScalerReportHandle) handleTask(task taskForWorker) {
	_ = "STUB: not implemented"
	return
}

// We want to avoid scaling down on empty polls if the server has never made any
// scaling decisions - otherwise we might never scale up again. If the server
// supports poller autoscaling, it's safe to scale down without having seen a
// decision.

func (prh *pollScalerReportHandle) updateTarget(f func(int64) int64) {
	_ = "STUB: not implemented"
	return
}

func (prh *pollScalerReportHandle) handleError(err error) {
	_ = "STUB: not implemented"
	// If we have never seen a scaling decision and the server doesn't support
	// poller autoscaling, we don't want to scale down on errors, because we
	// might never scale up again.
	return
}

func (prh *pollScalerReportHandle) run(stopCh <-chan struct{}) { _ = "STUB: not implemented"; return }

// Here we periodically check if we should permit increasing the
// poller count further. We do this by comparing the number of ingested items in the
// current period with the number of ingested items in the previous period. If we
// are successfully ingesting more items, then it makes sense to allow scaling up.
// If we aren't, then we're probably limited by how fast we can process the tasks
// and it's not worth increasing the poller count further.

func (prh *pollScalerReportHandle) newPeriod() { _ = "STUB: not implemented"; return }

func newPollerSemaphore(maxPermits int) *pollerSemaphore { _ = "STUB: not implemented"; return nil }

func (ps *pollerSemaphore) acquire(ctx context.Context) error {
	_ = "STUB: not implemented"

	// Acquire barrier.
	return nil
}

// Release barrier.

// Release barrier.

func (ps *pollerSemaphore) release() {
	_ = "STUB: not implemented"
	// Acquire barrier.
	return
}

// Release one waiter if there are any waiting.

// Release barrier.

func (ps *pollerSemaphore) updatePermits(maxPermits int) {
	_ = "STUB: not implemented"
	// Acquire barrier.
	return
}

// Release barrier.

func newScalableTaskPoller(
	poller taskPoller,
	logger log.Logger,
	pollerBehavior PollerBehavior,
	taskPollerType string,
	serverSupportsAutoscaling *atomic.Bool,
) scalableTaskPoller {
	_ = "STUB: not implemented"
	return *new(scalableTaskPoller)
}

// balance checks if the poller type is balanced with other poller types. The goal is to ensure that
// at least one poller of each type is running before allowing any poller of the given type to increase.
func (pb *pollerBalancer) balance(ctx context.Context, pollerType string) error {
	_ = "STUB: not implemented"
	return nil

	// If there are no pollers of this type, we can skip balancing.
	// This check must happen before iterating the map to avoid
	// non-deterministic map iteration visiting another type first
	// and unnecessarily blocking on its barrier.
}

// Check if all other poller types have at least one poller running.

// If all other poller types have at least one poller running, we are balanced

// If we have a barrier that means that at least one other poller type has no pollers running.
// We need to wait for that poller type to start a poller before we can continue.

func (pb *pollerBalancer) registerPollerType(pollerType string) { _ = "STUB: not implemented"; return }

func (pb *pollerBalancer) incrementPoller(pollerType string) { _ = "STUB: not implemented"; return }

func (pb *pollerBalancer) decrementPoller(pollerType string) { _ = "STUB: not implemented"; return }
