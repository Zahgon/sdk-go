package internal

// All code in this file is private to the package.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	// Server returns empty task after dynamicconfig.MatchingLongPollExpirationInterval (default is 60 seconds).
	// pollTaskServiceTimeOut should be dynamicconfig.MatchingLongPollExpirationInterval + some delta for full round trip to matching
	// because empty task should be returned before timeout is expired (expired timeout counts against SLO).
	pollTaskServiceTimeOut = 70 * time.Second

	stickyWorkflowTaskScheduleToStartTimeoutSeconds = 5

	ratioToForceCompleteWorkflowTaskComplete = 0.8
)

type workflowTaskPollerMode int

const (
	Mixed workflowTaskPollerMode = iota
	NonSticky
	Sticky
)

type (
	// taskPoller interface to poll for tasks
	taskPoller interface {
		// PollTask polls for one new task
		PollTask() (taskForWorker, error)
	}

	// taskProcessor interface to process tasks
	taskProcessor interface {
		// ProcessTask processes a task
		ProcessTask(interface{}) error
	}

	pollerScaleDecision struct {
		pollRequestDeltaSuggestion int
	}

	taskForWorker interface {
		scaleDecision() (pollerScaleDecision, bool)
		isEmpty() bool
	}

	// basePoller is the base class for all poller implementations
	basePoller struct {
		metricsHandler metrics.Handler // base metric handler used for rpc calls
		stopC          <-chan struct{}
		// The worker's build ID, either as defined by the user or automatically set
		workerBuildID string
		// Whether the worker has opted in to the build-id based versioning feature
		useBuildIDVersioning bool
		// The worker's deployment version identifier.
		workerDeploymentVersion WorkerDeploymentVersion
		// Server's capabilities
		capabilities *workflowservice.GetSystemInfoResponse_Capabilities
		// tracks timestamp for last poll request, for worker heartbeating
		pollTimeTracker *pollTimeTracker
		// Unique identifier for worker
		workerInstanceKey string
		// Server cancels polls on shutdown
		workerPollCompleteOnShutdown *atomic.Bool
	}

	// numPollerMetric tracks the number of active pollers and publishes a metric on it.
	numPollerMetric struct {
		lock       sync.Mutex
		numPollers int32
		gauge      metrics.Gauge
	}

	workflowTaskPoller struct {
		basePoller
		mode             workflowTaskPollerMode
		namespace        string
		taskQueueName    string
		identity         string
		service          workflowservice.WorkflowServiceClient
		taskHandler      WorkflowTaskHandler
		contextManager   WorkflowContextManager
		logger           log.Logger
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter

		stickyUUID                   string
		StickyScheduleToStartTimeout time.Duration

		pendingRegularPollCount int
		pendingStickyPollCount  int
		stickyBacklog           int64
		requestLock             sync.Mutex
		stickyCacheSize         int
		eagerActivityExecutor   *eagerActivityExecutor

		numNormalPollerMetric *numPollerMetric
		numStickyPollerMetric *numPollerMetric

		inboundPayloadVisitor     PayloadVisitor
		payloadVisitorConcurrency int
	}

	// workflowTaskProcessor implements processing of a workflow task and can create
	// workflow task pollers
	workflowTaskProcessor struct {
		basePoller
		namespace        string
		taskQueueName    string
		identity         string
		service          workflowservice.WorkflowServiceClient
		taskHandler      WorkflowTaskHandler
		contextManager   WorkflowContextManager
		logger           log.Logger
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter

		stickyUUID                   string
		StickyScheduleToStartTimeout time.Duration

		pendingRegularPollCount int
		pendingStickyPollCount  int
		stickyBacklog           int64
		stickyCacheSize         int
		eagerActivityExecutor   *eagerActivityExecutor

		numNormalPollerMetric *numPollerMetric
		numStickyPollerMetric *numPollerMetric

		inboundPayloadVisitor     PayloadVisitor
		outboundPayloadVisitor    PayloadVisitor
		payloadVisitorConcurrency int
	}

	// activityTaskPoller implements polling/processing a workflow task
	activityTaskPoller struct {
		basePoller
		namespace           string
		taskQueueName       string
		identity            string
		service             workflowservice.WorkflowServiceClient
		taskHandler         ActivityTaskHandler
		logger              log.Logger
		activitiesPerSecond float64
		numPollerMetric     *numPollerMetric
	}

	historyIteratorImpl struct {
		iteratorFunc  func(nextPageToken []byte) (*historypb.History, []byte, error)
		execution     *commonpb.WorkflowExecution
		nextPageToken []byte
		namespace     string
		service       workflowservice.WorkflowServiceClient
		// maxEventID is the maximum eventID that the history iterator is expected to return.
		// 0 means that the iterator will return all history events.
		maxEventID     int64
		metricsHandler metrics.Handler
		taskQueue      string
	}

	// retrievingHistoryIterator wraps a HistoryIterator and applies the inbound
	// payload visitor to each page fetched, resolving external storage references
	// in paginated history events that were not part of the initial poll response.
	retrievingHistoryIterator struct {
		inner                     HistoryIterator
		inboundVisitor            PayloadVisitor
		payloadVisitorConcurrency int
	}

	localActivityTaskPoller struct {
		basePoller
		handler      *localActivityTaskHandler
		logger       log.Logger
		laTunnel     *localActivityTunnel
		workerStopCh <-chan struct{}
	}

	localActivityTaskHandler struct {
		backgroundContext  context.Context
		metricsHandler     metrics.Handler
		logger             log.Logger
		dataConverter      converter.DataConverter
		contextPropagators []ContextPropagator
		interceptors       []WorkerInterceptor
		client             *WorkflowClient
		workerStopChannel  <-chan struct{}
	}

	localActivityResult struct {
		result  *commonpb.Payloads
		err     error
		task    *localActivityTask
		backoff time.Duration
	}

	localActivityTunnel struct {
		taskCh   chan *localActivityTask
		resultCh chan eagerOrPolledTask
		stopCh   <-chan struct{}
	}
)

func newNumPollerMetric(metricsHandler metrics.Handler, pollerType string) *numPollerMetric {
	_ = "STUB: not implemented"
	return nil
}

func (npm *numPollerMetric) increment() { _ = "STUB: not implemented"; return }

func (npm *numPollerMetric) decrement() { _ = "STUB: not implemented"; return }

func newLocalActivityTunnel(stopCh <-chan struct{}) *localActivityTunnel {
	_ = "STUB: not implemented"
	return nil
}

func (lat *localActivityTunnel) getTask() *localActivityTask { _ = "STUB: not implemented"; return nil }

func (lat *localActivityTunnel) sendTask(task *localActivityTask) bool {
	_ = "STUB: not implemented"
	return false
}

func isClientSideError(err error) bool {
	_ = "STUB: not implemented"
	// If an activity execution exceeds deadline.
	return false
}

// stopping returns true if worker is stopping right now
func (bp *basePoller) stopping() bool { _ = "STUB: not implemented"; return false }

// doPoll runs the given pollFunc in a separate go routine. Returns when any of the conditions are met:
//   - poll succeeds
//   - poll fails
//   - worker is stopping
func (bp *basePoller) doPoll(pollFunc func(ctx context.Context) (taskForWorker, error)) (taskForWorker, error) {
	_ = "STUB: not implemented"
	return *new(taskForWorker), nil
}

// Don't kill the gRPC stream. After ShutdownWorker, the server returns empty responses.

// TEMP FIX: Give the server a reasonable window to complete the poll after
// ShutdownWorker. Fall back to cancelling the poll if it takes too
// long, e.g. when the gRPC connection was closed before Stop().

// Legacy: cancel in-flight polls immediately on shutdown

func (bp *basePoller) getCapabilities() *workflowservice.GetSystemInfoResponse_Capabilities {
	_ = "STUB: not implemented"
	return nil
}

func (bp *basePoller) getDeploymentName() string { _ = "STUB: not implemented"; return "" }

// newWorkflowTaskProcessor creates a new workflow task poller which must have a one to one relationship to workflow worker
func newWorkflowTaskProcessor(
	taskHandler WorkflowTaskHandler,
	contextManager WorkflowContextManager,
	service workflowservice.WorkflowServiceClient,
	params workerExecutionParameters,
	stickyUUID string,
) *workflowTaskProcessor {
	_ = "STUB: not implemented"
	return nil
}

// PollTask polls a new task
func (wtp *workflowTaskPoller) PollTask() (taskForWorker, error) {
	_ = "STUB: not implemented"
	// Get the task.
	return *new(taskForWorker), nil
}

func (wtp *workflowTaskProcessor) createPoller(mode workflowTaskPollerMode) taskPoller {
	_ = "STUB: not implemented"
	return *new(taskPoller)
}

// ProcessTask processes a task which could be workflow task or local activity result
func (wtp *workflowTaskProcessor) ProcessTask(task interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (wtp *workflowTaskProcessor) processWorkflowTask(task *workflowTask) (retErr error) {
	_ = "STUB: not implemented"
	return nil

	// We didn't have task, poll might have timeout.
}

// close doneCh so local activity worker won't get blocked forever when trying to send back result to laResultCh.

// If we panic during processing the workflow task, we need to unlock the workflow context with an error to discard it.

// If we get an error responding to the workflow task we need to evict the execution from the cache.

// we are getting new workflow task, so reset the workflowTask and continue process the new one

func (wtp *workflowTaskProcessor) RespondTaskCompletedWithMetrics(
	taskCompletion *workflowTaskCompletion,
	taskErr error,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	startTime time.Time,
	downloadPayloadMetrics *workflowTaskStorageMetrics,
	workflowInfo *WorkflowInfo,
) (response *workflowservice.RespondWorkflowTaskCompletedResponse, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// The outbound visitor failed (e.g. storage driver error or panic). We
// cannot send the original response, so fall back to an explicit WFT
// failure so the server records the error immediately.

// Overwriting the original failure reason for metrics purposes

// We already know the first error was GRPC message too large, if there was another error when reporting the first error
// to the server it's probably more interesting for the user.

func (wtp *workflowTaskProcessor) sendTaskCompletedRequest(
	taskCompletion *workflowTaskCompletion,
	task *workflowservice.PollWorkflowTaskQueueResponse,
) (response *workflowservice.RespondWorkflowTaskCompletedResponse, err error) {
	_ = "STUB: not implemented"
	return nil,

		// Respond task completion.
		nil
}

// should not happen

// Only fail workflow task on first attempt, subsequent failure on the same workflow task will timeout.
// This is to avoid spin on the failed workflow task. Checking Attempt not nil for older server.

// should not happen

func (wtp *workflowTaskProcessor) reportGrpcMessageTooLarge(
	ctx context.Context,
	taskCompletion *workflowTaskCompletion,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	sendErr error,
) (emitFailMetric bool, err error) {
	_ = "STUB: not implemented"
	return false,

		// should not happen
		nil
}

// should not happen

func (wtp *workflowTaskProcessor) handleInboundVisitorError(task *workflowservice.PollWorkflowTaskQueueResponse, visitErr error) {
	_ = "STUB: not implemented"
	return
}

// Submit an explicit WFT failure so the server records the error immediately
// rather than waiting for the task to time out.

func (wtp *workflowTaskProcessor) errorToFailWorkflowTask(taskToken []byte, err error) *workflowservice.RespondWorkflowTaskFailedRequest {
	_ = "STUB: not implemented"
	return nil
}

// If it was a panic due to a bad state machine or if it was a history
// mismatch error, mark as non-deterministic

func (wtp *workflowTaskProcessor) errorToFailWorkflowTaskWithCause(taskToken []byte, err error, cause enumspb.WorkflowTaskFailedCause) *workflowservice.RespondWorkflowTaskFailedRequest {
	_ = "STUB: not implemented"
	return nil
}

//lint:ignore SA1019 ignore deprecated versioning APIs

type workflowTaskStorageMetrics struct {
	mu            sync.Mutex
	payloadCount  int
	totalSize     int64
	totalDuration time.Duration
	driverNames   map[string]struct{}
}

func (callback *workflowTaskStorageMetrics) PayloadBatchCompleted(count int, size int64, duration time.Duration, driverNames []string) {
	_ = "STUB: not implemented"
	return
}

func (callback *workflowTaskStorageMetrics) GetDriverNames() []string {
	_ = "STUB: not implemented"
	return nil
}

func newLocalActivityPoller(
	params workerExecutionParameters,
	laTunnel *localActivityTunnel,
	interceptors []WorkerInterceptor,
	client *WorkflowClient,
	workerStopCh <-chan struct{},
) *localActivityTaskPoller {
	_ = "STUB: not implemented"
	return nil
}

func (latp *localActivityTaskPoller) PollTask() (taskForWorker, error) {
	_ = "STUB: not implemented"
	return *new(taskForWorker), nil
}

func (latp *localActivityTaskPoller) ProcessTask(task interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// If shutdown is initiated after we begin local activity execution, there is no need to send result back to
// laResultCh, as both workers receive shutdown from top down.

// We need to send back the local activity result to unblock workflowTaskPoller.processWorkflowTask() which is
// synchronously listening on the laResultCh. We also want to make sure we don't block here forever in case
// processWorkflowTask() already returns and nobody is receiving from laResultCh. We guarantee that doneCh is closed
// before returning from workflowTaskPoller.processWorkflowTask().

// processWorkflowTask() already returns, just drop this local activity result.

func (lath *localActivityTaskHandler) executeLocalActivityTask(task *localActivityTask) (result *localActivityResult) {
	_ = "STUB: not implemented"
	return nil
}

// propagate context information into the local activity context from the headers

// panic handler

// If local activity takes longer than expected timeout, the context would already be DeadlineExceeded and
// the result would be discarded. Print a warning in this case.

// double check if result is ready.

// context is done

// should not happen

// local activity completed

func (wtp *workflowTaskPoller) release(kind enumspb.TaskQueueKind) {
	_ = "STUB: not implemented"
	return
}

func (wtp *workflowTaskPoller) updateBacklog(taskQueueKind enumspb.TaskQueueKind, backlogCountHint int64) {
	_ = "STUB: not implemented"
	return
}

// we only care about sticky backlog for now.

// getNextPollRequest returns appropriate next poll request based on poller configuration and mode.
// Simple rules:
//  1. if mode is NonSticky, always poll from regular task queue
//  2. if mode is Sticky, always poll from sticky task queue
//  3. if mode is Mixed
//     3.1. if sticky execution is disabled, always poll for regular task queue
//     3.2. otherwise:
//     3.2.1) if sticky task queue has backlog, always prefer to process sticky task first
//     3.2.2) poll from the task queue that has less pending requests (prefer sticky when they are the same).
func (wtp *workflowTaskPoller) getNextPollRequest() (request *workflowservice.PollWorkflowTaskQueueRequest) {
	_ = "STUB: not implemented"
	return nil
}

// Do nothing, taskQueue is already set to non-sticky

//lint:ignore SA1019 ignore deprecated versioning APIs

// Poll the workflow task queue and update the num_poller metric
func (wtp *workflowTaskPoller) pollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Poll for a single workflow task from the service
func (wtp *workflowTaskPoller) poll(ctx context.Context) (taskForWorker, error) {
	_ = "STUB: not implemented"
	return *new(taskForWorker), nil
}

// Emit using base scope as no workflow type information is available in the case of empty poll

func (wtp *workflowTaskPoller) toWorkflowTask(response *workflowservice.PollWorkflowTaskQueueResponse) *workflowTask {
	_ = "STUB: not implemented"
	return nil
}

func (wtp *workflowTaskProcessor) toWorkflowTask(response *workflowservice.PollWorkflowTaskQueueResponse) *workflowTask {
	_ = "STUB: not implemented"
	return nil
}

func (h *historyIteratorImpl) GetNextPage() (*historypb.History, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (h *historyIteratorImpl) Reset() { _ = "STUB: not implemented"; return }

func (h *historyIteratorImpl) HasNextPage() bool { _ = "STUB: not implemented"; return false }

func (r *retrievingHistoryIterator) GetNextPage() (*historypb.History, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (r *retrievingHistoryIterator) HasNextPage() bool { _ = "STUB: not implemented"; return false }
func (r *retrievingHistoryIterator) Reset()            { _ = "STUB: not implemented"; return }

func newGetHistoryPageFunc(
	ctx context.Context,
	service workflowservice.WorkflowServiceClient,
	namespace string,
	execution *commonpb.WorkflowExecution,
	lastEventID int64,
	metricsHandler metrics.Handler,
	taskQueue string,
) func(nextPageToken []byte) (*historypb.History, []byte, error) {
	_ = "STUB: not implemented"
	return nil
}

// While the SDK is processing a workflow task, the workflow task could timeout and server would start
// a new workflow task or the server looses the workflow task if it is a speculative workflow task. In either
// case, the new workflow task could have events that are beyond the last event ID that the SDK expects to process.
// In such cases, the SDK should return error indicating that the workflow task is stale since the result will not be used.

func newActivityTaskPoller(taskHandler ActivityTaskHandler, service workflowservice.WorkflowServiceClient, params workerExecutionParameters) *activityTaskPoller {
	_ = "STUB: not implemented"
	return nil
}

// Poll the activity task queue and update the num_poller metric
func (atp *activityTaskPoller) pollActivityTaskQueue(ctx context.Context, request *workflowservice.PollActivityTaskQueueRequest) (*workflowservice.PollActivityTaskQueueResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Poll for a single activity task from the service
func (atp *activityTaskPoller) poll(ctx context.Context) (taskForWorker, error) {
	_ = "STUB: not implemented"
	return *new(taskForWorker), nil
}

// No activity info is available on empty poll.  Emit using base scope.

// PollTask polls a new task
func (atp *activityTaskPoller) PollTask() (taskForWorker, error) {
	_ = "STUB: not implemented"
	// Get the task.
	return *new(taskForWorker), nil
}

// ProcessTask processes a new task
func (atp *activityTaskPoller) ProcessTask(task interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// We didn't have task, poll might have timeout.

// Process the activity task.

// err is returned in case of internal failure, such as unable to propagate context or context timeout.

// in case if activity execution failed, request should be of type RespondActivityTaskFailedRequest

func reportActivityComplete(
	ctx context.Context,
	service workflowservice.WorkflowServiceClient,
	request interface{},
	rpcMetricsHandler metrics.Handler,
) error {
	_ = "STUB: not implemented"
	return nil

	// nothing to report
}

func reportActivityCompleteByID(
	ctx context.Context,
	service workflowservice.WorkflowServiceClient,
	request interface{},
	rpcMetricsHandler metrics.Handler,
) error {
	_ = "STUB: not implemented"
	return nil

	// nothing to report
}

func convertActivityResultToRespondRequest(
	identity string,
	taskToken []byte,
	result *commonpb.Payloads,
	err error,
	dataConverter converter.DataConverter,
	failureConverter converter.FailureConverter,
	namespace string,
	cancelAllowed bool,
	versionStamp *commonpb.WorkerVersionStamp,
	deployment *deploymentpb.Deployment,
	workerDeploymentOptions *deploymentpb.WorkerDeploymentOptions,
) interface{} {
	_ = "STUB: not implemented"
	return nil
}

// activity result is pending and will be completed asynchronously.
// nothing to report at this point

// Only respond with canceled if allowed

// If a canceled error is returned but it wasn't allowed, we have to wrap in
// an unexpected-cancel application error

func convertActivityResultToRespondRequestByID(
	identity string,
	namespace string,
	workflowID string,
	runID string,
	activityID string,
	result *commonpb.Payloads,
	err error,
	dataConverter converter.DataConverter,
	failureConverter converter.FailureConverter,
	cancelAllowed bool,
) interface{} {
	_ = "STUB: not implemented"
	return nil
}

// activity result is pending and will be completed asynchronously.
// nothing to report at this point

// Only respond with canceled if allowed

// If a canceled error is returned but it wasn't allowed, we have to wrap in
// an unexpected-cancel application error

func (wft *workflowTask) isEmpty() bool { _ = "STUB: not implemented"; return false }

func (wft *workflowTask) scaleDecision() (pollerScaleDecision, bool) {
	_ = "STUB: not implemented"
	return *new(pollerScaleDecision), false
}

func (at *activityTask) isEmpty() bool { _ = "STUB: not implemented"; return false }

func (at *activityTask) scaleDecision() (pollerScaleDecision, bool) {
	_ = "STUB: not implemented"
	return *new(pollerScaleDecision), false
}

func (*localActivityTask) isEmpty() bool { _ = "STUB: not implemented"; return false }

func (*localActivityTask) scaleDecision() (pollerScaleDecision, bool) {
	_ = "STUB: not implemented"
	return *new(pollerScaleDecision), false
}

func (*eagerWorkflowTask) isEmpty() bool { _ = "STUB: not implemented"; return false }

func (*eagerWorkflowTask) scaleDecision() (pollerScaleDecision, bool) {
	_ = "STUB: not implemented"
	return *new(pollerScaleDecision), false
}

func (nt *nexusTask) isEmpty() bool { _ = "STUB: not implemented"; return false }

func (nt *nexusTask) scaleDecision() (pollerScaleDecision, bool) {
	_ = "STUB: not implemented"
	return *new(pollerScaleDecision), false
}

// commandAwarePayloadVisitor is a wrapper around a PayloadVisitor that adds command-specific context information
type commandAwarePayloadVisitor struct {
	innerVisitor PayloadVisitor
	workflowInfo *WorkflowInfo
}

var _ PayloadVisitorWithContextHook = (*commandAwarePayloadVisitor)(nil)

func (v *commandAwarePayloadVisitor) Visit(ctx *proxy.VisitPayloadsContext, payload []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (v *commandAwarePayloadVisitor) ContextHook(ctx context.Context, msg proto.Message) (context.Context, error) {
	_ = "STUB: not implemented"
	return *new(context.Context), nil
}

// The new run keeps the same workflow ID. WorkflowType comes from the
// command if specified (type change), otherwise falls back to the current
// type already in context. RunID is omitted — the new run hasn't started.

// Set target to parent context if not a continue-as-new workflow
