package internal

// All code in this file is private to the package.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	defaultStickyCacheSize = 10000

	noRetryBackoff = time.Duration(-1)

	defaultDefaultHeartbeatThrottleInterval               = 30 * time.Second
	defaultMaxHeartbeatThrottleInterval                   = 60 * time.Second
	defaultMaxConcurrentWorkflowTaskExternalStorageVisits = 3
)

var (
	// ErrActivityPaused is returned from an activity heartbeat or the cause of an activity's context to indicate that the activity is paused.
	//
	// WARNING: Activity pause is currently experimental
	ErrActivityPaused = errors.New("activity paused")

	// ErrActivityReset is returned from an activity heartbeat or the cause of an activity's context to indicate that the activity has been reset.
	//
	// WARNING: Activity reset is currently experimental
	ErrActivityReset = errors.New("activity reset")
)

type (
	// workflowExecutionEventHandler process a single event.
	workflowExecutionEventHandler interface {
		// Process a single event and return the assosciated commands.
		// Return List of commands made, any error.
		ProcessEvent(event *historypb.HistoryEvent, isReplay bool, isLast bool) error
		// ProcessInteraction processes interaction inputs
		ProcessMessage(msg *protocolpb.Message, isReplay bool, isLast bool) error
		// ProcessQuery process a query request.
		ProcessQuery(queryType string, queryArgs *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error)
		StackTrace() string
		// Close for cleaning up resources on this event handler
		Close()
	}

	// workflowTask wraps a workflow task.
	workflowTask struct {
		task            *workflowservice.PollWorkflowTaskQueueResponse
		historyIterator HistoryIterator
		doneCh          chan struct{}
		laResultCh      chan *localActivityResult

		// This channel must be initialized with a one-size buffer and is used to indicate when
		// it is time for a local activity to be retried
		laRetryCh chan *localActivityTask
	}

	// eagerWorkflowTask represents a workflow task sent from an eager workflow executor
	eagerWorkflowTask struct {
		task *workflowservice.PollWorkflowTaskQueueResponse
	}

	// activityTask wraps a activity task.
	activityTask struct {
		task   *workflowservice.PollActivityTaskQueueResponse
		permit *SlotPermit
	}

	// workflowExecutionContextImpl is the cached workflow state for sticky execution
	workflowExecutionContextImpl struct {
		mutex        sync.Mutex
		workflowInfo *WorkflowInfo
		wth          *workflowTaskHandlerImpl

		eventHandler *workflowExecutionEventHandler

		isWorkflowCompleted bool
		result              *commonpb.Payloads
		err                 error
		// previousStartedEventID is the event ID of the workflow task started event of the previous workflow task.
		previousStartedEventID int64
		// lastHandledEventID is the event ID of the last event that the workflow state machine processed.
		lastHandledEventID int64

		newCommands         []*commandpb.Command
		newMessages         []*protocolpb.Message
		currentWorkflowTask *workflowservice.PollWorkflowTaskQueueResponse
		laTunnel            *localActivityTunnel
		cached              bool
	}

	// workflowTaskHandlerImpl is the implementation of WorkflowTaskHandler
	workflowTaskHandlerImpl struct {
		namespace                 string
		metricsHandler            metrics.Handler
		ppMgr                     pressurePointMgr
		logger                    log.Logger
		identity                  string
		workerBuildID             string
		useBuildIDForVersioning   bool
		workerDeploymentVersion   WorkerDeploymentVersion
		defaultVersioningBehavior VersioningBehavior
		enableLoggingInReplay     bool
		registry                  *registry
		laTunnel                  *localActivityTunnel
		workflowPanicPolicy       WorkflowPanicPolicy
		dataConverter             converter.DataConverter
		failureConverter          converter.FailureConverter
		contextPropagators        []ContextPropagator
		cache                     *WorkerCache
		deadlockDetectionTimeout  time.Duration
		capabilities              *workflowservice.GetSystemInfoResponse_Capabilities
	}

	activityProvider func(name string) activity

	// activityTaskHandlerImpl is the implementation of ActivityTaskHandler
	activityTaskHandlerImpl struct {
		taskQueueName                    string
		identity                         string
		client                           *WorkflowClient
		metricsHandler                   metrics.Handler
		logger                           log.Logger
		backgroundContext                context.Context
		registry                         *registry
		activityProvider                 activityProvider
		dataConverter                    converter.DataConverter
		failureConverter                 converter.FailureConverter
		workerStopCh                     <-chan struct{}
		contextPropagators               []ContextPropagator
		namespace                        string
		defaultHeartbeatThrottleInterval time.Duration
		maxHeartbeatThrottleInterval     time.Duration
		versionStamp                     *commonpb.WorkerVersionStamp
		deployment                       *deploymentpb.Deployment
		workerDeploymentOptions          *deploymentpb.WorkerDeploymentOptions
		inboundPayloadVisitor            PayloadVisitor
		outboundPayloadVisitor           PayloadVisitor
		payloadVisitorConcurrency        int
	}

	// history wrapper method to help information about events.
	history struct {
		workflowTask       *workflowTask
		eventsHandler      *workflowExecutionEventHandlerImpl
		loadedEvents       []*historypb.HistoryEvent
		currentIndex       int
		nextEventID        int64 // next expected eventID for sanity
		lastEventID        int64 // last expected eventID, zero indicates read until end of stream
		lastHandledEventID int64 // last event ID that was processed
		next               []*historypb.HistoryEvent
		nextMessages       []*protocolpb.Message
		nextFlags          []sdkFlag
		binaryChecksum     string
		sdkVersion         string
		sdkName            string
	}

	workflowTaskHeartbeatError struct {
		Message string
	}

	historyMismatchError struct {
		message string
	}

	unknownSdkFlagError struct {
		message string
	}

	preparedTask struct {
		events         []*historypb.HistoryEvent
		markers        []*historypb.HistoryEvent
		flags          []sdkFlag
		acceptedMsgs   []*protocolpb.Message
		admittedMsgs   []*protocolpb.Message
		binaryChecksum string
		sdkVersion     string
		sdkName        string
		// Is null if there was no task completed event to read the build ID from (but may be
		// empty string if there was, and it was empty)
		buildID *string
	}

	finishedTask struct {
		isFailed       bool
		binaryChecksum string
		flags          []sdkFlag
		sdkVersion     string
		sdkName        string
	}

	workflowTaskCompletion struct {
		rawRequest             proto.Message
		applyCompletionMetrics func()
	}
)

func newHistory(lastHandledEventID int64, task *workflowTask, eventsHandler *workflowExecutionEventHandlerImpl) *history {
	_ = "STUB: not implemented"
	return nil
}

func (e workflowTaskHeartbeatError) Error() string { _ = "STUB: not implemented"; return "" }

func historyMismatchErrorf(f string, v ...interface{}) historyMismatchError {
	_ = "STUB: not implemented"
	return *new(historyMismatchError)
}

func (h historyMismatchError) Error() string { _ = "STUB: not implemented"; return "" }

func (s unknownSdkFlagError) Error() string {
	_ = "STUB: not implemented"

	// Get workflow start event.
	return ""
}

func (eh *history) GetWorkflowStartedEvent() (*historypb.HistoryEvent, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (eh *history) IsReplayEvent(event *historypb.HistoryEvent) bool {
	_ = "STUB: not implemented"
	return false
}

// isNextWorkflowTaskFailed checks if the workflow task failed or completed. If it did complete returns some information
// on the completed workflow task.
func (eh *history) isNextWorkflowTaskFailed() (task finishedTask, err error) {
	_ = "STUB: not implemented"
	return *new(finishedTask), nil
}

// Server can return an empty page so if we need the next event we must keep checking until we either get it
// or know we have no more pages to check
// current page ends and there is more pages

// If not replaying we should not expect to find any more events

//lint:ignore SA1019 ignore deprecated versioning APIs

// If a flag is not recognized (value is too high or not defined), it must fail the workflow task

func (eh *history) loadMoreEvents() error { _ = "STUB: not implemented"; return nil }

func isCommandEvent(eventType enumspb.EventType) bool { _ = "STUB: not implemented"; return false }

// nextTask returns the next task to be processed.
func (eh *history) nextTask() (*preparedTask, error) { _ = "STUB: not implemented"; return nil, nil }

func (eh *history) hasMoreEvents() bool { _ = "STUB: not implemented"; return false }

func (eh *history) getMoreEvents() (*historypb.History, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (eh *history) verifyAllEventsProcessed() error { _ = "STUB: not implemented"; return nil }

func (eh *history) prepareTask() (*preparedTask, error) { _ = "STUB: not implemented"; return nil, nil }

// Process events

// load more history events if needed

// Skip

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

// shrink loaded events so it can be GCed

func isPreloadMarkerEvent(event *historypb.HistoryEvent) bool {
	_ = "STUB: not implemented"
	return false
}

func inferMessageFromAcceptedEvent(attrs *historypb.WorkflowExecutionUpdateAcceptedEventAttributes) *protocolpb.Message {
	_ = "STUB: not implemented"
	return nil
}

// newWorkflowTaskHandler returns an implementation of workflow task handler.
func newWorkflowTaskHandler(params workerExecutionParameters, ppMgr pressurePointMgr, registry *registry) WorkflowTaskHandler {
	_ = "STUB: not implemented"
	return *new(WorkflowTaskHandler)
}

func newWorkflowExecutionContext(
	workflowInfo *WorkflowInfo,
	taskHandler *workflowTaskHandlerImpl,
) *workflowExecutionContextImpl {
	_ = "STUB: not implemented"
	return nil
}

// Lock acquires the lock on this context object, use Unlock(error) to release
// the lock.
func (w *workflowExecutionContextImpl) Lock() {
	_ = "STUB: not implemented"

	// Unlock cleans up after the provided error and its own internal view of the
	// workflow error state by clearing itself and removing itself from cache as
	// needed. It is an error to call this function without having called the Lock
	// function first and the behavior is undefined. Regardless of the error
	// handling involved, the context will be unlocked when this call returns.
	return
}

func (w *workflowExecutionContextImpl) Unlock(err error) { _ = "STUB: not implemented"; return }

// TODO: in case of closed, it assumes the close command always succeed. need server side change to return
// error to indicate the close failure case. This should be a rare case. For now, always remove the cache, and
// if the close command failed, the next command will have to rebuild the state.

// Clear the state so other tasks waiting on the context know it should be discarded.

// Clear the state if we never cached the workflow so coroutines can be
// exited

func (w *workflowExecutionContextImpl) getEventHandler() *workflowExecutionEventHandlerImpl {
	_ = "STUB: not implemented"
	return nil
}

func (w *workflowExecutionContextImpl) completeWorkflow(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (w *workflowExecutionContextImpl) onEviction() {
	_ = "STUB: not implemented"
	// onEviction is run by LRU cache's removeFunc in separate goroutinue
	return
}

// Emit force eviction metrics.
// This metrics indicates too many concurrent running workflows to fit in sticky cache.
// Eviction on error or on workflow complete is normal and expected.

func (w *workflowExecutionContextImpl) IsDestroyed() bool { _ = "STUB: not implemented"; return false }

func (w *workflowExecutionContextImpl) clearState() { _ = "STUB: not implemented"; return }

// Set isReplay to true to prevent user code in defer guarded by !isReplaying() from running

func (w *workflowExecutionContextImpl) createEventHandler() { _ = "STUB: not implemented"; return }

func resetHistory(task *workflowservice.PollWorkflowTaskQueueResponse, historyIterator HistoryIterator) (*historypb.History, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wth *workflowTaskHandlerImpl) createWorkflowContext(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowExecutionContextImpl, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Setup workflow Info

// Use the original execution run ID from the start event as the initial seed.
// Original execution run ID stays the same for the entire chain of workflow resets.
// This helps us keep child workflow IDs consistent up until a reset-point is encountered.

func (wth *workflowTaskHandlerImpl) GetOrCreateWorkflowContext(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	historyIterator HistoryIterator,
) (workflowContext *workflowExecutionContextImpl, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Verify the cached state is current and for the correct worker

// query task and we have a valid cached state

// non query task and we have a valid cached state

// possible another task already destroyed this context.

// non query task and cached state is missing events, we need to discard the cached state and build a new one.

// If the workflow was not cached or the cache was stale.

// we are getting partial history task, but cached state was already evicted.
// we need to reset history so we get events from beginning to replay/rebuild the state

func isFullHistory(history *historypb.History) bool { _ = "STUB: not implemented"; return false }

func (w *workflowExecutionContextImpl) resetStateIfDestroyed(task *workflowservice.PollWorkflowTaskQueueResponse, historyIterator HistoryIterator) error {
	_ = "STUB: not implemented"
	// It is possible that 2 threads (one for workflow task and one for query task) that both are getting this same
	// cached workflowContext. If one task finished with err, it would destroy the cached state. In that case, the
	// second task needs to reset the cache state and start from beginning of the history.
	return nil
}

// reset history events if necessary

// Reset the search attributes and memos from the WorkflowExecutionStartedEvent.
// The search attributes and memo may have been modified by calls like UpsertMemo
// or UpsertSearchAttributes. They must be reset to avoid non determinism on replay.

// ProcessWorkflowTask processes all the events of the workflow task.
func (wth *workflowTaskHandlerImpl) ProcessWorkflowTask(
	workflowTask *workflowTask,
	workflowContext *workflowExecutionContextImpl,
	heartbeatFunc workflowTaskHeartbeatFunc,
) (taskCompletion *workflowTaskCompletion, errRet error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// For non-graceful shutdown, the LA worker stops before this function, so there
// is no need to continue heartbeating. Instead, we can exit early, giving up
// the slot this function takes, a little sooner.

// stopCh closed means worker is shutting down and there's
// no need for LA heartbeat

// force complete, call the workflow task heartbeat function

// if workflow task heartbeat failed, the workflow execution context will be cleared and eventHandler will be nil

// local activity result ready

// workflow task is not done yet, still waiting for more local activities

func (w *workflowExecutionContextImpl) ProcessWorkflowTask(workflowTask *workflowTask) (*workflowTaskCompletion, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// After processing the workflow task, update the last handled event ID
// to the last event ID in the history. We do this regardless of whether the workflow task
// was successfully processed or not. This is because a failed workflow task will cause the
// cache to be evicted and the next workflow task will start from the beginning of the history.

// If we are in the replayer we should always check the history replay, even if the workflow is completed
// Skip if the workflow panicked to avoid potentially breaking old histories

// This is set to nil once recorded

// Peak ahead to confirm there are no more events

// Check if we are replaying so we know if we should use the messages in the WFT or the history

// Check if we need to replace the update message synthesize from an
// accepted event with the update message synthesize from an admitted event

// At this point, all update messages should have a body

// Since replayCommands updates a loop early, keep track of index before the
// early update to handle replaying incomplete WFE

// Reset the mutable side effect markers recorded

// Markers are from the events that are produced from the current workflow task.

// local activity marker needs to be applied after workflow task started event

// marker events are processed separately

// Any pressure points.

// because we don't run all events through this code path, we have
// to run ProcessMessages both before and after ProcessEvent to
// catch any messages that should have been delivered _before_ this
// event but perhaps were not because there were attached to an
// event (e.g. WFTScheduledEvent) that does not come through this
// loop.

// now apply local activity markers

// Non-deterministic error could happen in 2 different places:
//   1) the replay commands does not match to history events. This is usually due to non backwards compatible code
// change to workflow logic. For example, change calling one activity to a different activity.
//   2) the command state machine is trying to make illegal state transition while replay a history event (like
// activity task completed), but the corresponding workflow code that start the event has been removed. In that case
// the replay of that event will panic on the command state machine and the workflow will be marked as completed
// with the panic error.

// check if commands from reply matches to the history events

func (w *workflowExecutionContextImpl) ProcessLocalActivityResult(workflowTask *workflowTask, lar *localActivityResult) (*workflowTaskCompletion, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// nothing to do here as we are retrying...

func (w *workflowExecutionContextImpl) applyWorkflowPanicPolicy(workflowTask *workflowTask, workflowError error) (*workflowTaskCompletion, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// complete workflow with custom error will fail the workflow

// return error here will be convert to WorkflowTaskFailed for the first time, and ignored for subsequent
// attempts which will cause WorkflowTaskTimeout and server will retry forever until issue got fixed or
// workflow timeout.

func (w *workflowExecutionContextImpl) retryLocalActivity(lar *localActivityResult) bool {
	_ = "STUB: not implemented"
	return false
}

// we need a local retry

// Send retry signal

// Task is already done. Abort retrying.

// Backoff could be large and potentially much larger than WorkflowTaskTimeout. We cannot just sleep locally for
// retry. Because it will delay the local activity from complete which keeps the workflow task open. In order to
// keep workflow task open, we have to keep "heartbeating" current workflow task.
// In that case, it is more efficient to create a server timer with backoff duration and retry when that backoff
// timer fires. So here we will return false to indicate we don't need local retry anymore. However, we have to
// store the current attempt and backoff to the same LocalActivityResultMarker so the replay can do the right thing.
// The backoff timer will be created by workflow.ExecuteLocalActivity().

func getRetryBackoff(lar *localActivityResult, now time.Time) time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func getRetryBackoffWithNowTime(p *RetryPolicy, attempt int32, err error, now, expireTime time.Time) time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

// max attempt reached

// Extract backoff interval from error if it is a retryable error.
// Not using errors.As() since we don't want to explore the whole error chain.

// Calculate next backoff interval if the error did not contain the next backoff interval.
// attempt starts from 1

// math.Pow() could overflow

// cap next interval to MaxInterval

func (w *workflowExecutionContextImpl) CompleteWorkflowTask(workflowTask *workflowTask, waitLocalActivities bool) *workflowTaskCompletion {
	_ = "STUB: not implemented"
	return nil
}

// w.laTunnel could be nil for worker.ReplayHistory() because there is no worker started, in that case we don't
// care about the pending local activities, and just return because the result is ignored anyway by the caller.

// start new local activity tasks

// cannot complete workflow task as there are pending local activities

func (w *workflowExecutionContextImpl) hasPendingLocalActivityWork() bool {
	_ = "STUB: not implemented"
	return false
}

// don't run local activity for query task

func (w *workflowExecutionContextImpl) clearCurrentTask() { _ = "STUB: not implemented"; return }

func (w *workflowExecutionContextImpl) skipReplayCheck() bool {
	_ = "STUB: not implemented"
	return false
}

func (w *workflowExecutionContextImpl) SetCurrentTask(task *workflowservice.PollWorkflowTaskQueueResponse) {
	_ = "STUB: not implemented"
	return
}

// do not update the previousStartedEventID for query task

func (w *workflowExecutionContextImpl) SetPreviousStartedEventID(eventID int64) {
	_ = "STUB: not implemented"
	// We must reset the last event we handled to be after the last WFT we really completed
	// + any command events (since the SDK "processed" those when it emitted the commands). This
	// is also equal to what we just processed in the speculative task, minus two, since we
	// would've just handled the most recent WFT started event, and we need to drop that & the
	// schedule event just before it.
	return
}

func (w *workflowExecutionContextImpl) ResetIfStale(task *workflowservice.PollWorkflowTaskQueueResponse, historyIterator HistoryIterator) error {
	_ = "STUB: not implemented"
	return nil
}

func skipDeterministicCheckForCommand(d *commandpb.Command, _ *sdkFlags) bool {
	_ = "STUB: not implemented"
	return false
}

func skipDeterministicCheckForEvent(e *historypb.HistoryEvent, sdkFlags *sdkFlags) bool {
	_ = "STUB: not implemented"
	return false
}

// special check for upsert change version event
func skipDeterministicCheckForUpsertChangeVersion(events []*historypb.HistoryEvent, idx int) bool {
	_ = "STUB: not implemented"
	return false
}

func matchReplayWithHistory(
	replayCommands []*commandpb.Command,
	historyEvents []*historypb.HistoryEvent,
	msgs []outboxEntry,
	sdkFlags *sdkFlags,
) error {
	_ = "STUB: not implemented"
	return nil
}

func lastPartOfName(name string) string { _ = "STUB: not implemented"; return "" }

func isCommandMatchEvent(d *commandpb.Command, e *historypb.HistoryEvent, obes []outboxEntry) bool {
	_ = "STUB: not implemented"
	return false
}

//lint:ignore SA1019 deprecated namespace field

//lint:ignore SA1019 deprecated namespace field

func isSearchAttributesMatched(attrFromEvent, attrFromCommand *commonpb.SearchAttributes) bool {
	_ = "STUB: not implemented"
	return false
}

func isMemoMatched(attrFromEvent, attrFromCommand *commonpb.Memo) bool {
	_ = "STUB: not implemented"
	return false
}

// return true if the check fails:
//
//	namespace is not empty in command
//	and namespace is not replayNamespace
//	and namespaces unmatch in command and events
func checkNamespacesInCommandAndEvent(eventNamespace, commandNamespace string) bool {
	_ = "STUB: not implemented"
	return false
}

func (wth *workflowTaskHandlerImpl) completeWorkflow(
	eventHandler *workflowExecutionEventHandlerImpl,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	workflowContext *workflowExecutionContextImpl,
	commands []*commandpb.Command,
	messages []*protocolpb.Message,
	forceNewWorkflowTask bool,
) workflowTaskCompletion {
	_ = "STUB: not implemented"
	// for query task
	return *new(workflowTaskCompletion)
}

// complete workflow task

// Workflow canceled

// Continue as new error.

// ContinueAsNewError.RetryPolicy is optional.
// If not set, use the retry policy from the workflow context.

// Workflow failures

// Workflow completion

//lint:ignore SA1019 ignore deprecated versioning APIs

// Return request and a function that will update certain metrics

func (wth *workflowTaskHandlerImpl) executeAnyPressurePoints(event *historypb.HistoryEvent, isInReplay bool) error {
	_ = "STUB: not implemented"
	return nil
}

func newActivityTaskHandler(
	client *WorkflowClient,
	params workerExecutionParameters,
	registry *registry,
) ActivityTaskHandler {
	_ = "STUB: not implemented"
	return *new(ActivityTaskHandler)
}

func newActivityTaskHandlerWithCustomProvider(
	client *WorkflowClient,
	params workerExecutionParameters,
	registry *registry,
	activityProvider activityProvider,
) ActivityTaskHandler {
	_ = "STUB: not implemented"
	return *new(ActivityTaskHandler)
}

// heartbeatVisitorError wraps an outbound payload visitor error from a heartbeat.
// It is used as the context cancellation cause so Execute() can detect that
// RespondActivityTaskFailed was already sent proactively and skip sending a second response.
type heartbeatVisitorError struct{ err error }

func (e heartbeatVisitorError) Error() string { _ = "STUB: not implemented"; return "" }
func (e heartbeatVisitorError) Unwrap() error { _ = "STUB: not implemented"; return nil }

type temporalInvoker struct {
	sync.Mutex
	identity       string
	service        workflowservice.WorkflowServiceClient
	metricsHandler metrics.Handler
	taskToken      []byte
	// cancelHandler is called when the activity is canceled by a heartbeat request.
	cancelHandler context.CancelCauseFunc
	// Amount of time to wait between each pending heartbeat send
	heartbeatThrottleInterval time.Duration
	hbBatchEndTimer           *time.Timer // Whether we started a batch of operations that need to be reported in the cycle. This gets started on a user call.
	lastDetailsToReport       **commonpb.Payloads
	closeCh                   chan struct{}
	workerStopChannel         <-chan struct{}
	namespace                 string
	excludeInternalFromRetry  *atomic.Bool // borrowed from client in order to tell if internal errors are retriable
	outboundPayloadVisitor    PayloadVisitor
	failureConverter          converter.FailureConverter
}

func (i *temporalInvoker) Heartbeat(ctx context.Context, details *commonpb.Payloads, skipBatching bool) error {
	_ = "STUB: not implemented"
	return nil
}

// If we have started batching window, keep track of last reported progress.

// If the activity is canceled, the activity can ignore the cancellation and do its work
// and complete. Our cancellation is co-operative, so we will try to heartbeat.

// We have successfully sent heartbeat, start next batching window.

// Create timer to fire before the threshold to report.

// We are close to deadline.

// Activity worker is close to stop. This does the same steps as batch timer ends.

// We got closed.

// We close the batch and report the progress.

// TODO: there is a potential race condition here as the lock is released here and
// locked again in the Hearbeat() method. This possible that a heartbeat call from
// user activity grabs the lock first and calls internalHeartBeat before this
// batching goroutine, which means some activity progress will be lost.

func (i *temporalInvoker) internalHeartBeat(ctx context.Context, details *commonpb.Payloads) (bool, error) {
	_ = "STUB: not implemented"
	return false,

		// We don't want the recording of the heartbeat to keep retrying the RPC
		// longer than the throttle interval. However, sometimes the interval is so
		// small that the context is cancelled before it even starts the call.
		// Therefore, we'll make sure not to timeout the context faster than the
		// minimum RPC timeout.
		nil
}

// Proactively fail the task so the server can retry immediately rather than
// waiting for the heartbeat timeout. Errors are ignored — if the RPC fails the
// activity will still be timed out by the server.

// We are asked to cancel. inform the activity about cancellation through context.

// We will pass these through as cancellation for now but something we can change
// later when we have setter on cancel handler.

// No error, do nothing.

// We are asked to pause/reset. inform the activity about cancellation through context.

// Transient errors are getting retried for the duration of the heartbeat timeout.
// The fact that error has been returned means that activity should now be timed out, hence we should
// propagate cancellation to the handler.

// This error won't be returned to user check RecordActivityHeartbeat().

func (i *temporalInvoker) Close(ctx context.Context, flushBufferedHeartbeat bool) {
	_ = "STUB: not implemented"
	return
}

func (i *temporalInvoker) GetClient(options ClientOptions) Client {
	_ = "STUB: not implemented"
	return *new(Client)
}

func newServiceInvoker(
	taskToken []byte,
	identity string,
	service workflowservice.WorkflowServiceClient,
	metricsHandler metrics.Handler,
	cancelHandler context.CancelCauseFunc,
	heartbeatThrottleInterval time.Duration,
	workerStopChannel <-chan struct{},
	namespace string,
	excludeInternalFromRetry *atomic.Bool,
	outboundPayloadVisitor PayloadVisitor,
	failureConverter converter.FailureConverter,
) ServiceInvoker {
	_ = "STUB: not implemented"
	return *new(ServiceInvoker)
}

// Execute executes an implementation of the activity.
func (ath *activityTaskHandlerImpl) Execute(taskQueue string, t *workflowservice.PollActivityTaskQueueResponse) (result interface{}, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// The root context is only cancelled when the worker is finished shutting down.

// We must capture the context here because it is changed later to one that is
// cancelled when the activity is done

// flush buffered heartbeat if activity was not successfully completed.

// In case if activity is not registered we should report a failure to the server to allow activity retry
// instead of making it stuck on the same attempt.

// panic handler

// propagate context information into the activity context from the headers

// Check if context canceled at a higher level before we cancel it ourselves

// The heartbeat visitor failure path proactively sent RespondActivityTaskFailed,
// skip sending another response regardless of what the activity returned.

// Cancels that don't originate from the server will have separate cancel reasons, like
// ErrWorkerShutdown or ErrActivityPaused

// Default to Error

// Downgrade to Debug for benign application errors

// Use backgroundContext as base so a cancelled activity context (e.g. pause/reset)
// does not prevent the outbound storage visitor from making HTTP calls.

func (ath *activityTaskHandlerImpl) visitorErrorToActivityFailure(msgPrefix string, t *workflowservice.PollActivityTaskQueueResponse, err error) *workflowservice.RespondActivityTaskFailedRequest {
	_ = "STUB: not implemented"
	return nil
}

func (ath *activityTaskHandlerImpl) getActivity(name string) activity {
	_ = "STUB: not implemented"
	return *new(activity)
}

func (ath *activityTaskHandlerImpl) getRegisteredActivityNames() (activityNames []string) {
	_ = "STUB: not implemented"
	return nil
}

func (ath *activityTaskHandlerImpl) getHeartbeatThrottleInterval(heartbeatTimeout time.Duration) time.Duration {
	_ = "STUB: not implemented"
	// Set interval as 80% of timeout if present, or the configured default if
	// present, or the system default otherwise
	return *new(time.Duration)
}

// Use the configured max if present, or the system default otherwise

// Limit interval to a max

func createNewCommand(commandType enumspb.CommandType) *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func createNewCommandWithMetadata(commandType enumspb.CommandType, metadata *sdk.UserMetadata) *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func recordActivityHeartbeat(ctx context.Context, service workflowservice.WorkflowServiceClient, metricsHandler metrics.Handler,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) error {
	_ = "STUB: not implemented"
	return nil
}

func recordActivityHeartbeatByID(ctx context.Context, service workflowservice.WorkflowServiceClient, metricsHandler metrics.Handler,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
) error {
	_ = "STUB: not implemented"
	return nil
}

// This enables verbose logging in the client library.
// check worker.EnableVerboseLogging()
func traceLog(fn func()) { _ = "STUB: not implemented"; return }
