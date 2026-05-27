package internal

// All code in this file is private to the package.

import (
	"errors"
	"sync"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/protocol"
	"go.temporal.io/sdk/log"
)

const (
	queryResultSizeLimit             = 2000000 // 2MB
	changeVersionSearchAttrSizeLimit = 2048
)

// Assert that structs do indeed implement the interfaces
var (
	_ WorkflowEnvironment           = (*workflowEnvironmentImpl)(nil)
	_ workflowExecutionEventHandler = (*workflowExecutionEventHandlerImpl)(nil)
)

type (
	// completionHandler Handler to indicate completion result
	completionHandler func(result *commonpb.Payloads, err error)

	// workflowExecutionEventHandlerImpl handler to handle workflowExecutionEventHandler
	workflowExecutionEventHandlerImpl struct {
		*workflowEnvironmentImpl
		workflowDefinition WorkflowDefinition
	}

	scheduledTimer struct {
		callback ResultHandler
		handled  bool
	}

	scheduledActivity struct {
		callback             ResultHandler
		waitForCancelRequest bool
		handled              bool
		activityType         ActivityType
		// Per-activity context-aware converters so that cancellation details and
		// failures are decoded with the correct ActivitySerializationContext,
		// matching how they were encoded by the activity worker.
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter
	}

	scheduledNexusOperation struct {
		startedCallback   func(token string, err error)
		completedCallback func(result *commonpb.Payload, err error)
		cancellationType  NexusOperationCancellationType
		endpoint          string
		service           string
		operation         string
	}

	scheduledChildWorkflow struct {
		resultCallback      ResultHandler
		startedCallback     func(r WorkflowExecution, e error)
		waitForCancellation bool
		handled             bool
		// Per-child-workflow context-aware converters so that cancellation
		// details and failures are decoded with the correct
		// WorkflowSerializationContext, matching how they were encoded.
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter
	}

	scheduledCancellation struct {
		callback ResultHandler
		handled  bool
	}

	scheduledSignal struct {
		callback ResultHandler
		handled  bool
	}

	sendCfg struct {
		addCmd bool
		pred   func(*historypb.HistoryEvent) bool
	}

	msgSendOpt func(so *sendCfg)

	outboxEntry struct {
		eventPredicate func(*historypb.HistoryEvent) bool
		msg            *protocolpb.Message
	}

	// workflowEnvironmentImpl an implementation of WorkflowEnvironment represents a environment for workflow execution.
	workflowEnvironmentImpl struct {
		workflowInfo *WorkflowInfo

		commandsHelper             *commandsHelper
		outbox                     []outboxEntry
		sideEffectResult           map[int64]*commonpb.Payloads
		changeVersions             map[string]Version
		pendingLaTasks             map[string]*localActivityTask
		completedLaAttemptsThisWFT uint32
		// mutableSideEffect is a map for each mutable side effect ID where each key is the
		// number of times the mutable side effect was called in a workflow
		// execution per ID.
		mutableSideEffect map[string]map[int]*commonpb.Payloads
		unstartedLaTasks  map[string]struct{}
		openSessions      map[string]*SessionInfo

		// Set of mutable side effect IDs that are recorded on the next task for use
		// during replay to determine whether a command should be created. The keys
		// are the user-provided IDs + "_" + the command counter.
		mutableSideEffectsRecorded map[string]bool
		// Records the number of times a mutable side effect was called per ID over the
		// life of the workflow. Used to help distinguish multiple calls to MutableSideEffect in the same
		// WorkflowTask.
		mutableSideEffectCallCounter map[string]int

		// LocalActivities have a separate, individual counter instead of relying on actual commandEventIDs.
		// This is because command IDs are only incremented on activity completion, which breaks
		// local activities that are spawned in parallel as they would all share the same command ID
		localActivityCounterID int64

		sideEffectCounterID int64

		currentReplayTime time.Time // Indicates current replay time of the command.
		currentLocalTime  time.Time // Local time when currentReplayTime was updated.

		completeHandler completionHandler                                                          // events completion handler
		cancelHandler   func()                                                                     // A cancel handler to be invoked on a cancel notification
		signalHandler   func(name string, input *commonpb.Payloads, header *commonpb.Header) error // A signal handler to be invoked on a signal event
		queryHandler    func(queryType string, queryArgs *commonpb.Payloads, header *commonpb.Header) (*commonpb.Payloads, error)
		updateHandler   func(name string, id string, args *commonpb.Payloads, header *commonpb.Header, callbacks UpdateCallbacks)

		logger                log.Logger
		isReplay              bool // flag to indicate if workflow is in replay mode
		enableLoggingInReplay bool // flag to indicate if workflow should enable logging in replay mode

		metricsHandler           metrics.Handler
		registry                 *registry
		dataConverter            converter.DataConverter
		failureConverter         converter.FailureConverter
		contextPropagators       []ContextPropagator
		deadlockDetectionTimeout time.Duration
		sdkFlags                 *sdkFlags
		sdkVersionUpdated        bool
		sdkVersion               string
		sdkNameUpdated           bool
		sdkName                  string
		// Any update requests received in a workflow task before we have registered
		// any handlers are not scheduled and are queued here until either their
		// handler is registered or the event loop runs out of work and they are rejected.
		bufferedUpdateRequests map[string][]func()

		protocols *protocol.Registry
	}

	localActivityTask struct {
		sync.Mutex
		workflowTask    *workflowTask
		activityID      string
		params          *ExecuteLocalActivityParams
		callback        LocalActivityResultHandler
		wc              *workflowExecutionContextImpl
		canceled        bool
		cancelFunc      func()
		attempt         int32  // attempt starting from 1
		attemptsThisWFT uint32 // Number of attempts started during this workflow task
		pastFirstWFT    bool   // Set true once this LA has lived for more than one workflow task
		retryPolicy     *RetryPolicy
		expireTime      time.Time
		scheduledTime   time.Time // Time the activity was scheduled initially.
		header          *commonpb.Header
	}

	localActivityMarkerData struct {
		ActivityID   string
		ActivityType string
		ReplayTime   time.Time
		Attempt      int32         // record attempt, starting from 1.
		Backoff      time.Duration // retry backoff duration.
	}
)

var (
	// ErrUnknownMarkerName is returned if there is unknown marker name in the history.
	ErrUnknownMarkerName = errors.New("unknown marker name")
	// ErrMissingMarkerDetails is returned when marker details are nil.
	ErrMissingMarkerDetails = errors.New("marker details are nil")
	// ErrMissingMarkerDataKey is returned when marker details doesn't have data key.
	ErrMissingMarkerDataKey = errors.New("marker key is missing in details")
	// ErrUnknownHistoryEvent is returned if there is an unknown event in history and the SDK needs to handle it
	ErrUnknownHistoryEvent = errors.New("unknown history event")
)

func newWorkflowExecutionEventHandler(
	workflowInfo *WorkflowInfo,
	completeHandler completionHandler,
	logger log.Logger,
	enableLoggingInReplay bool,
	metricsHandler metrics.Handler,
	registry *registry,
	dataConverter converter.DataConverter,
	failureConverter converter.FailureConverter,
	contextPropagators []ContextPropagator,
	deadlockDetectionTimeout time.Duration,
	capabilities *workflowservice.GetSystemInfoResponse_Capabilities,
) workflowExecutionEventHandler {
	_ = "STUB: not implemented"
	return *new(workflowExecutionEventHandler)
}

// Attempt to skip 1 log level to remove the ReplayLogger from the stack.

func (s *scheduledTimer) handle(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (s *scheduledActivity) handle(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (s *scheduledChildWorkflow) handle(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (s *scheduledChildWorkflow) handleFailedToStart(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (t *localActivityTask) cancel() { _ = "STUB: not implemented"; return }

func (s *scheduledCancellation) handle(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (s *scheduledSignal) handle(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) takeOutgoingMessages() []*protocolpb.Message {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) ScheduleUpdate(name string, id string, args *commonpb.Payloads, hdr *commonpb.Header, callbacks UpdateCallbacks) {
	_ = "STUB: not implemented"
	return
}

func withExpectedEventPredicate(pred func(*historypb.HistoryEvent) bool) msgSendOpt {
	_ = "STUB: not implemented"
	return *new(msgSendOpt)
}

func (wc *workflowEnvironmentImpl) Send(msg *protocolpb.Message, opts ...msgSendOpt) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) getNewSdkNameAndReset() string {
	_ = "STUB: not implemented"
	return ""
}

func (wc *workflowEnvironmentImpl) getNewSdkVersionAndReset() string {
	_ = "STUB: not implemented"
	return ""
}

func (wc *workflowEnvironmentImpl) getNextLocalActivityID() string {
	_ = "STUB: not implemented"
	return ""
}

func (wc *workflowEnvironmentImpl) getNextSideEffectID() int64 { _ = "STUB: not implemented"; return 0 }

func (wc *workflowEnvironmentImpl) WorkflowInfo() *WorkflowInfo {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) TypedSearchAttributes() SearchAttributes {
	_ = "STUB: not implemented"
	return *new(SearchAttributes)
}

func (wc *workflowEnvironmentImpl) Complete(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) RequestCancelChildWorkflow(namespace string, workflowID string) {
	_ = "STUB: not implemented"
	// For cancellation of child workflow only, we do not use cancellation ID and run ID
	return
}

func (wc *workflowEnvironmentImpl) RequestCancelExternalWorkflow(namespace, workflowID, runID string, callback ResultHandler) {
	_ = "STUB: not implemented"
	// for cancellation of external workflow, we have to use cancellation ID and set isChildWorkflowOnly to false
	return
}

func (wc *workflowEnvironmentImpl) SignalExternalWorkflow(
	namespace string,
	workflowID string,
	runID string,
	signalName string,
	input *commonpb.Payloads,
	_ /* THIS IS FOR TEST FRAMEWORK. DO NOT USE HERE. */ interface{},
	header *commonpb.Header,
	childWorkflowOnly bool,
	callback ResultHandler,
) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) UpsertSearchAttributes(attributes map[string]interface{}) error {
	_ = "STUB: not implemented"
	// This has to be used in WorkflowEnvironment implementations instead of in Workflow for testsuite mock purpose.
	return nil
}

// to ensure backward compatibility on searchable GetVersion, use latest changeVersion as upsertID

// this is for getInfo correctness

func (wc *workflowEnvironmentImpl) UpsertTypedSearchAttributes(attributes SearchAttributes) error {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) updateWorkflowInfoWithSearchAttributes(attributes *commonpb.SearchAttributes) {
	_ = "STUB: not implemented"
	return
}

func mergeSearchAttributes(current, upsert *commonpb.SearchAttributes) *commonpb.SearchAttributes {
	_ = "STUB: not implemented"
	return nil
}

func validateAndSerializeSearchAttributes(attributes map[string]interface{}) (*commonpb.SearchAttributes, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *workflowEnvironmentImpl) UpsertMemo(memoMap map[string]interface{}) error {
	_ = "STUB: not implemented"
	// This has to be used in WorkflowEnvironment implementations instead of in Workflow for testsuite mock purpose.
	return nil
}

// this is for getInfo correctness

func (wc *workflowEnvironmentImpl) updateWorkflowInfoWithMemo(memo *commonpb.Memo) {
	_ = "STUB: not implemented"
	return
}

func mergeMemo(current, upsert *commonpb.Memo) *commonpb.Memo {
	_ = "STUB: not implemented"
	return nil
}

func validateAndSerializeMemo(memoMap map[string]interface{}, dc converter.DataConverter, useUserDC bool) (*commonpb.Memo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *workflowEnvironmentImpl) RegisterCancelHandler(handler func()) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) ExecuteChildWorkflow(
	params ExecuteWorkflowParams, callback ResultHandler, startedHandler func(r WorkflowExecution, e error),
) {
	_ = "STUB: not implemented"
	// Backward compatibility: generate WorkflowID if not set by caller.
	// The Go SDK interceptor sets this before serialization so it's available
	// to context-aware codecs, but bindings callers may not set it.
	return
}

//lint:ignore SA1019 deprecated namespace field

//lint:ignore SA1019 ignore deprecated old versioning APIs

func (wc *workflowEnvironmentImpl) ExecuteNexusOperation(params ExecuteNexusOperationParams, callback func(*commonpb.Payload, error), startedHandler func(token string, e error)) int64 {
	_ = "STUB: not implemented"
	return 0
}

func (wc *workflowEnvironmentImpl) RequestCancelNexusOperation(seq int64) {
	_ = "STUB: not implemented"
	return
}

// Make sure to unblock the futures.

func (wc *workflowEnvironmentImpl) RegisterSignalHandler(
	handler func(name string, input *commonpb.Payloads, header *commonpb.Header) error,
) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) RegisterQueryHandler(
	handler func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error),
) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) RegisterUpdateHandler(
	handler func(string, string, *commonpb.Payloads, *commonpb.Header, UpdateCallbacks),
) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) GetLogger() log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (wc *workflowEnvironmentImpl) GetMetricsHandler() metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (wc *workflowEnvironmentImpl) GetDataConverter() converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

func (wc *workflowEnvironmentImpl) GetFailureConverter() converter.FailureConverter {
	_ = "STUB: not implemented"
	return *new(converter.FailureConverter)
}

func (wc *workflowEnvironmentImpl) GetContextPropagators() []ContextPropagator {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) IsReplaying() bool { _ = "STUB: not implemented"; return false }

func (wc *workflowEnvironmentImpl) GenerateSequenceID() string {
	_ = "STUB: not implemented"
	return ""
}

func (wc *workflowEnvironmentImpl) GenerateSequence() int64 { _ = "STUB: not implemented"; return 0 }

func (wc *workflowEnvironmentImpl) CreateNewCommand(commandType enumspb.CommandType) *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) ExecuteActivity(parameters ExecuteActivityParams, callback ResultHandler) ActivityID {
	_ = "STUB: not implemented"
	// Backward compatibility: generate ScheduleID/ActivityID if not set by caller.
	// The Go SDK interceptor sets these before serialization so they're available
	// to context-aware codecs, but bindings callers may not set them.
	return *new(ActivityID)
}

// We set this as true if not disabled on the params knowing it will be set as
// false just before request by the eager activity executor if eager activity
// execution is otherwise disallowed

func (wc *workflowEnvironmentImpl) RequestCancelActivity(activityID ActivityID) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) ExecuteLocalActivity(params ExecuteLocalActivityParams, callback LocalActivityResultHandler) LocalActivityID {
	_ = "STUB: not implemented"
	return *new(LocalActivityID)
}

func newLocalActivityTask(params ExecuteLocalActivityParams, callback LocalActivityResultHandler, activityID string) *localActivityTask {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) RequestCancelLocalActivity(activityID LocalActivityID) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) SetCurrentReplayTime(replayTime time.Time) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) Now() time.Time {
	_ = "STUB: not implemented"
	return *new(time.Time)
}

func (wc *workflowEnvironmentImpl) NewTimer(d time.Duration, options TimerOptions, callback ResultHandler) *TimerID {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) RequestCancelTimer(timerID TimerID) {
	_ = "STUB: not implemented"
	return
}

func validateVersion(changeID string, version, minSupported, maxSupported Version) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) GetVersion(changeID string, minSupported, maxSupported Version) Version {
	_ = "STUB: not implemented"
	return *new(Version)
}

// GetVersion for changeID is called first time in replay mode, use DefaultVersion

// GetVersion for changeID is called first time (non-replay mode), generate a marker command for it.
// Also upsert search attributes to enable ability to search by changeVersion.

// Server has a limit for the max size of a single search attribute value. If we exceed the default limit
// do not try to upsert as it will cause the workflow to fail.

func createSearchAttributesForChangeVersion(changeID string, version Version, existingChangeVersions map[string]Version) map[string]interface{} {
	_ = "STUB: not implemented"
	return nil
}

func getChangeVersions(changeID string, version Version, existingChangeVersions map[string]Version) []string {
	_ = "STUB: not implemented"
	return nil
}

func getChangeVersion(changeID string, version Version) string {
	_ = "STUB: not implemented"
	return ""
}

func (wc *workflowEnvironmentImpl) SideEffect(f func() (*commonpb.Payloads, error), callback ResultHandler, summary string) {
	_ = "STUB: not implemented"
	return
}

// Once the SideEffect has been consumed, we can free the referenced payload
// to reduce memory pressure

func (wc *workflowEnvironmentImpl) TryUse(flag sdkFlag) bool {
	_ = "STUB: not implemented"
	return false
}

func (wc *workflowEnvironmentImpl) QueueUpdate(name string, f func()) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) HandleQueuedUpdates(name string) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) DrainUnhandledUpdates() bool {
	_ = "STUB: not implemented"
	return false

	// Check if any buffered update requests remain when we have no more coroutines to run and let them schedule so they are rejected.
	// Generally iterating a map in workflow code is bad because it is non deterministic
	// this case is fine since all these update handles will be rejected and not recorded in history.
}

// lookupMutableSideEffect gets the current value of the MutableSideEffect for id for the
// current call count of id.
func (wc *workflowEnvironmentImpl) lookupMutableSideEffect(id string) *commonpb.Payloads {
	_ = "STUB: not implemented"
	// Fail if ID not found
	return nil
}

// Find the most recent call at/before the current call count

// Garbage collect old entries

func (wc *workflowEnvironmentImpl) MutableSideEffect(id string, f func() interface{}, equals func(a, b interface{}) bool, summary string) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

// During replay, we only generate a command if there was a known marker
// recorded on the next task. We have to append the current command
// counter to the user-provided ID to avoid duplicates.

// This should not happen

func (wc *workflowEnvironmentImpl) isEqualValue(newValue interface{}, encodedOldValue *commonpb.Payloads, equals func(a, b interface{}) bool) bool {
	_ = "STUB: not implemented"
	return false

	// new value is nil
}

func decodeValue(encodedValue converter.EncodedValue, value interface{}) interface{} {
	_ = "STUB: not implemented"
	// We need to decode oldValue out of encodedValue, first we need to prepare valuePtr as the same type as value
	return nil
}

func (wc *workflowEnvironmentImpl) encodeValue(value interface{}) *commonpb.Payloads {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) encodeArg(arg interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *workflowEnvironmentImpl) recordMutableSideEffect(id string, callCountHint int, data *commonpb.Payloads, summary string) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

func (wc *workflowEnvironmentImpl) AddSession(sessionInfo *SessionInfo) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) RemoveSession(sessionID string) {
	_ = "STUB: not implemented"
	return
}

func (wc *workflowEnvironmentImpl) getOpenSessions() []*SessionInfo {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentImpl) GetRegistry() *registry {
	_ = "STUB: not implemented"

	// ResetLAWFTAttemptCounts resets the number of attempts in this WFT for all LAs to 0 - should be
	// called at the beginning of every WFT
	return nil
}

func (wc *workflowEnvironmentImpl) ResetLAWFTAttemptCounts() { _ = "STUB: not implemented"; return }

// GatherLAAttemptsThisWFT returns the total number of attempts in this WFT for all LAs who are
// past their first WFT
func (wc *workflowEnvironmentImpl) GatherLAAttemptsThisWFT() uint32 {
	_ = "STUB: not implemented"
	return 0
}

func (weh *workflowExecutionEventHandlerImpl) ProcessEvent(
	event *historypb.HistoryEvent,
	isReplay bool,
	isLast bool,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

// No Operation

// No Operation

// No Operation

// No Operation

// Set replay clock.

// Update workflow info fields

// Reset the counter on command helper used for generating ID for commands

// No Operation

// update the childWorkflowIDSeed if the workflow was reset at this point.

// No Operation

// No Operation

// No Operation.

// No Operation

// No Operation.

//lint:ignore SA1019 ignore deprecated control

// No Operation

// No Operation

// No Operation

// No Operation

// all forms of completions are handled by the same method.

// Do not fail to be forward compatible with new events

// When replaying histories to get stack trace or current state the last event might be not
// workflow task started. So always call OnWorkflowTaskStarted on the last event.
// Don't call for EventType_WorkflowTaskStarted as it was already called when handling it.

func (weh *workflowExecutionEventHandlerImpl) ProcessMessage(
	msg *protocolpb.Message,
	isReplay bool,
	isLast bool,
) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) ProcessQuery(
	queryType string,
	queryArgs *commonpb.Payloads,
	header *commonpb.Header,
) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// We are intentionally not handling this here but rather in the
// normal handler so it has access to the options/context as
// needed.

func (weh *workflowExecutionEventHandlerImpl) StackTrace() string {
	_ = "STUB: not implemented"
	return ""
}

func (weh *workflowExecutionEventHandlerImpl) Close() { _ = "STUB: not implemented"; return }

func (weh *workflowExecutionEventHandlerImpl) handleWorkflowExecutionStarted(
	attributes *historypb.WorkflowExecutionStartedEventAttributes,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

// We set this flag at workflow start because changing it on a mid-workflow
// WFT results in inconsistent values for SDKFlags during replay (i.e.
// replay sees the _final_ value of applied flags, not intermediate values
// as the value varies by WFT)

// Invoke the workflow.

func (weh *workflowExecutionEventHandlerImpl) handleActivityTaskCompleted(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleActivityTaskFailed(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleActivityTaskTimedOut(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleActivityTaskCanceled(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

// Clear this so we don't have a recursive call that while executing might call the cancel one.

func (weh *workflowExecutionEventHandlerImpl) handleTimerFired(event *historypb.HistoryEvent) {
	_ = "STUB: not implemented"
	return
}

func (weh *workflowExecutionEventHandlerImpl) handleWorkflowExecutionCancelRequested() {
	_ = "STUB: not implemented"
	return
}

func (weh *workflowExecutionEventHandlerImpl) handleMarkerRecorded(
	eventID int64,
	attributes *historypb.MarkerRecordedEventAttributes,
) error {
	_ = "STUB: not implemented"
	return nil
}

// versionSearchAttributeUpdatedName is optional and was only added later so do not expect all version
// markers to have this.

// Side effect data is actually a wrapper of ID + data, so we need to
// extract the second value as the actual data

// An old version of the SDK did not write the counter hint so we have to assume.
// If multiple mutable side effects on the same ID are in a WFT only the last value is used.

// We must mark that it is recorded so we can know whether a command
// needs to be generated during replay

// This must be stored with the counter

func (weh *workflowExecutionEventHandlerImpl) handleLocalActivityMarker(details map[string]*commonpb.Payloads, failure *failurepb.Failure, params LocalActivityMarkerParams) error {
	_ = "STUB: not implemented"
	return nil
}

// history marker mismatch to the current code.

// Result might not be there if local activity doesn't have return value.

// update time

// resume workflow execution after apply local activity result

func (weh *workflowExecutionEventHandlerImpl) ProcessLocalActivityResult(lar *localActivityResult) error {
	_ = "STUB: not implemented"
	return nil
}

// convert local activity result and error to marker data

// encode marker data

// create marker event for local activity result

// apply the local activity result to workflow

func (weh *workflowExecutionEventHandlerImpl) handleWorkflowExecutionSignaled(
	attributes *historypb.WorkflowExecutionSignaledEventAttributes,
) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleStartChildWorkflowExecutionFailed(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleChildWorkflowExecutionStarted(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleChildWorkflowExecutionCompleted(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleChildWorkflowExecutionFailed(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleChildWorkflowExecutionCanceled(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleChildWorkflowExecutionTimedOut(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleChildWorkflowExecutionTerminated(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleNexusOperationStarted(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

//lint:ignore SA1019 this field is sent by servers older than 1.27.0.

func (weh *workflowExecutionEventHandlerImpl) handleNexusOperationCompleted(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

// This is only called internally and should never happen.

// Also unblock the start future

// We didn't get a started event, the operation completed synchronously.

func (weh *workflowExecutionEventHandlerImpl) handleNexusOperationCancelRequested(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleNexusOperationCancelRequestDelivered(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

// This is only called internally and should never happen.

// API version 1.47.0 was released without the ScheduledEventID field on these events, so if we got this event
// without that field populated, then just ignore and fall back to default WaitCompleted behavior.

func (weh *workflowExecutionEventHandlerImpl) handleUpsertWorkflowSearchAttributes(event *historypb.HistoryEvent) {
	_ = "STUB: not implemented"
	return
}

func (weh *workflowExecutionEventHandlerImpl) handleWorkflowPropertiesModified(
	event *historypb.HistoryEvent,
) {
	_ = "STUB: not implemented"
	return
}

func (weh *workflowExecutionEventHandlerImpl) handleRequestCancelExternalWorkflowExecutionInitiated(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	// For cancellation of child workflow only, we do not use cancellation ID
	// for cancellation of external workflow, we have to use cancellation ID
	return nil
}

//lint:ignore SA1019 ignore deprecated control

func (weh *workflowExecutionEventHandlerImpl) handleExternalWorkflowExecutionCancelRequested(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	// For cancellation of child workflow only, we do not use cancellation ID
	// for cancellation of external workflow, we have to use cancellation ID
	return nil
}

// for cancel external workflow, we need to set the future

func (weh *workflowExecutionEventHandlerImpl) handleRequestCancelExternalWorkflowExecutionFailed(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	// For cancellation of child workflow only, we do not use cancellation ID
	// for cancellation of external workflow, we have to use cancellation ID
	return nil
}

// for cancel external workflow, we need to set the future

func (weh *workflowExecutionEventHandlerImpl) handleSignalExternalWorkflowExecutionCompleted(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) handleSignalExternalWorkflowExecutionFailed(event *historypb.HistoryEvent) error {
	_ = "STUB: not implemented"
	return nil
}

func (weh *workflowExecutionEventHandlerImpl) protocolConstructorForMessage(
	msg *protocolpb.Message,
) (func() protocol.Instance, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertContinueAsNewSuggestedReasonsFromProto(
	reasons []enumspb.SuggestContinueAsNewReason,
) []ContinueAsNewSuggestedReason {
	_ = "STUB: not implemented"
	return nil
}
