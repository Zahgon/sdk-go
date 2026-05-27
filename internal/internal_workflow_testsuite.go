package internal

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/facebookgo/clock"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/durationpb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	defaultTestNamespace        = "default-test-namespace"
	defaultTestTaskQueue        = "default-test-taskqueue"
	defaultTestWorkflowID       = "default-test-workflow-id"
	defaultTestRunID            = "default-test-run-id"
	defaultTestWorkflowTypeName = "default-test-workflow-type-name"
	workflowTypeNotSpecified    = "workflow-type-not-specified"

	// These are copied from service implementation
	reservedTaskQueuePrefix = "/__temporal_sys/"
	maxIDLengthLimit        = 1000
	maxWorkflowTimeout      = 24 * time.Hour * 365 * 10

	defaultMaximumAttemptsForUnitTest = 10
)

type (
	testTimerHandle struct {
		env            *testWorkflowEnvironmentImpl
		callback       ResultHandler
		timer          *clock.Timer
		wallTimer      *clock.Timer
		duration       time.Duration
		mockTimeToFire time.Time
		wallTimeToFire time.Time
		timerID        int64
	}

	testActivityHandle struct {
		callback         ResultHandler
		heartbeatDetails *commonpb.Payloads
		token            testActivityToken
		task             *workflowservice.PollActivityTaskQueueResponse
		// Per-activity context-aware converters for async completion path.
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter
		// Timeout tracking
		startTime         time.Time // when activity started executing
		lastHeartbeatTime time.Time
		// Timeout result (set by monitoring goroutine)
		timedOut           bool
		timeoutType        enumspb.TimeoutType // which timeout occurred
		cancelTimeoutWatch func()              // cancels the timeout monitoring goroutine
	}

	testWorkflowHandle struct {
		env      *testWorkflowEnvironmentImpl
		callback ResultHandler
		handled  bool
		params   *ExecuteWorkflowParams
		err      error
	}

	testNexusOperationHandle struct {
		env             *testWorkflowEnvironmentImpl
		seq             int64
		params          ExecuteNexusOperationParams
		operationToken  string
		cancelRequested bool
		started         bool
		done            bool
		onCompleted     func(*commonpb.Payload, error)
		onStarted       func(opID string, e error)
		isMocked        bool
	}

	testNexusAsyncOperationHandle struct {
		result *commonpb.Payload
		err    error
		delay  time.Duration
	}

	// Interface for nexus.OperationReference without the types as generics.
	testNexusOperationReference interface {
		Name() string
		InputType() reflect.Type
		OutputType() reflect.Type
	}

	testCallbackHandle struct {
		callback          func()
		startWorkflowTask bool // start a new workflow task after callback() is handled.
		env               *testWorkflowEnvironmentImpl
	}

	// activityTimeoutResult is a marker type used to indicate that an activity
	// timed out due to missing heartbeats or exceeding StartToCloseTimeout.
	activityTimeoutResult struct {
		timeoutType enumspb.TimeoutType
		details     *commonpb.Payloads // last heartbeat details (for heartbeat timeout)
	}

	activityExecutorWrapper struct {
		*activityExecutor
		env *testWorkflowEnvironmentImpl
	}

	workflowExecutorWrapper struct {
		*workflowExecutor
		env *testWorkflowEnvironmentImpl
	}

	mockWrapper struct {
		env           *testWorkflowEnvironmentImpl
		name          string
		fn            interface{}
		isWorkflow    bool
		dataConverter converter.DataConverter
	}

	taskQueueSpecificActivity struct {
		fn         interface{}
		taskQueues map[string]struct{}
	}

	updateResult struct {
		success   interface{}
		err       error
		update_id string
		callbacks []updateCallbacksWrapper
		completed bool
	}

	// testWorkflowEnvironmentShared is the shared data between parent workflow and child workflow test environments
	testWorkflowEnvironmentShared struct {
		locker    sync.Mutex
		testSuite *WorkflowTestSuite

		taskQueueSpecificActivities map[string]*taskQueueSpecificActivity

		workflowMock              *mock.Mock
		activityMock              *mock.Mock
		nexusMock                 *mock.Mock
		service                   workflowservice.WorkflowServiceClient
		logger                    log.Logger
		metricsHandler            metrics.Handler
		contextPropagators        []ContextPropagator
		identity                  string
		detachedChildWaitDisabled bool

		mockClock *clock.Mock
		wallClock clock.Clock

		callbackChannel            chan testCallbackHandle
		testTimeout                time.Duration
		activityTimeoutGracePeriod time.Duration // grace period for activities to react to context deadline
		header                     *commonpb.Header

		counterID              int64
		activities             map[testActivityToken]*testActivityHandle
		localActivities        map[string]*localActivityTask
		timers                 map[string]*testTimerHandle
		runningWorkflows       map[string]*testWorkflowHandle
		runningNexusOperations map[int64]*testNexusOperationHandle
		nexusAsyncOpHandle     map[string]*testNexusAsyncOperationHandle
		nexusOperationRefs     map[string]map[string]testNexusOperationReference

		runningCount int

		expectedWorkflowMockCalls map[string]struct{}
		expectedActivityMockCalls map[string]struct{}
		expectedNexusMockCalls    map[string]struct{}

		onActivityStartedListener         func(activityInfo *ActivityInfo, ctx context.Context, args converter.EncodedValues)
		onActivityCompletedListener       func(activityInfo *ActivityInfo, result converter.EncodedValue, err error)
		onActivityCanceledListener        func(activityInfo *ActivityInfo)
		onLocalActivityStartedListener    func(activityInfo *ActivityInfo, ctx context.Context, args []interface{})
		onLocalActivityCompletedListener  func(activityInfo *ActivityInfo, result converter.EncodedValue, err error)
		onLocalActivityCanceledListener   func(activityInfo *ActivityInfo)
		onActivityHeartbeatListener       func(activityInfo *ActivityInfo, details converter.EncodedValues)
		onChildWorkflowStartedListener    func(workflowInfo *WorkflowInfo, ctx Context, args converter.EncodedValues)
		onChildWorkflowCompletedListener  func(workflowInfo *WorkflowInfo, result converter.EncodedValue, err error)
		onChildWorkflowCanceledListener   func(workflowInfo *WorkflowInfo)
		onTimerScheduledListener          func(timerID string, duration time.Duration)
		onTimerFiredListener              func(timerID string)
		onTimerCanceledListener           func(timerID string)
		onNexusOperationStartedListener   func(service string, operation string, args converter.EncodedValue)
		onNexusOperationCompletedListener func(service string, operation string, result converter.EncodedValue, err error)
		onNexusOperationCanceledListener  func(service string, operation string)
	}

	// testWorkflowEnvironmentImpl is the environment that runs the workflow/activity unit tests.
	testWorkflowEnvironmentImpl struct {
		*testWorkflowEnvironmentShared
		parentEnv *testWorkflowEnvironmentImpl
		registry  *registry

		workflowInfo   *WorkflowInfo
		workflowDef    WorkflowDefinition
		changeVersions map[string]Version
		openSessions   map[string]*SessionInfo

		workflowCancelHandler func()
		signalHandler         func(name string, input *commonpb.Payloads, header *commonpb.Header) error
		queryHandler          func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error)
		updateHandler         func(name string, id string, input *commonpb.Payloads, header *commonpb.Header, resp UpdateCallbacks)
		updateMap             map[string]*updateResult
		startedHandler        func(r WorkflowExecution, e error)

		isWorkflowCompleted bool
		testResult          converter.EncodedValue
		testError           error
		doneChannel         chan struct{}
		doneChannelOnce     sync.Once
		workerOptions       WorkerOptions
		dataConverter       converter.DataConverter
		failureConverter    converter.FailureConverter
		runTimeout          time.Duration

		heartbeatDetails *commonpb.Payloads

		workerStopChannel  chan struct{}
		sessionEnvironment *testSessionEnvironmentImpl

		// True if this was created only for testing activities not workflows.
		activityEnvOnly             bool
		executeActivitiesInWorkflow bool

		workflowFunctionExecuting bool
		bufferedUpdateRequests    map[string][]func()

		sdkFlags *sdkFlags
	}

	testSessionEnvironmentImpl struct {
		*sessionEnvironmentImpl
		testWorkflowEnvironment *testWorkflowEnvironmentImpl
	}

	// UpdateCallbacksWrapper is a wrapper to UpdateCallbacks. It allows us to dedup duplicate update IDs in the test environment.
	updateCallbacksWrapper struct {
		uc       UpdateCallbacks
		env      *testWorkflowEnvironmentImpl
		updateID string
	}

	testActivityToken struct {
		activityID string
		runID      string
	}
)

func newTestWorkflowEnvironmentImpl(s *WorkflowTestSuite, parentRegistry *registry) *testWorkflowEnvironmentImpl {
	_ = "STUB: not implemented"
	return nil
}

// move forward the mock clock to start time.

// put current workflow as a running workflow so child can send signal to parent

// setup mock service

// need lock as this is running in activity worker's goroutinue

// If we're only in an activity environment, posted callbacks are not
// invoked

func (env *testWorkflowEnvironmentImpl) setStartTime(startTime time.Time) {
	_ = "STUB: not implemented"
	// move forward the mock clock to start time.
	return
}

// if start time not set, use current clock time

func (env *testWorkflowEnvironmentImpl) setCurrentHistoryLength(length int) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setCurrentHistorySize(size int) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setContinueAsNewSuggested(suggest bool) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setContinueAsNewSuggestedReasons(reasons []ContinueAsNewSuggestedReason) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setTargetWorkerDeploymentVersionChanged(changed bool) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setContinuedExecutionRunID(rid string) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) newTestWorkflowEnvironmentForChild(
	params *ExecuteWorkflowParams,
	callback ResultHandler,
	startedHandler func(r WorkflowExecution, e error),
) (*testWorkflowEnvironmentImpl, error) {
	_ = "STUB: not implemented"
	// create a new test env
	return nil, nil
}

// set workflow info data for child workflow

// duplicate workflow ID

func (env *testWorkflowEnvironmentImpl) setWorkerOptions(options WorkerOptions) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setIdentity(identity string) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setDataConverter(dataConverter converter.DataConverter) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setFailureConverter(failureConverter converter.FailureConverter) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setContextPropagators(contextPropagators []ContextPropagator) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setWorkerStopChannel(c chan struct{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setDetachedChildWaitDisabled(detachedChildWaitDisabled bool) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setActivityTaskQueue(taskqueue string, activityFns ...interface{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) executeWorkflow(workflowFn interface{}, args ...interface{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) executeWorkflowInternal(delayStart time.Duration, workflowType string, input *commonpb.Payloads) {
	_ = "STUB: not implemented"
	return
}

// Current TestWorkflowEnvironment only support to run one workflow.
// Created task to support testing multiple workflows with one env instance
// https://github.com/temporalio/go-sdk/issues/50

// For child workflows, the interceptor already wraps converters with the
// child's WorkflowSerializationContext before setting them on the child env.
// Only wrap for the root workflow to avoid double-wrapping.

// env.workflowDef.Execute() method will execute dispatcher. We want the dispatcher to only run in main loop.
// In case of child workflow, this executeWorkflowInternal() is run in separate goroutinue, so use postCallback
// to make sure workflowDef.Execute() is run in main loop.

// kick off first workflow task to start the workflow

// we need to delayStart start workflow, decrease runningCount so mockClock could auto forward

func (env *testWorkflowEnvironmentImpl) getWorkflowDefinition(wt WorkflowType) (WorkflowDefinition, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowDefinition), nil
}

func (env *testWorkflowEnvironmentImpl) TryUse(flag sdkFlag) bool {
	_ = "STUB: not implemented"
	return false
}

func (env *testWorkflowEnvironmentImpl) GenerateSequence() int64 {
	_ = "STUB: not implemented"
	return 0
}

func (env *testWorkflowEnvironmentImpl) QueueUpdate(name string, f func()) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) HandleQueuedUpdates(name string) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) DrainUnhandledUpdates() bool {
	_ = "STUB: not implemented"
	// Due to mock registration the test environment cannot run the workflow function
	// in the first "workflow task". We need to delay the draining until the main function has
	// had a chance to run.
	return false
}

// Check if any buffered update requests remain when we have no more coroutines to run and let them schedule so they are rejected.
// Generally iterating a map in workflow code is bad because it is non deterministic
// this case is fine since all these update handles will be rejected and not recorded in history.

func (env *testWorkflowEnvironmentImpl) executeActivity(
	activityFn interface{},
	args ...interface{},
) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// ensure activityFn is registered to defaultTestTaskQueue

// will never happen

func (env *testWorkflowEnvironmentImpl) executeLocalActivity(
	activityFn interface{},
	args ...interface{},
) (val converter.EncodedValue, err error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

func (env *testWorkflowEnvironmentImpl) startWorkflowTask() { _ = "STUB: not implemented"; return }

func (env *testWorkflowEnvironmentImpl) isChildWorkflow() bool {
	_ = "STUB: not implemented"
	return false
}

func (env *testWorkflowEnvironmentImpl) closeDoneChannel() { _ = "STUB: not implemented"; return }

func (env *testWorkflowEnvironmentImpl) startMainLoop() { _ = "STUB: not implemented"; return }

// child workflow rely on parent workflow's main loop to process events
// wait until workflow is complete

// notify all child workflows to exit their main loop

// use non-blocking-select to check if there is anything pending in the main thread.

// this will drain the callbackChannel

// nothing to process, main thread is blocked at this moment, now check if we should auto fire next timer

// no timer to fire, wait for things to do or timeout.

// not able to complete workflow within test timeout, workflow likely stuck somewhere,
// check workflow stack for more details.

func (env *testWorkflowEnvironmentImpl) shouldStopEventLoop() bool {
	_ = "STUB: not implemented"
	// Check if any detached children are still running if not disabled.
	return false
}

// ignore root workflow

func (env *testWorkflowEnvironmentImpl) registerDelayedCallback(f func(), delayDuration time.Duration) {
	_ = "STUB: not implemented"
	return
}

func (c *testCallbackHandle) processCallback() { _ = "STUB: not implemented"; return }

func (env *testWorkflowEnvironmentImpl) autoFireNextTimer() bool {
	_ = "STUB: not implemented"
	return false
}

// find next timer

// function to fire timer

// Move mockClock forward, this will fire the timer, and the timer callback will remove timer from timers.

// fire timer if there is no running activity

// nextTimer already set, meaning we already have a wall clock timer for the nextTimer setup earlier. And the
// previously scheduled wall time to fire is before the wallTimeToFire calculated this time. This could happen
// if workflow was blocked while there was activity running, and when that activity completed, there are some
// other activities still running while the nextTimer is still that same nextTimer. In that case, we should not
// reset the wall time to fire for the nextTimer.

// wallTimer was scheduled, but the wall time to fire should be earlier based on current calculation.

// there is running activities, we would fire next timer only if wall time passed by nextTimer duration.

// make sure it is running in the main loop

func (env *testWorkflowEnvironmentImpl) postCallback(cb func(), startWorkflowTask bool) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RequestCancelActivity(activityID ActivityID) {
	_ = "STUB: not implemented"
	return
}

// RequestCancelTimer request to cancel timer on this testWorkflowEnvironmentImpl.
func (env *testWorkflowEnvironmentImpl) RequestCancelTimer(timerID TimerID) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) Complete(result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return
}

// this is completion of child workflow

// It is possible that child workflow could complete after cancellation. In that case, childWorkflowHandle
// would have already been removed from the runningWorkflows map by RequestCancelWorkflow().

// check if a retry is needed

// rerun requested, so we don't want to post the error to parent workflow, return here.

// no rerun, child workflow is done.

// deliver result

/* true to trigger parent workflow to resume to handle child workflow's result */

// properly handle child workflows based on their ParentClosePolicy

func (env *testWorkflowEnvironmentImpl) handleParentClosePolicy() {
	_ = "STUB: not implemented"
	return
}

// noop

func (h *testWorkflowHandle) rerunAsChild() bool { _ = "STUB: not implemented"; return false }

// remove the current child workflow from the pending child workflow map because
// the childWorkflowID will be the same for retry run.

/* child workflow already started */

// pass down the last completion result

// TODO (shtin): convert env.testResult to *commonpb.Payloads

// not successful run this time, carry over from whatever previous run pass to this run.

// remove the current child workflow from the pending child workflow map because
// the childWorkflowID will be the same for retry run.

/* child workflow already started */

/* child workflow already started */

func (env *testWorkflowEnvironmentImpl) CompleteActivity(taskToken []byte, result interface{}, err error) error {
	_ = "STUB: not implemented"
	return nil
}

// We do allow canceled error to be passed here

/* do not auto schedule workflow task, because activity might be still pending */

func (env *testWorkflowEnvironmentImpl) GetLogger() log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (env *testWorkflowEnvironmentImpl) GetMetricsHandler() metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (env *testWorkflowEnvironmentImpl) GetDataConverter() converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

func (env *testWorkflowEnvironmentImpl) GetFailureConverter() converter.FailureConverter {
	_ = "STUB: not implemented"
	return *new(converter.FailureConverter)
}

func (env *testWorkflowEnvironmentImpl) GetContextPropagators() []ContextPropagator {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) ExecuteActivity(parameters ExecuteActivityParams, callback ResultHandler) ActivityID {
	_ = "STUB: not implemented"
	return *new(ActivityID)
}

// Backward compatibility: generate ScheduleID/ActivityID if not set by caller.

// Start timeout monitoring if any timeout is configured

// Determine check interval - use the smallest configured timeout divided by 2

// activity already completed

// Check StartToCloseTimeout first (it's more severe)
// Add grace period to give well-behaved activities time to react to context deadline

// Check HeartbeatTimeout

// activity runs in separate goroutinue outside of workflow dispatcher
// do callback in a defer to handle calls to runtime.Goexit inside the activity (which is done by t.FailNow)

// Stop timeout monitoring

// already closed

// Check if any timeout occurred

// Override result with timeout error

// post activity result to workflow dispatcher

/* do not auto schedule workflow task, because activity might be still pending */

func minDur(a *durationpb.Duration, b *durationpb.Duration) *durationpb.Duration {
	_ = "STUB: not implemented"
	return nil
}

// Copy of the server function func (v *commandAttrValidator) validateActivityScheduleAttributes
func (env *testWorkflowEnvironmentImpl) validateActivityScheduleAttributes(
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
	runTimeout time.Duration,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.

// We are in !validScheduleToClose due to the first if above

// Deduction failed as there's not enough information to fill in missing timeouts.

// ensure activity timeout never larger than workflow timeout

// Copy of the service func (v *commandAttrValidator) validatedTaskQueue
func (env *testWorkflowEnvironmentImpl) validatedTaskQueue(
	taskQueue *taskqueuepb.TaskQueue,
	defaultVal string,
) (*taskqueuepb.TaskQueue, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// copy of the service func ValidateRetryPolicy(policy *commonpb.RetryPolicy)
func (env *testWorkflowEnvironmentImpl) validateRetryPolicy(policy *commonpb.RetryPolicy) error {
	_ = "STUB: not implemented"

	// nil policy is valid which means no retry
	return nil
}

// One maximum attempt effectively disable retries. Validating the
// rest of the arguments is pointless

func (env *testWorkflowEnvironmentImpl) addNewActivityHandle(task *workflowservice.PollActivityTaskQueueResponse, callback func(result *commonpb.Payloads, err error), dc converter.DataConverter, fc converter.FailureConverter) *testActivityHandle {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) getActivityHandle(token testActivityToken) (*testActivityHandle, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func (env *testWorkflowEnvironmentImpl) deleteHandle(token testActivityToken) {
	_ = "STUB: not implemented"
	return
}

func (t *testActivityToken) toBytes() []byte {
	_ = "STUB: not implemented"
	// we don't entirely control activity ID, so runID goes first to make reconstructing from bytes easier
	return nil
}

func activityTokenFromBytes(token []byte) (testActivityToken, bool) {
	_ = "STUB: not implemented"
	return *new(testActivityToken), false
}

func (env *testWorkflowEnvironmentImpl) executeActivityWithRetryForTest(
	taskHandler ActivityTaskHandler,
	parameters ExecuteActivityParams,
	task *workflowservice.PollActivityTaskQueueResponse,
) (result interface{}) {
	_ = "STUB: not implemented"
	return nil
}

// check if a retry is needed

// need a retry

// register the delayed call back first, otherwise other timers may be fired before the retry timer
// is enqueued.

// no retry

func fromProtoRetryPolicy(p *commonpb.RetryPolicy) *RetryPolicy {
	_ = "STUB: not implemented"
	return nil
}

func getRetryBackoffFromProtoRetryPolicy(prp *commonpb.RetryPolicy, attempt int32, err error, now, expireTime time.Time) time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

func ensureDefaultRetryPolicy(parameters *ExecuteActivityParams) {
	_ = "STUB: not implemented"
	// ensure default retry policy
	return
}

// NOTE: the default MaximumAttempts for retry policy set by server is 0 which means unlimited retries.
// However, unlimited retry with automatic fast forward clock in test framework will cause the CPU to spin and test
// to go forever. So we need to set a reasonable default max attempts for unit test.

func (env *testWorkflowEnvironmentImpl) ExecuteLocalActivity(params ExecuteLocalActivityParams, callback LocalActivityResultHandler) LocalActivityID {
	_ = "STUB: not implemented"
	return *new(LocalActivityID)
}

// local activity could be registered, if so use the registered name. This name is only used to find a mock.

// We have to skip the interceptors on the first call because
// ExecuteWithActualArgs is actually invoked twice to support a mock activity
// function result

// substitute the local activity function so we could replace with mock if it is supplied.

func (env *testWorkflowEnvironmentImpl) RequestCancelLocalActivity(activityID LocalActivityID) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) handleActivityResult(activityHandle *testActivityHandle, result interface{},
	dataConverter converter.DataConverter) {
	_ = "STUB: not implemented"
	return
}

// In case activity returns ErrActivityResultPending, the respond will be nil, and we don't need to do anything.
// Activity will need to complete asynchronously using CompleteActivity().

// this is running in dispatcher

// Activity timed out due to missing heartbeats or exceeding StartToCloseTimeout

// For StartToCloseTimeout, the cause is context.DeadlineExceeded since
// we set up the context deadline to match the timeout

func (env *testWorkflowEnvironmentImpl) wrapActivityError(activityID ActivityID, activityType string, retryState enumspb.RetryState, activityErr error) error {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) handleLocalActivityResult(result *localActivityResult) {
	_ = "STUB: not implemented"
	return
}

// If error is present do not return value

// Always return CanceledError for canceled tasks

// runBeforeMockCallReturns is registered as mock call's RunFn by *mock.Call.Run(fn). It will be called by testify's
// mock.MethodCalled() before it returns.
func (env *testWorkflowEnvironmentImpl) runBeforeMockCallReturns(call *MockCallWrapper, args mock.Arguments) {
	_ = "STUB: not implemented"
	return
}

// we want this mock call to block until the wait duration is elapsed (on workflow clock).

// increase runningCount as the mock call is ready to resume.
// this will unblock mock call

// make sure decrease runningCount after delayed callback is posted

// reduce runningCount, since this mock call is about to be blocked.

// this will block until mock clock move forward by waitDuration

// run the actual runFn if it was setup

// Execute executes the activity code.
func (a *activityExecutorWrapper) Execute(ctx context.Context, input *commonpb.Payloads) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// If activity handle cannot be found, we assume it was cancelled

// wait until listener returns

// ExecuteWithActualArgs executes the activity code.
func (a *activityExecutorWrapper) ExecuteWithActualArgs(ctx context.Context, inputArgs []interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// check if mock returns function which must match to the actual function.

// Execute executes the workflow code.
func (w *workflowExecutorWrapper) Execute(ctx Context, input *commonpb.Payloads) (result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// This is to prevent auto-forwarding mock clock before main workflow starts. For child workflow, we increase
// the counter in env.ExecuteChildWorkflow(). We cannot do it here for child workflow, because we need to make
// sure the counter is increased before returning from ExecuteChildWorkflow().

// This method is called by workflow's dispatcher. In this test suite, it is run in the main loop. We cannot block
// the main loop, but the mock could block if it is configured to wait. So we need to use a separate goroutinue to
// run the mock, and resume after mock call returns.

// make a copy of the context for getWorkflowMockReturn() call to avoid race condition

// Ensure ctxCopy matches real execution: apply header propagation to the context

// getWorkflowMockReturn could block if mock is configured to wait. The returned mockRet is what has been configured
// for the mock by using MockCallWrapper.Return(). The mockRet could be mock values or mock function. We process
// the returned mockRet by calling executeMock() later in the main thread after it is send over via mockReadyChannel.

/* true to trigger the dispatcher for this workflow so it resume from mockReadyChannel block*/

// This will block workflow dispatcher (on temporal channel), which the dispatcher understand and will return from
// ExecuteUntilAllBlocked() so the main loop is not blocked. The dispatcher will unblock when getWorkflowMockReturn() returns.

// reduce runningCount to allow auto-forwarding mock clock after current workflow dispatcher run is blocked (aka
// ExecuteUntilAllBlocked() returns).

// workflow was mocked.

/* startedHandler could be nil for retry */
// notify parent that child workflow is started

// no mock, so call the actual workflow

func (m *mockWrapper) getCtxArg(ctx interface{}) []interface{} {
	_ = "STUB: not implemented"
	return nil
}

func (m *mockWrapper) getActivityMockReturn(ctx interface{}, input *commonpb.Payloads) (retArgs mock.Arguments) {
	_ = "STUB: not implemented"
	return *new(mock.Arguments)
}

// no mock

func (m *mockWrapper) getWorkflowMockReturn(ctx interface{}, input *commonpb.Payloads) (retArgs mock.Arguments) {
	_ = "STUB: not implemented"
	return *new(mock.Arguments)
}

// no mock

func (m *mockWrapper) getNexusMockReturn(
	ctx interface{},
	operation string,
	input interface{},
	options interface{},
) (retArgs mock.Arguments) {
	_ = "STUB: not implemented"
	return *new(mock.Arguments)
}

// no mock

func (m *mockWrapper) getMockReturn(ctx interface{}, input *commonpb.Payloads, envMock *mock.Mock) (retArgs mock.Arguments) {
	_ = "STUB: not implemented"
	return *new(mock.Arguments)
}

func (m *mockWrapper) getActivityMockReturnWithActualArgs(ctx interface{}, inputArgs []interface{}) (retArgs mock.Arguments) {
	_ = "STUB: not implemented"
	return *new(mock.Arguments)
}

// no mock

func (m *mockWrapper) getMockReturnWithActualArgs(ctx interface{}, inputArgs []interface{}, envMock *mock.Mock) (retArgs mock.Arguments) {
	_ = "STUB: not implemented"
	return *new(mock.Arguments)
}

func (m *mockWrapper) getMockFn(mockRet mock.Arguments) interface{} {
	_ = "STUB: not implemented"
	return nil
}

// check if mock returns function which must match to the actual function.

// mockDummyActivity is used to register mocks by name

func (m *mockWrapper) getMockValue(mockRet mock.Arguments) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// check if mockRet have same types as function's return types

// we already verified function either has 1 return value (error) or 2 return values (result, error)

// last mock return must be error

// these are supported nil-able types. (reflect.Chan, reflect.Func are nil-able, but not supported)

// this will never happen, panic just in case

func (m *mockWrapper) executeMock(ctx interface{}, input *commonpb.Payloads, mockRet mock.Arguments) (result *commonpb.Payloads, err error) {
	_ = "STUB: not implemented"
	// have to handle panics here to support calling ExecuteChildWorkflow(...).GetChildWorkflowExecution().Get(...)
	// when a child is mocked.
	return nil, nil
}

// check if mock returns function which must match to the actual function.

// we found a mock function that matches to actual function, so call that mockFn

func (env *testWorkflowEnvironmentImpl) newTestActivityTaskHandler(taskQueue string, dataConverter converter.DataConverter) ActivityTaskHandler {
	_ = "STUB: not implemented"
	return *new(ActivityTaskHandler)
}

// activity are bind to specific task queue but not to current task queue

// Special handling for session creation and completion activities.
// If real creation activity is used, it will block timers from autofiring.

func newTestActivityTask(namespace string, attr *commandpb.ScheduleActivityTaskCommandAttributes) *workflowservice.PollActivityTaskQueueResponse {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) newTimer(
	d time.Duration,
	options TimerOptions,
	callback ResultHandler,
	notifyListener bool,
) *TimerID {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) NewTimer(
	d time.Duration,
	options TimerOptions,
	callback ResultHandler,
) *TimerID {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) Now() time.Time {
	_ = "STUB: not implemented"
	return *new(time.Time)
}

func (env *testWorkflowEnvironmentImpl) WorkflowInfo() *WorkflowInfo {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) TypedSearchAttributes() SearchAttributes {
	_ = "STUB: not implemented"
	return *new(SearchAttributes)
}

func (env *testWorkflowEnvironmentImpl) RegisterWorkflow(w interface{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterWorkflowWithOptions(w interface{}, options RegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterDynamicWorkflow(w interface{}, options DynamicRegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterActivity(a interface{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterActivityWithOptions(a interface{}, options RegisterActivityOptions) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterDynamicActivity(w interface{}, options DynamicRegisterActivityOptions) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterNexusService(s *nexus.Service) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterCancelHandler(handler func()) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterSignalHandler(
	handler func(name string, input *commonpb.Payloads, header *commonpb.Header) error,
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterUpdateHandler(
	handler func(name string, id string, input *commonpb.Payloads, header *commonpb.Header, resp UpdateCallbacks),
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RegisterQueryHandler(
	handler func(string, *commonpb.Payloads, *commonpb.Header) (*commonpb.Payloads, error),
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RequestCancelChildWorkflow(_, workflowID string) {
	_ = "STUB: not implemented"
	return
}

// current workflow is a parent workflow, and we are canceling a child workflow

func (env *testWorkflowEnvironmentImpl) RequestCancelExternalWorkflow(namespace, workflowID, runID string, callback ResultHandler) {
	_ = "STUB: not implemented"
	return
}

// The way testWorkflowEnvironment is setup today, we close the child workflow dispatcher before calling
// the workflowCancelHandler. A larger refactor would be needed to handle this similar to non-test code.
// Maybe worth doing when https://github.com/temporalio/go-sdk/issues/50 is tackled.

// current workflow is a parent workflow, and we are canceling a child workflow

// target workflow is not child workflow, we need the mock. The mock needs to be called in a separate goroutinue
// so it can block and wait on the requested delay time (if configured). If we run it in main thread, and the mock
// configured to delay, it will block the main loop which stops the world.

// below call will panic if mock is not properly setup.

func (env *testWorkflowEnvironmentImpl) IsReplaying() bool {
	_ = "STUB: not implemented"
	// this test environment never replay
	return false
}

func (env *testWorkflowEnvironmentImpl) SignalExternalWorkflow(
	namespace string,
	workflowID string,
	runID string,
	signalName string,
	input *commonpb.Payloads,
	arg interface{},
	header *commonpb.Header,
	childWorkflowOnly bool,
	callback ResultHandler,
) {
	_ = "STUB: not implemented"
	// check if target workflow is a known workflow
	return
}

// target workflow is a child

// child already completed (NOTE: we have only one failed cause now)

// resume child workflow since a signal is sent.

// here we signal a child workflow but we cannot find it

// target workflow is not child workflow, we need the mock. The mock needs to be called in a separate goroutinue
// so it can block and wait on the requested delay time (if configured). If we run it in main thread, and the mock
// configured to delay, it will block the main loop which stops the world.

// below call will panic if mock is not properly setup.

func (env *testWorkflowEnvironmentImpl) ExecuteChildWorkflow(params ExecuteWorkflowParams, callback ResultHandler, startedHandler func(r WorkflowExecution, e error)) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) executeChildWorkflowWithDelay(delayStart time.Duration, params ExecuteWorkflowParams, callback ResultHandler, startedHandler func(r WorkflowExecution, e error)) {
	_ = "STUB: not implemented"
	return
}

// childEnv can be nil when WorkflowIDConflictPolicy is USE_EXISTING and there's already a running
// workflow. This is only possible in the test environment for running Nexus handler workflow.

// run child workflow in separate goroutinue

func (env *testWorkflowEnvironmentImpl) newTestNexusTaskHandler(
	opHandle *testNexusOperationHandle,
) *nexusTaskHandler {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) ExecuteNexusOperation(
	params ExecuteNexusOperationParams,
	callback func(*commonpb.Payload, error),
	startedHandler func(opID string, e error),
) int64 {
	_ = "STUB: not implemented"

	// Use lower case header values to simulate how the Nexus SDK (used internally by the "real" server) would transmit
	// these headers over the wire.
	return 0
}

// The real server allows requests to take up to 10 seconds, mimic that behavior here.
// Note that if a user sets the Request-Timeout header, it gets overridden.

// Propagate operation timeout to the handler via header.

// Timer to fail the nexus operation due to schedule to close timeout.

// For async operation, there are two scenarios:
// 1. operation already started: the callback has already been called with the operation id,
//    and calling again is no-op;
// 2. operation didn't start yet: there's no operation id to set.

// Timer to fail the nexus operation due to schedule to start timeout.

// Only timeout if operation hasn't started yet

// No retries for operations, fail the operation immediately.

// Convert to a nexus HandlerError first to simulate the flow in the server.

//lint:ignore SA1019 handle legacy operation error format for backward compatibility.

// To simulate the server flow, convert to failure and then back to a Go error.
// This ensures that the error's `Failure` is set, the same way as it would outside of the test env.

//lint:ignore SA1019 handle legacy operation error format for backward compatibility.

func (env *testWorkflowEnvironmentImpl) RequestCancelNexusOperation(seq int64) {
	_ = "STUB: not implemented"
	return
}

// Avoid duplicate cancelation.

// Mark this cancelation request in case the operation hasn't started yet.
// Cancel will be called after start.

// Only cancel after started, we need an operation ID.

func (env *testWorkflowEnvironmentImpl) RegisterNexusAsyncOperationCompletion(
	service string,
	operation string,
	token string,
	result any,
	err error,
	delay time.Duration,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Getting the locker to prevent race condition if this function is called while
// the test env is already running.

func (env *testWorkflowEnvironmentImpl) getNexusAsyncOperationCompletionHandle(
	service string,
	operation string,
	token string,
) *testNexusAsyncOperationHandle {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) setNexusAsyncOperationCompletionHandle(
	service string,
	operation string,
	token string,
	handle *testNexusAsyncOperationHandle,
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) deleteNexusAsyncOperationCompletionHandle(
	service string,
	operation string,
	token string,
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) scheduleNexusAsyncOperationCompletion(
	handle *testNexusOperationHandle,
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) resolveNexusOperation(seq int64, token string, result *commonpb.Payload, err error) {
	_ = "STUB: not implemented"
	return
}

// Populate the token in case the operation completes before it marked as started.
// startedCallback is idempotent and will be a noop in case the operation has already been marked as started.

func (env *testWorkflowEnvironmentImpl) getNexusOperationHandle(
	seqID int64,
) (*testNexusOperationHandle, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func (env *testWorkflowEnvironmentImpl) setNexusOperationHandle(
	seqID int64,
	handle *testNexusOperationHandle,
) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) deleteNexusOperationHandle(seqID int64) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) makeUniqueNexusOperationToken(
	service string,
	operation string,
	token string,
) string {
	_ = "STUB: not implemented"
	return ""
}

func (env *testWorkflowEnvironmentImpl) SideEffect(f func() (*commonpb.Payloads, error), callback ResultHandler, _ string) {
	_ = "STUB: not implemented"
	return
}

// SideEffect returns a single value, not (value, error)

func (env *testWorkflowEnvironmentImpl) GetVersion(changeID string, minSupported, maxSupported Version) (retVersion Version) {
	_ = "STUB: not implemented"
	return *new(Version)
}

// GetVersion for changeID is mocked

// GetVersion is mocked with any changeID.

// no mock setup, so call regular path

func (env *testWorkflowEnvironmentImpl) getMockedVersion(mockedChangeID, changeID string, minSupported, maxSupported Version) (Version, bool) {
	_ = "STUB: not implemented"
	return *new(Version), false
}

// mock not found

// below call will panic if mock is not properly setup.

// Add context if first param

func getMockMethodForGetVersion(changeID string) string { _ = "STUB: not implemented"; return "" }

func (env *testWorkflowEnvironmentImpl) UpsertSearchAttributes(attributes map[string]interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// mock not found

func validateAndSerializeTypedSearchAttributes(searchAttributes map[SearchAttributeKey]interface{}) (*commonpb.SearchAttributes, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (env *testWorkflowEnvironmentImpl) UpsertTypedSearchAttributes(attributes SearchAttributes) error {
	_ = "STUB: not implemented"
	// Don't immediately return the error from validateAndSerializeTypedSearchAttributes, as we may need to call the mock
	return nil
}

// mock not found

func (env *testWorkflowEnvironmentImpl) UpsertMemo(memoMap map[string]interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// mock not found

func (env *testWorkflowEnvironmentImpl) MutableSideEffect(id string, f func() interface{}, _ func(a, b interface{}) bool, _ string) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

// MutableSideEffect returns a single value, not (value, error)

func (env *testWorkflowEnvironmentImpl) AddSession(sessionInfo *SessionInfo) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) RemoveSession(sessionID string) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) encodeValue(value interface{}) *commonpb.Payloads {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) nextID() int64 { _ = "STUB: not implemented"; return 0 }

func (a *testActivityHandle) getActivityInfo() *ActivityInfo { _ = "STUB: not implemented"; return nil }

func (env *testWorkflowEnvironmentImpl) cancelWorkflow(callback ResultHandler) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) cancelWorkflowByID(workflowID string, runID string, callback ResultHandler) {
	_ = "STUB: not implemented"
	return
}

// RequestCancelWorkflow needs to be run in main thread

func (env *testWorkflowEnvironmentImpl) signalWorkflow(name string, input interface{}, startWorkflowTask bool) {
	_ = "STUB: not implemented"
	return
}

// Do not send any headers on test invocations

func (env *testWorkflowEnvironmentImpl) signalWorkflowByID(workflowID, signalName string, input interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Do not send any headers on test invocations

func (env *testWorkflowEnvironmentImpl) queryWorkflow(queryType string, args ...interface{}) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// Do not send any headers on test invocations

func (env *testWorkflowEnvironmentImpl) updateWorkflow(name string, id string, uc UpdateCallbacks, args ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// check for duplicate update ID

// Do not send any headers on test invocations

func (env *testWorkflowEnvironmentImpl) updateWorkflowByID(workflowID, name, id string, uc UpdateCallbacks, args ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Check for duplicate update ID

func (env *testWorkflowEnvironmentImpl) queryWorkflowByID(workflowID, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// Do not send any headers on test invocations

func (env *testWorkflowEnvironmentImpl) getWorkflowMockRunFn(callWrapper *MockCallWrapper) func(args mock.Arguments) {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) getActivityMockRunFn(callWrapper *MockCallWrapper) func(args mock.Arguments) {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) getNexusOperationMockRunFn(
	callWrapper *MockCallWrapper,
) func(args mock.Arguments) {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) setLastCompletionResult(result interface{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) setLastError(err error) { _ = "STUB: not implemented"; return }

func (env *testWorkflowEnvironmentImpl) setHeartbeatDetails(details interface{}) {
	_ = "STUB: not implemented"
	return
}

func (env *testWorkflowEnvironmentImpl) GetRegistry() *registry {
	_ = "STUB: not implemented"
	return nil
}

func (env *testWorkflowEnvironmentImpl) setStartWorkflowOptions(options StartWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

// Reassign the ID in running Workflows so SignalWorkflowByID can find the workflow

func newTestSessionEnvironment(testWorkflowEnvironment *testWorkflowEnvironmentImpl,
	params *workerExecutionParameters, concurrentSessionExecutionSize int) *testSessionEnvironmentImpl {
	_ = "STUB: not implemented"
	return nil
}

func (t *testSessionEnvironmentImpl) SignalCreationResponse(_ context.Context, sessionID string) error {
	_ = "STUB: not implemented"
	return nil
}

// function signature for mock SignalExternalWorkflow
func mockFnSignalExternalWorkflow(string, string, string, string, interface{}) error {
	_ = "STUB: not implemented"

	// function signature for mock RequestCancelExternalWorkflow
	return nil
}

func mockFnRequestCancelExternalWorkflow(string, string, string) error {
	_ = "STUB: not implemented"

	// function signature for mock GetVersion
	return nil
}

func mockFnGetVersion(string, Version, Version) Version {
	_ = "STUB: not implemented"
	return *

	// function signature for mock SideEffect
	new(Version)
}

func mockFnSideEffect() interface{} {
	_ = "STUB: not implemented"

	// function signature for mock MutableSideEffect
	return nil
}

func mockFnMutableSideEffect(string) interface{} {
	_ = "STUB: not implemented"

	// make sure interface is implemented
	return nil
}

var _ WorkflowEnvironment = (*testWorkflowEnvironmentImpl)(nil)

func (uc updateCallbacksWrapper) Accept() { _ = "STUB: not implemented"; return }

func (uc updateCallbacksWrapper) Reject(err error) { _ = "STUB: not implemented"; return }

func (uc updateCallbacksWrapper) Complete(success interface{}, err error) {
	_ = "STUB: not implemented"
	// cache update result so we can dedup duplicate update IDs
	return
}

func (h *testNexusOperationHandle) newStartTask() *workflowservice.PollNexusTaskQueueResponse {
	_ = "STUB: not implemented"
	return nil
}

// This is effectively ignored.

// The test client uses this to call resolveNexusOperation.

func (h *testNexusOperationHandle) newCancelTask() *workflowservice.PollNexusTaskQueueResponse {
	_ = "STUB: not implemented"
	return nil
}

// completedCallback is a callback registered to handle operation completion.
// Must be called in a postCallback block.
func (h *testNexusOperationHandle) completedCallback(result *commonpb.Payload, err error) {
	_ = "STUB: not implemented"

	// Ignore duplicate completions.
	return
}

// startedCallback is a callback registered to handle operation start.
// Must be called in a postCallback block.
func (h *testNexusOperationHandle) startedCallback(token string, e error) {
	_ = "STUB: not implemented"

	// Ignore duplciate starts.
	return
}

// Start the StartToCloseTimeout timer if configured and operation started successfully

// Only timeout if operation hasn't completed yet

func (h *testNexusOperationHandle) cancel() { _ = "STUB: not implemented"; return }

// No retries in the test env, fail the operation immediately.

// No retries in the test env, fail the operation immediately.

//lint:ignore SA1019 handle legacy operation error format for backward compatibility.

type testNexusHandler struct {
	nexus.UnimplementedHandler

	env      *testWorkflowEnvironmentImpl
	opHandle *testNexusOperationHandle
	handler  nexus.Handler
}

func newTestNexusHandler(
	env *testWorkflowEnvironmentImpl,
	opHandle *testNexusOperationHandle,
) (nexus.Handler, error) {
	_ = "STUB: not implemented"
	return *new(nexus.Handler), nil
}

func (r *testNexusHandler) StartOperation(
	ctx context.Context,
	service string,
	operation string,
	input *nexus.LazyValue,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[any], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// rebuild the input as *nexus.LazyValue

// this should not be possible

// wait until listener returns

// we already verified function has 2 return values (result, error)
// last mock return must be error

// If the result is nexus.HandlerStartOperationResultSync, check the result value type
// matches the operation return type.

func (r *testNexusHandler) CancelOperation(
	ctx context.Context,
	service string,
	operation string,
	token string,
	options nexus.CancelOperationOptions,
) error {
	_ = "STUB: not implemented"
	return nil

	// if the operation was mocked, then there's no workflow running
}

func (env *testWorkflowEnvironmentImpl) registerNexusOperationReference(
	service string,
	opRef testNexusOperationReference,
) {
	_ = "STUB: not implemented"
	return
}

// testNexusOperation implements nexus.RegisterableOperation and serves as dummy
// operation that can be created from a testNexusOperationReference, so that
// mocked Nexus operations can be registered in a Nexus service.
type testNexusOperation struct {
	nexus.UnimplementedOperation[any, any]
	testNexusOperationReference
}

var _ nexus.RegisterableOperation = (*testNexusOperation)(nil)

func (o *testNexusOperation) Name() string { _ = "STUB: not implemented"; return "" }

func (o *testNexusOperation) InputType() reflect.Type {
	_ = "STUB: not implemented"
	return *new(reflect.Type)
}

func (o *testNexusOperation) OutputType() reflect.Type {
	_ = "STUB: not implemented"
	return *new(reflect.Type)
}

func newTestNexusOperation(opRef testNexusOperationReference) *testNexusOperation {
	_ = "STUB: not implemented"
	return nil
}

func (res *updateResult) post_callbacks(env *testWorkflowEnvironmentImpl) {
	_ = "STUB: not implemented"
	return
}
