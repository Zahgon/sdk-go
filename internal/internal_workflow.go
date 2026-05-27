package internal

// All code in this file is private to the package.

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/sdk/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	defaultSignalChannelSize    = 100000 // really large buffering size(100K)
	defaultCoroutineExitTimeout = 100 * time.Millisecond

	panicIllegalAccessCoroutineState = "getState: illegal access from outside of workflow context"
	unhandledUpdateWarningMessage    = "[TMPRL1102] Workflow finished while update handlers are still running. This may have interrupted work that the" +
		" update handler was doing, and the client that sent the update will receive a 'workflow execution" +
		" already completed' RPCError instead of the update result. You can wait for all update" +
		" handlers to complete by using `workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })`. Alternatively, if both you and the clients sending the update" +
		" are okay with interrupting running handlers when the workflow finishes, and causing clients to" +
		" receive errors, then you can disable this warning via UnfinishedPolicy in UpdateHandlerOptions."
)

type (
	syncWorkflowDefinition struct {
		workflow   workflow
		dispatcher dispatcher
		cancel     CancelFunc
		rootCtx    Context
	}

	workflowResult struct {
		workflowResult *commonpb.Payloads
		error          error
	}

	futureImpl struct {
		value   interface{}
		err     error
		ready   bool
		channel *channelImpl
		chained []asyncFuture // Futures that are chained to this one
	}

	// Implements WaitGroup interface
	waitGroupImpl struct {
		n        int      // the number of coroutines to wait on
		waiting  bool     // indicates whether WaitGroup.Wait() has been called yet for the WaitGroup
		future   Future   // future to signal that all awaited members of the WaitGroup have completed
		settable Settable // used to unblock the future when all coroutines have completed
	}

	// Implements Mutex interface
	mutexImpl struct {
		locked bool
	}

	// Implements Semaphore interface
	semaphoreImpl struct {
		size int64
		cur  int64
	}

	// Dispatcher is a container of a set of coroutines.
	dispatcher interface {
		// ExecuteUntilAllBlocked executes coroutines one by one in deterministic order
		// until all of them are completed or blocked on Channel or Selector or timeout is reached.
		ExecuteUntilAllBlocked(deadlockDetectionTimeout time.Duration) (err error)
		// IsDone returns true when all of coroutines are completed
		IsDone() bool
		IsClosed() bool
		IsExecuting() bool
		Close()             // Destroys all coroutines without waiting for their completion
		StackTrace() string // Stack trace of all coroutines owned by the Dispatcher instance

		// NewCoroutine creates a new coroutine. To be called from within another coroutine.
		// Used by the interceptors.
		NewCoroutine(ctx Context, name string, highPriority bool, f func(ctx Context)) Context
	}

	// Workflow is an interface that any workflow should implement.
	// Code of a workflow must be deterministic. It must use workflow.Channel, workflow.Selector, and workflow.Go instead of
	// native channels, select and go. It also must not use range operation over map as it is randomized by go runtime.
	// All time manipulation should use current time returned by GetTime(ctx) method.
	// Note that workflow.Context is used instead of context.Context to avoid use of raw channels.
	workflow interface {
		Execute(ctx Context, input *commonpb.Payloads) (result *commonpb.Payloads, err error)
	}

	sendCallback struct {
		value interface{}
		fn    func() bool // false indicates that callback didn't accept the value
	}

	receiveCallback struct {
		// false result means that callback didn't accept the value and it is still up for delivery
		fn func(v interface{}, more bool) bool
	}

	channelImpl struct {
		name            string                  // human readable channel name
		size            int                     // Channel buffer size. 0 for non buffered.
		buffer          []interface{}           // buffered messages
		blockedSends    []*sendCallback         // puts waiting when buffer is full.
		blockedReceives []*receiveCallback      // receives waiting when no messages are available.
		closed          bool                    // true if channel is closed.
		recValue        *interface{}            // Used only while receiving value, this is used as pre-fetch buffer value from the channel.
		dataConverter   converter.DataConverter // for decode data
		env             WorkflowEnvironment
	}

	// Single case statement of the Select
	selectCase struct {
		channel     *channelImpl                       // Channel of this case.
		receiveFunc *func(c ReceiveChannel, more bool) // function to call when channel has a message. nil for send case.

		sendFunc   *func()         // function to call when channel accepted a message. nil for receive case.
		sendValue  *interface{}    // value to send to the channel. Used only for send case.
		future     asyncFuture     // Used for future case
		futureFunc *func(f Future) // function to call when Future is ready
	}

	// Implements Selector interface
	selectorImpl struct {
		name        string
		cases       []*selectCase // cases that this select is comprised from
		defaultFunc *func()       // default case
	}

	// unblockFunc is passed evaluated by a coroutine yield. When it returns false the yield returns to a caller.
	// stackDepth is the depth of stack from the last blocking call relevant to user.
	// Used to truncate internal stack frames from thread stack.
	unblockFunc func(status string, stackDepth int) (keepBlocked bool)

	coroutineState struct {
		name         string
		dispatcher   *dispatcherImpl  // dispatcher this context belongs to
		aboutToBlock chan bool        // used to notify dispatcher that coroutine that owns this context is about to block
		unblock      chan unblockFunc // used to notify coroutine that it should continue executing.
		keptBlocked  bool             // true indicates that coroutine didn't make any progress since the last yield unblocking
		closed       atomic.Bool      // indicates that owning coroutine has finished execution
		blocked      atomic.Bool
		panicError   error // non nil if coroutine had unhandled panic
	}

	dispatcherImpl struct {
		sequence         int
		channelSequence  int // used to name channels
		selectorSequence int // used to name channels
		coroutines       []*coroutineState
		executing        bool       // currently running ExecuteUntilAllBlocked. Used to avoid recursive calls to it.
		mutex            sync.Mutex // used to synchronize executing
		closed           bool
		interceptor      WorkflowOutboundInterceptor
		logger           log.Logger
		deadlockDetector *deadlockDetector
		readOnly         bool
		// allBlockedCallback is called when all coroutines are blocked,
		// returns true if the callback updated any coroutines state and there may be more work
		allBlockedCallback func() bool
		newEagerCoroutines []*coroutineState
	}

	// WorkflowOptions options passed to the workflow function
	// The current timeout resolution implementation is in seconds and uses math.Ceil() as the duration. But is
	// subjected to change in the future.
	WorkflowOptions struct {
		TaskQueueName            string
		WorkflowExecutionTimeout time.Duration
		WorkflowRunTimeout       time.Duration
		WorkflowTaskTimeout      time.Duration
		Namespace                string
		WorkflowID               string
		WaitForCancellation      bool
		WorkflowIDReusePolicy    enumspb.WorkflowIdReusePolicy
		// WorkflowIDConflictPolicy and OnConflictOptions are only used in test environment for
		// running Nexus operations as child workflow.
		WorkflowIDConflictPolicy enumspb.WorkflowIdConflictPolicy
		OnConflictOptions        *OnConflictOptions
		DataConverter            converter.DataConverter
		RetryPolicy              *commonpb.RetryPolicy
		Priority                 *commonpb.Priority
		CronSchedule             string
		ContextPropagators       []ContextPropagator
		Memo                     map[string]interface{}
		SearchAttributes         map[string]interface{}
		TypedSearchAttributes    SearchAttributes
		ParentClosePolicy        enumspb.ParentClosePolicy
		StaticSummary            string
		StaticDetails            string
		signalChannels           map[string]Channel
		requestedSignalChannels  map[string]*requestedSignalChannel
		queryHandlers            map[string]*queryHandler
		updateHandlers           map[string]*updateHandler
		// runningUpdatesHandles is a map of update handlers that are currently running.
		runningUpdatesHandles     map[string]UpdateInfo
		VersioningIntent          VersioningIntent
		InitialVersioningBehavior ContinueAsNewVersioningBehavior
		// currentDetails is the user-set string returned on metadata query as
		// WorkflowMetadata.current_details
		currentDetails string
	}

	// ExecuteWorkflowParams parameters of the workflow invocation
	ExecuteWorkflowParams struct {
		WorkflowOptions
		WorkflowType         *WorkflowType
		Input                *commonpb.Payloads
		Header               *commonpb.Header
		dataConverter        converter.DataConverter    // context-aware DC from ExecuteChildWorkflow
		failureConverter     converter.FailureConverter // context-aware FC from ExecuteChildWorkflow
		attempt              int32                      // used by test framework to support child workflow retry
		scheduledTime        time.Time                  // used by test framework to support child workflow retry
		lastCompletionResult *commonpb.Payloads         // used by test framework to support cron
	}

	// decodeFutureImpl
	decodeFutureImpl struct {
		*futureImpl
		fn            interface{}
		dataConverter converter.DataConverter // optional: if set, used instead of ctx DC
	}

	childWorkflowFutureImpl struct {
		*decodeFutureImpl             // for child workflow result
		executionFuture   *futureImpl // for child workflow execution future
	}

	nexusOperationFutureImpl struct {
		*decodeFutureImpl             // for the result
		executionFuture   *futureImpl // for the NexusOperationExecution
	}

	asyncFuture interface {
		Future
		// Used by selectorImpl
		// If Future is ready returns its value immediately.
		// If not registers callback which is called when it is ready.
		GetAsync(callback *receiveCallback) (v interface{}, ok bool, err error)

		// Used by selectorImpl
		RemoveReceiveCallback(callback *receiveCallback)

		// This future will added to list of dependency futures.
		ChainFuture(f Future)

		// Gets the current value and error.
		// Make sure this is called once the future is ready.
		GetValueAndError() (v interface{}, err error)

		Set(value interface{}, err error)
	}

	requestedSignalChannel struct {
		options SignalChannelOptions
	}

	queryHandler struct {
		fn            interface{}
		queryType     string
		dataConverter converter.DataConverter
		options       QueryHandlerOptions
	}

	// updateSchedulerImpl adapts the coro dispatcher to the UpdateScheduler interface
	updateSchedulerImpl struct {
		dispatcher dispatcher
	}
)

const (
	workflowEnvironmentContextKey    = "workflowEnv"
	workflowInterceptorContextKey    = "workflowInterceptor"
	localActivityFnContextKey        = "localActivityFn"
	workflowEnvInterceptorContextKey = "envInterceptor"
	workflowResultContextKey         = "workflowResult"
	coroutinesContextKey             = "coroutines"
	workflowEnvOptionsContextKey     = "wfEnvOptions"
	updateInfoContextKey             = "updateInfo"
)

// Assert that structs do indeed implement the interfaces
var _ Channel = (*channelImpl)(nil)
var _ Selector = (*selectorImpl)(nil)
var _ WaitGroup = (*waitGroupImpl)(nil)
var _ dispatcher = (*dispatcherImpl)(nil)

// 1MB buffer to fit combined stack trace of all active goroutines
var stackBuf [1024 * 1024]byte

var (
	errCoroStackNotFound   = errors.New("coroutine stack not found")
	errStackTraceTruncated = errors.New("stack trace truncated: stackBuf is too small")
)

// Pointer to pointer to workflow result
func getWorkflowResultPointerPointer(ctx Context) **workflowResult {
	_ = "STUB: not implemented"
	return nil
}

func getWorkflowEnvironment(ctx Context) WorkflowEnvironment {
	_ = "STUB: not implemented"
	return *new(WorkflowEnvironment)
}

func getWorkflowEnvironmentInterceptor(ctx Context) *workflowEnvironmentInterceptor {
	_ = "STUB: not implemented"
	return nil
}

type workflowEnvironmentInterceptor struct {
	env                 WorkflowEnvironment
	dispatcher          dispatcher
	inboundInterceptor  WorkflowInboundInterceptor
	fn                  interface{}
	outboundInterceptor WorkflowOutboundInterceptor
}

func (wc *workflowEnvironmentInterceptor) Go(ctx Context, name string, f func(ctx Context)) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func getWorkflowOutboundInterceptor(ctx Context) WorkflowOutboundInterceptor {
	_ = "STUB: not implemented"
	return *new(WorkflowOutboundInterceptor)
}

func (f *futureImpl) Get(ctx Context, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// If the value set was a pointer and is the same type as the wanted result,
// instead of panicking because it is not a pointer to a pointer, we will just
// set the pointer

// Used by selectorImpl
// If Future is ready returns its value immediately.
// If not registers callback which is called when it is ready.
func (f *futureImpl) GetAsync(callback *receiveCallback) (v interface{}, ok bool, err error) {
	_ = "STUB: not implemented"
	return nil, false, nil
}

// Future uses Channel.Close to indicate that it is ready.
// So more being true (channel is still open) indicates future is not ready.

// RemoveReceiveCallback removes the callback from future's channel to avoid closure leak.
// Used by selectorImpl
func (f *futureImpl) RemoveReceiveCallback(callback *receiveCallback) {
	_ = "STUB: not implemented"
	return
}

func (f *futureImpl) IsReady() bool { _ = "STUB: not implemented"; return false }

func (f *futureImpl) Set(value interface{}, err error) { _ = "STUB: not implemented"; return }

func (f *futureImpl) SetValue(value interface{}) { _ = "STUB: not implemented"; return }

func (f *futureImpl) SetError(err error) { _ = "STUB: not implemented"; return }

func (f *futureImpl) Chain(future Future) { _ = "STUB: not implemented"; return }

func (f *futureImpl) ChainFuture(future Future) { _ = "STUB: not implemented"; return }

func (f *futureImpl) GetValueAndError() (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (f *childWorkflowFutureImpl) GetChildWorkflowExecution() Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

func (f *childWorkflowFutureImpl) SignalChildWorkflow(ctx Context, signalName string, data interface{}) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// Put header on context before executing

func (f *nexusOperationFutureImpl) GetNexusOperationExecution() Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

func newWorkflowContext(
	env WorkflowEnvironment,
	interceptors []WorkerInterceptor,
) (*workflowEnvironmentInterceptor, Context, error) {
	_ = "STUB: not implemented"
	// Create context with default values
	return nil, *new(Context), nil
}

// Create interceptor and put it on context as inbound and put it on context
// as the default outbound interceptor before init

// Intercept, run init, and put the new outbound interceptor on the context

func (d *syncWorkflowDefinition) Execute(env WorkflowEnvironment, header *commonpb.Header, input *commonpb.Payloads) {
	_ = "STUB: not implemented"
	return
}

// We want to execute the user workflow definition from the first workflow task started,
// so they can see everything before that. Here we would have all initialization done, hence
// we are yielding.

// set the information from the headers that is to be propagated in the workflow context

// It is ok to call this method multiple times.
// it doesn't do anything new, the context remains canceled.

// Put the header on context

// Put the header on context if server supports it

// As a special case, we handle __temporal_workflow_metadata query
// here instead of in workflowExecutionEventHandlerImpl.ProcessQuery
// because we need the context environment to do so.

// Use raw value built from default converter because we don't want to use
// user-conversion

// A handler must be present since it is needed for argument decoding,
// even if the interceptor intercepts query handling

// Decode the arguments

// Invoke

// Encode the result

func (d *syncWorkflowDefinition) OnWorkflowTaskStarted(deadlockDetectionTimeout time.Duration) {
	_ = "STUB: not implemented"
	return
}

func (d *syncWorkflowDefinition) StackTrace() string { _ = "STUB: not implemented"; return "" }

func (d *syncWorkflowDefinition) Close() { _ = "STUB: not implemented"; return }

// NewDispatcher creates a new Dispatcher instance with a root coroutine function.
// Context passed to the root function is child of the passed rootCtx.
// This way rootCtx can be used to pass values to the coroutine code.
func newDispatcher(rootCtx Context, interceptor *workflowEnvironmentInterceptor, root func(ctx Context), allBlockedCallback func() bool) (*dispatcherImpl, Context) {
	_ = "STUB: not implemented"
	return nil, *new(Context)
}

// executeDispatcher executed coroutines in the calling thread and calls workflow completion callbacks
// if root workflow function returned
func executeDispatcher(ctx Context, dispatcher dispatcher, timeout time.Duration) {
	_ = "STUB: not implemented"
	return
}

// Result is not set, so workflow is still executing

// Warn if there are any update handlers still running

// Verify that the workflow did not fail. If it did we will not warn about unhandled updates.

// For troubleshooting stack pretty printing only.
// Set to true to see full stack trace that includes framework methods.
const disableCleanStackTraces = false

func getState(ctx Context) *coroutineState { _ = "STUB: not implemented"; return nil }

func assertNotInReadOnlyState(ctx Context) { _ = "STUB: not implemented"; return }

// use the dispatcher state instead of the coroutine state because contexts can be
// shared

func assertNotInReadOnlyStateCancellation(ctx Context) { _ = "STUB: not implemented"; return }

// For cancellation the dispatcher may not be running because workflow cancellation
// is sent outside of the dispatchers loop.

// use the dispatcher state instead of the coroutine state because contexts can be
// shared

func getStateIfRunning(ctx Context) *coroutineState { _ = "STUB: not implemented"; return nil }

func (c *channelImpl) Name() string { _ = "STUB: not implemented"; return "" }

func (c *channelImpl) CanReceiveWithoutBlocking() bool { _ = "STUB: not implemented"; return false }

func (c *channelImpl) CanSendWithoutBlocking() bool { _ = "STUB: not implemented"; return false }

func (c *channelImpl) Receive(ctx Context, valuePtr interface{}) (more bool) {
	_ = "STUB: not implemented"
	return false
}

// channel closed and empty

// corrupt signal. Drop and reset process

// Corrupt signal. Drop and reset process.

func (c *channelImpl) ReceiveWithTimeout(ctx Context, timeout time.Duration, valuePtr interface{}) (ok, more bool) {
	_ = "STUB: not implemented"
	return false, false
}

// context canceled

// timed out

func (c *channelImpl) ReceiveAsync(valuePtr interface{}) (ok bool) {
	_ = "STUB: not implemented"
	return false
}

func (c *channelImpl) ReceiveAsyncWithMoreFlag(valuePtr interface{}) (ok bool, more bool) {
	_ = "STUB: not implemented"
	return false, false
}

// channel closed and empty

// keep consuming until a good signal is hit or channel is drained

func (c *channelImpl) Len() int { _ = "STUB: not implemented"; return 0 }

// ok = true means that value was received
// more = true means that channel is not closed and more deliveries are possible
func (c *channelImpl) receiveAsyncImpl(callback *receiveCallback) (v interface{}, ok bool, more bool) {
	_ = "STUB: not implemented"
	return nil, false, false
}

// Move blocked sends into buffer

func (c *channelImpl) removeReceiveCallback(callback *receiveCallback) {
	_ = "STUB: not implemented"
	return
}

func (c *channelImpl) removeSendCallback(callback *sendCallback) { _ = "STUB: not implemented"; return }

func (c *channelImpl) Send(ctx Context, v interface{}) { _ = "STUB: not implemented"; return }

// Check for closed in the loop as close can be called when send is blocked

func (c *channelImpl) SendAsync(v interface{}) (ok bool) { _ = "STUB: not implemented"; return false }

func (c *channelImpl) sendAsyncImpl(v interface{}, pair *sendCallback) (ok bool) {
	_ = "STUB: not implemented"
	return false
}

// false from callback indicates that value wasn't consumed

func (c *channelImpl) Close() {
	_ = "STUB: not implemented"

	// Use a copy of blockedReceives for iteration as invoking callback could result in modification
	return
}

// All blocked sends are going to panic

// Takes a value and assigns that 'to' value. logs a metric if it is unable to deserialize
func (c *channelImpl) assignValue(from interface{}, to interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// add to metrics

// initialYield is called at the beginning of coroutine execution.
// stackDepth is the depth of the top of the stack to omit when a stack trace is generated,
// to hide frames internal to the framework.
func (s *coroutineState) initialYield(stackDepth int, status string) {
	_ = "STUB: not implemented"
	return
}

// isPanicking reports whether the current goroutine is executing during panic unwinding. It checks
// for runtime.gopanic on the call stack via runtime.Callers().
func isPanicking() bool { _ = "STUB: not implemented"; return false }

// yield indicates that coroutine cannot make progress and should sleep
// this call blocks
func (s *coroutineState) yield(status string) {
	_ = "STUB: not implemented"

	// Unfortunately we lose the real panic message here, but the stack trace will still contain
	// the right lines.
	return
}

// omit three levels of stack. To adjust change to 0 and count the lines to remove.

func getStackTrace(coroutineName, status string, stackDepth int) string {
	_ = "STUB: not implemented"
	return ""
}

// Omit top stackDepth frames + top status line.
// Omit bottom two frames which is wrapping of coroutine in a goroutine.

func getStackTraceRaw(top string, omitTop, omitBottom int) string {
	_ = "STUB: not implemented"
	return ""
}

func filterStackTrace(stack string, omitTop, omitBottom int) string {
	_ = "STUB: not implemented"
	return ""
}

// If the start is after the end, the depth was invalid originally so return
// the entire raw stack

func getCoroStackTrace(crt *coroutineState, status string, stackDepth int) (string, error) {
	_ = "STUB: not implemented"
	// Can't dump goroutines selectively :(
	// Instead, we identify a coroutine's stack trace by the *coroutineState pointer address
	// in its function arguments. To avoid false positives, we also match on the fixed
	// member function name.
	return "", nil
}

// NOTE: This could happen if coroutineState is moved between runtime.Stack(...)
// and formatting needle. However, Go's GC is currently non-moving.

// coroStack spans from the stackDelim before idx to the stackDelim after idx

// skip over delimiter

// Omit top stackDepth frames + top status line.
// Omit bottom two frames which is wrapping of coroutine in a goroutine.

// unblocked is called by coroutine to indicate that since the last time yield was unblocked channel or select
// where unblocked versus calling yield again after checking their condition
func (s *coroutineState) unblocked() { _ = "STUB: not implemented"; return }

func (s *coroutineState) call(timeout time.Duration) { _ = "STUB: not implemented"; return }

// unblock

// Defaults are populated in the worker options during worker startup, but test environment
// may have no default value for the deadlock detection timeout, so we also need to set it here for
// backwards compatibility.

// Use workflowPanicError since this used to call panic(msg)

func (s *coroutineState) close() { _ = "STUB: not implemented"; return }

// exit tries to run Goexit on the coroutine and wait for it to exit
// within timeout. If it doesn't exit within timeout, it will log a warning.
func (s *coroutineState) exit(logger log.Logger, warnTimeout time.Duration) {
	_ = "STUB: not implemented"
	return
}

// We need to make sure the coroutine is closed, otherwise we risk concurrent coroutines running
// at the same time causing a race condition.

func (s *coroutineState) stackTrace() string { _ = "STUB: not implemented"; return "" }

func (s *coroutineState) run(ctx Context, f func(ctx Context)) { _ = "STUB: not implemented"; return }

// keep receiver argument alive for getCoroStackTrace

func (d *dispatcherImpl) NewCoroutine(ctx Context, name string, highPriority bool, f func(ctx Context)) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func (d *dispatcherImpl) newState(name string, highPriority bool) *coroutineState {
	_ = "STUB: not implemented"
	return nil
}

// Update requests need to be added to the front of the dispatchers coroutine list so they
// are handled before the root coroutine.

func (d *dispatcherImpl) IsClosed() bool { _ = "STUB: not implemented"; return false }

func (d *dispatcherImpl) ExecuteUntilAllBlocked(deadlockDetectionTimeout time.Duration) (err error) {
	_ = "STUB: not implemented"
	return nil
}

// Keep executing until at least one goroutine made some progress

// Give every coroutine chance to execute removing closed ones

// TODO: Support handling of panic in a coroutine by dispatcher.
// TODO: Dump all outstanding coroutines if one of them panics

// c.call() can close the context so check again

// remove the closed one from the slice

// If any eager coroutines were created by the last coroutine we
// need to schedule them now.

// Set allBlocked to false if new coroutines where created

func (d *dispatcherImpl) IsDone() bool { _ = "STUB: not implemented"; return false }

func (d *dispatcherImpl) IsExecuting() bool { _ = "STUB: not implemented"; return false }

func (d *dispatcherImpl) getIsReadOnly() bool { _ = "STUB: not implemented"; return false }

func (d *dispatcherImpl) setIsReadOnly(readOnly bool) { _ = "STUB: not implemented"; return }

func (d *dispatcherImpl) Close() { _ = "STUB: not implemented"; return }

// We need to exit the coroutines in a separate goroutine because:
// 	* The coroutine may be stuck and won't respond to the exit request.
// 	* On exit the coroutines defers will still run and that may block.

func (d *dispatcherImpl) StackTrace() string { _ = "STUB: not implemented"; return "" }

func (s *selectorImpl) AddReceive(c ReceiveChannel, f func(c ReceiveChannel, more bool)) Selector {
	_ = "STUB: not implemented"
	return *new(Selector)
}

func (s *selectorImpl) AddSend(c SendChannel, v interface{}, f func()) Selector {
	_ = "STUB: not implemented"
	return *new(Selector)
}

func (s *selectorImpl) AddFuture(future Future, f func(future Future)) Selector {
	_ = "STUB: not implemented"
	return *new(Selector)
}

func (s *selectorImpl) AddDefault(f func()) { _ = "STUB: not implemented"; return }

func (s *selectorImpl) HasPending() bool { _ = "STUB: not implemented"; return false }

func (s *selectorImpl) Select(ctx Context) { _ = "STUB: not implemented"; return }

// Pre-store c.recValue to prevent signal loss when AddDefault
// blocks. Without channelLostMsgFlag, always pre-store (original
// #1624 fix). With channelLostMsgFlag, only pre-store when a
// default branch exists to avoid overwriting c.recValue when
// multiple selectors are blocked on the same channel.

// Select() returns in this case/branch. The callback won't be called for this case. However, callback
// will be called for previous cases/branches. We should set readyBranch so that when other case/branch
// become ready they won't consume the value for this Select() call.

// Avoid assigning pointer to nil interface which makes
// c.RecValue != nil and breaks the nil check at the beginning of receiveAsyncImpl

// callback closure is added to channel's blockedReceives, we need to clean it up to avoid closure leak

// Select() returns in this case/branch. The callback won't be called for this case. However, callback
// will be called for previous cases/branches. We should set readyBranch so that when other case/branch
// become ready they won't consume the value for this Select() call.

// callback closure is added to channel's blockedSends, we need to clean it up to avoid closure leak

// Select() returns in this case/branch. The callback won't be called for this case. However, callback
// will be called for previous cases/branches. We should set readyBranch so that when other case/branch
// become ready they won't consume the value for this Select() call.

// callback closure is added to future's channel's blockedReceives, need to clean up to avoid leak

// NewWorkflowDefinition creates a WorkflowDefinition from a Workflow
func newSyncWorkflowDefinition(workflow workflow) *syncWorkflowDefinition {
	_ = "STUB: not implemented"
	return nil
}

func getValidatedWorkflowFunction(workflowFunc interface{}, args []interface{}, dataConverter converter.DataConverter, r *registry) (*WorkflowType, *commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func getWorkflowEnvOptions(ctx Context) *WorkflowOptions { _ = "STUB: not implemented"; return nil }

func setWorkflowEnvOptionsIfNotExist(ctx Context) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func getDataConverterFromWorkflowContext(ctx Context) converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

func getRegistryFromWorkflowContext(ctx Context) *registry { _ = "STUB: not implemented"; return nil }

// getSignalChannel finds the associated channel for the signal.
func (w *WorkflowOptions) getSignalChannel(ctx Context, signalName string) ReceiveChannel {
	_ = "STUB: not implemented"
	return *new(ReceiveChannel)
}

// GetUnhandledSignalNames returns signal names that have unconsumed signals.
func GetUnhandledSignalNames(ctx Context) []string { _ = "STUB: not implemented"; return nil }

// GetCurrentDetails gets the previously-set current details.
//
// NOTE: Experimental
func GetCurrentDetails(ctx Context) string { _ = "STUB: not implemented"; return "" }

// SetCurrentDetails sets the current details.
//
// NOTE: Experimental
func SetCurrentDetails(ctx Context, details string) { _ = "STUB: not implemented"; return }

func getWorkflowMetadata(ctx Context) (*sdk.WorkflowMetadata, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Queries

// Signals

// Updates

// Sort interaction definitions

func sortWorkflowInteractionDefinitions(defns []*sdk.WorkflowInteractionDefinition) {
	_ = "STUB: not implemented"
	return
}

// getUnhandledSignalNames returns signal names that have unconsumed signals.
func (w *WorkflowOptions) getUnhandledSignalNames() []string { _ = "STUB: not implemented"; return nil }

func (w *WorkflowOptions) getRunningUpdateHandles() map[string]UpdateInfo {
	_ = "STUB: not implemented"
	return nil
}

func (d *decodeFutureImpl) Get(ctx Context, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// newDecodeFuture creates a new future as well as associated Settable that is used to set its value.
// fn - the decoded value needs to be validated against a function.
func newDecodeFuture(ctx Context, fn interface{}) (Future, Settable) {
	_ = "STUB: not implemented"
	return *new(Future), *new(Settable)
}

// setQueryHandler sets query handler for given queryType.
func setQueryHandler(ctx Context, queryType string, handler interface{}, options QueryHandlerOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// setUpdateHandler sets update handler for a given update name.
func setUpdateHandler(ctx Context, updateName string, handler interface{}, opts UpdateHandlerOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// Data and Failure converter wrapped with WorkflowSerializationContext in newWorkflowExecutionEventHandler.

// validateEquivalentParams verifies that both arguments are functions and that
// said functions take the exact same parameter types in the same order but not
// considering the presence or absence of a workflow.Context parameter in the
// zeroth position.
func validateEquivalentParams(fn1, fn2 interface{}) error { _ = "STUB: not implemented"; return nil }

// ignore the presence of a workflow.Context as a first param

func validateQueryHandlerFn(fn interface{}) error { _ = "STUB: not implemented"; return nil }

func (h *queryHandler) execute(input []interface{}) (result interface{}, err error) {
	_ = "STUB: not implemented"
	// if query handler panic, convert it to error
	return nil, nil
}

// query handler code try to access workflow functions outside of workflow context, make error message
// more descriptive and clear.

// Add adds delta, which may be negative, to the WaitGroup counter.
// If the counter becomes zero, all goroutines blocked on Wait are released.
// If the counter goes negative, Add panics.
//
// Note that calls with a positive delta that occur when the counter is zero
// must happen before a Wait. Calls with a negative delta, or calls with a
// positive delta that start when the counter is greater than zero, may happen
// at any time.
// Typically this means the calls to Add should execute before the statement
// creating the goroutine or other event to be waited for.
// If a WaitGroup is reused to wait for several independent sets of events,
// new Add calls must happen after all previous Wait calls have returned.
//
// param delta int -> the value to increment the WaitGroup counter by
func (wg *waitGroupImpl) Add(delta int) { _ = "STUB: not implemented"; return }

// Done decrements the WaitGroup counter by 1, indicating
// that a coroutine in the WaitGroup has completed
func (wg *waitGroupImpl) Done() {
	_ = "STUB: not implemented"

	// Wait blocks and waits for specified number of coroutines to
	// finish executing and then unblocks once the counter has reached 0.
	//
	// param ctx Context -> workflow context
	return
}

func (wg *waitGroupImpl) Wait(ctx Context) { _ = "STUB: not implemented"; return }

func (wg *waitGroupImpl) Go(ctx Context, f func(Context)) { _ = "STUB: not implemented"; return }

// Spawn starts a new coroutine with Dispatcher.NewCoroutine
func (us updateSchedulerImpl) Spawn(ctx Context, name string, highPriority bool, f func(Context)) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// Yield calls the yield function on the coroutineState associated with the
// supplied workflow context.
func (us updateSchedulerImpl) Yield(ctx Context, reason string) { _ = "STUB: not implemented"; return }

func (m *mutexImpl) Lock(ctx Context) error { _ = "STUB: not implemented"; return nil }

func (m *mutexImpl) TryLock(ctx Context) bool { _ = "STUB: not implemented"; return false }

func (m *mutexImpl) Unlock() { _ = "STUB: not implemented"; return }

func (m *mutexImpl) IsLocked() bool { _ = "STUB: not implemented"; return false }

func (s *semaphoreImpl) Acquire(ctx Context, n int64) error { _ = "STUB: not implemented"; return nil }

func (s *semaphoreImpl) TryAcquire(ctx Context, n int64) bool {
	_ = "STUB: not implemented"
	return false
}

func (s *semaphoreImpl) Release(n int64) { _ = "STUB: not implemented"; return }

func incrementWorkflowTaskFailureCounter(metricsHandler metrics.Handler, failureReason string) {
	_ = "STUB: not implemented"
	return
}
