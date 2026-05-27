package internal

// All code in this file is private to the package.

import (
	"context"
	"io"
	"math"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

const (
	// Set to 2 pollers for now, can adjust later if needed. The typical RTT (round-trip time) is below 1ms within data
	// center. And the poll API latency is about 5ms. With 2 poller, we could achieve around 300~400 RPS.
	defaultConcurrentPollRoutineSize = 2

	defaultAutoscalingInitialNumberOfPollers = 5   // Default initial number of pollers when using autoscaling.
	defaultAutoscalingMinimumNumberOfPollers = 1   // Default minimum number of pollers when using autoscaling.
	defaultAutoscalingMaximumNumberOfPollers = 100 // Default maximum number of pollers when using autoscaling.

	defaultMaxConcurrentActivityExecutionSize = 1000   // Large concurrent activity execution size (1k)
	defaultWorkerActivitiesPerSecond          = 100000 // Large activity executions/sec (unlimited)

	defaultMaxConcurrentLocalActivityExecutionSize = 1000   // Large concurrent activity execution size (1k)
	defaultWorkerLocalActivitiesPerSecond          = 100000 // Large activity executions/sec (unlimited)

	defaultTaskQueueActivitiesPerSecond = 100000.0 // Large activity executions/sec (unlimited)

	defaultMaxConcurrentTaskExecutionSize = 1000   // hardcoded max task execution size.
	defaultWorkerTaskExecutionRate        = 100000 // Large task execution rate (unlimited)

	defaultPollerRate = 1000

	defaultMaxConcurrentSessionExecutionSize = 1000 // Large concurrent session execution size (1k)

	defaultDeadlockDetectionTimeout = time.Second // By default kill workflow tasks that are running more than 1 sec.
	// Unlimited deadlock detection timeout is used when we want to allow workflow tasks to run indefinitely, such
	// as during debugging.
	unlimitedDeadlockDetectionTimeout = math.MaxInt64

	testTagsContextKey = "temporal-testTags"
)

type (
	// WorkflowWorker wraps the code for hosting workflow types.
	// And worker is mapped 1:1 with task queue. If the user want's to poll multiple
	// task queue names they might have to manage 'n' workers for 'n' task queues.
	workflowWorker struct {
		executionParameters workerExecutionParameters
		workflowService     workflowservice.WorkflowServiceClient
		worker              *baseWorker
		localActivityWorker *baseWorker
		identity            string
		stopC               chan struct{}
		localActivityStopC  chan struct{}
		stickyUUID          string // Used for ShutdownWorker call
	}

	// ActivityWorker wraps the code for hosting activity types.
	// TODO: Worker doing heartbeating automatically while activity task is running
	activityWorker struct {
		executionParameters workerExecutionParameters
		workflowService     workflowservice.WorkflowServiceClient
		poller              taskPoller
		worker              *baseWorker
		identity            string
		stopC               chan struct{}
	}

	// sessionWorker wraps the code for hosting session creation, completion and
	// activities within a session. The creationWorker polls from a global taskqueue,
	// while the activityWorker polls from a resource specific taskqueue.
	sessionWorker struct {
		creationWorker *activityWorker
		activityWorker *activityWorker
	}

	// Worker overrides.
	workerOverrides struct {
		workflowTaskHandler WorkflowTaskHandler
		activityTaskHandler ActivityTaskHandler
		slotSupplier        SlotSupplier
	}

	// workerExecutionParameters defines worker configure/execution options.
	workerExecutionParameters struct {
		// Namespace name.
		Namespace string

		// Task queue name to poll.
		TaskQueue string

		// The tuner for the worker.
		Tuner WorkerTuner

		// Defines rate limiting on number of activity tasks that can be executed per second per worker.
		WorkerActivitiesPerSecond float64

		// Defines rate limiting on number of local activities that can be executed per second per worker.
		WorkerLocalActivitiesPerSecond float64

		// TaskQueueActivitiesPerSecond is the throttling limit for activity tasks controlled by the server.
		TaskQueueActivitiesPerSecond float64

		// User can provide an identity for the debuggability. If not provided the framework has
		// a default option.
		Identity string

		// The worker's build ID used for versioning, if one was set.
		//
		// Deprecated: use DeploymentOptions.Version for versioning instead.
		WorkerBuildID string

		// If true the worker is opting in to build ID based versioning.
		//
		// Deprecated: use DeploymentOptions.UseVersioning for versioning instead.
		UseBuildIDForVersioning bool

		// Worker deployment options containing all deployment versioning configuration.
		DeploymentOptions WorkerDeploymentOptions

		MetricsHandler metrics.Handler

		Logger log.Logger

		// Enable logging in replay mode
		EnableLoggingInReplay bool

		// Context to store user provided key/value pairs
		BackgroundContext context.Context

		// Context cancel function to cancel user context
		BackgroundContextCancel context.CancelCauseFunc

		StickyScheduleToStartTimeout time.Duration

		// WorkflowPanicPolicy is used for configuring how client's workflow task handler deals with workflow
		// code panicking which includes non backwards compatible changes to the workflow code without appropriate
		// versioning (see workflow.GetVersion).
		// The default behavior is to block workflow execution until the problem is fixed.
		WorkflowPanicPolicy WorkflowPanicPolicy

		DataConverter converter.DataConverter

		FailureConverter converter.FailureConverter

		// WorkerStopTimeout is the time delay before hard terminate worker
		WorkerStopTimeout time.Duration

		// WorkerStopChannel is a read only channel listen on worker close. The worker will close the channel before exit.
		WorkerStopChannel <-chan struct{}

		// WorkerFatalErrorCallback is a callback for fatal errors that should stop
		// the worker.
		WorkerFatalErrorCallback func(error)

		// SessionResourceID is a unique identifier of the resource the session will consume
		SessionResourceID string

		ContextPropagators []ContextPropagator

		// DeadlockDetectionTimeout specifies workflow task timeout.
		DeadlockDetectionTimeout time.Duration

		DefaultHeartbeatThrottleInterval time.Duration

		MaxHeartbeatThrottleInterval time.Duration

		// WorkflowTaskPollerBehavior defines the behavior of the workflow task poller.
		WorkflowTaskPollerBehavior PollerBehavior

		// ActivityTaskPollerBehavior defines the behavior of the activity task poller.
		ActivityTaskPollerBehavior PollerBehavior

		// NexusTaskPollerBehavior defines the behavior of the nexus task poller.
		NexusTaskPollerBehavior PollerBehavior

		// Pointer to the shared worker cache
		cache *WorkerCache

		eagerActivityExecutor *eagerActivityExecutor

		capabilities *workflowservice.GetSystemInfoResponse_Capabilities

		pollTimeTracker *pollTimeTracker

		workerInstanceKey string

		workerPollCompleteOnShutdown *atomic.Bool

		// Set to true during start() when the namespace has the poller_autoscaling capability.
		serverSupportsAutoscaling *atomic.Bool

		inboundPayloadVisitor PayloadVisitor

		outboundPayloadVisitor PayloadVisitor

		payloadVisitorConcurrency int

		setErrorLimits func(*payloadLimits)
	}

	// HistoryJSONOptions are options for HistoryFromJSON.
	HistoryJSONOptions struct {
		// LastEventID, if set, will only load history up to this ID (inclusive).
		LastEventID int64
	}

	// Represents the version of a specific worker deployment.
	//
	// Exposed as: [go.temporal.io/sdk/worker.WorkerDeploymentVersion]
	WorkerDeploymentVersion struct {
		// The name of the deployment this worker version belongs to
		DeploymentName string
		// The build id specific to this worker
		BuildID string
	}
)

var debugMode = os.Getenv("TEMPORAL_DEBUG") != ""

// newWorkflowWorker returns an instance of the workflow worker.
func newWorkflowWorker(client *WorkflowClient, params workerExecutionParameters, ppMgr pressurePointMgr, registry *registry) *workflowWorker {
	_ = "STUB: not implemented"
	return nil
}

func ensureRequiredParams(params *workerExecutionParameters) { _ = "STUB: not implemented"; return }

// create default logger if user does not supply one (should happen in tests only).

// Err cannot happen since these slot numbers are guaranteed valid

// getBuildID returns either the user-defined build ID if it was provided, or an autogenerated one
// using getBinaryChecksum
func (params *workerExecutionParameters) getBuildID() string { _ = "STUB: not implemented"; return "" }

// Returns true if this worker is part of our system namespace or per-namespace system task queue
func (params *workerExecutionParameters) isInternalWorker() bool {
	_ = "STUB: not implemented"
	return false
}

func newWorkflowWorkerInternal(client *WorkflowClient, params workerExecutionParameters, ppMgr pressurePointMgr, overrides *workerOverrides, registry *registry) *workflowWorker {
	_ = "STUB: not implemented"
	return nil
}

// Get a workflow task handler.

func newWorkflowTaskWorkerInternal(
	taskHandler WorkflowTaskHandler,
	contextManager WorkflowContextManager,
	client *WorkflowClient,
	params workerExecutionParameters,
	stopC chan struct{},
	interceptors []WorkerInterceptor,
) *workflowWorker {
	_ = "STUB: not implemented"
	return nil
}

// Generate stickyUUID here so it can be stored in workflowWorker for ShutdownWorker call

// We want a separate stop channel for local activities because when a worker shuts down,
// we need to allow pending local activities to finish running for that workflow task.
// After all pending local activities are handled, we then close the local activity stop channel.

// laTunnel is the glue that hookup 3 parts

// 1) workflow handler will send local activity task to laTunnel

// 2) local activity task poller will poll from laTunnel, and result will be pushed to laTunnel

// 3) the result pushed to laTunnel will be sent as task to workflow worker to process.

// Start the worker.
func (ww *workflowWorker) Start() error { _ = "STUB: not implemented"; return nil }

// TODO: propagate error

// Stop the worker.
func (ww *workflowWorker) Stop() {
	_ = "STUB: not implemented"

	// TODO: remove the stop methods in favor of the workerStopChannel
	return
}

func newSessionWorker(client *WorkflowClient, params workerExecutionParameters, env *registry, maxConcurrentSessionExecutionSize int) *sessionWorker {
	_ = "STUB: not implemented"
	// Session workers poll on resource-specific task queues not included in
	// ShutdownWorker, so the server will never cancel their polls. Use the
	// legacy immediate-cancel path instead of graceful shutdown.
	return nil
}

// For now resourceID is hidden from user so we will always create a unique one for each worker.

// For the resource specific task queue, we don't need to include deployment options
// Save them to restore later

// Disable versioning for activity worker within session, but still send deployment name for debug purpose

// Although we have session token bucket to limit session size across creation
// and recreation, we also limit it here for creation only

func (sw *sessionWorker) Start() error { _ = "STUB: not implemented"; return nil }

func (sw *sessionWorker) Stop() { _ = "STUB: not implemented"; return }

func newActivityWorker(
	client *WorkflowClient,
	params workerExecutionParameters,
	overrides *workerOverrides,
	env *registry,
	sessionTokenBucket *sessionTokenBucket,
) *activityWorker {
	_ = "STUB: not implemented"
	return nil
}

// Get a activity task handler.

// Start the worker.
func (aw *activityWorker) Start() error { _ = "STUB: not implemented"; return nil }

// TODO: propagate errors

// Stop the worker.
func (aw *activityWorker) Stop() { _ = "STUB: not implemented"; return }

type registry struct {
	sync.Mutex
	nexusServices                 map[string]*nexus.Service
	workflowFuncMap               map[string]interface{}
	workflowAliasMap              map[string]string
	workflowVersioningBehaviorMap map[string]VersioningBehavior
	activityFuncMap               map[string]activity
	activityAliasMap              map[string]string
	dynamicWorkflow               interface{}
	dynamicWorkflowOptions        DynamicRegisterWorkflowOptions
	dynamicActivity               activity
	_                             DynamicRegisterActivityOptions
	interceptors                  []WorkerInterceptor
}

type registryOptions struct {
	disableAliasing bool
}

func (r *registry) RegisterWorkflow(af interface{}) { _ = "STUB: not implemented"; return }

func (r *registry) RegisterWorkflowWithOptions(
	wf interface{},
	options RegisterWorkflowOptions,
) {
	_ = "STUB: not implemented"
	// Support direct registration of WorkflowDefinition
	return
}

// Validate that it is a function

func (r *registry) RegisterDynamicWorkflow(wf interface{}, options DynamicRegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

// Support direct registration of WorkflowDefinition

// Validate that it is a function

func (r *registry) RegisterActivity(af interface{}) { _ = "STUB: not implemented"; return }

func (r *registry) RegisterActivityWithOptions(
	af interface{},
	options RegisterActivityOptions,
) {
	_ = "STUB: not implemented"
	// Support direct registration of activity
	return
}

// Validate that it is a function

func (r *registry) registerActivityStructWithOptions(aStruct interface{}, options RegisterActivityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// skip private method

func (r *registry) RegisterDynamicActivity(af interface{}, options DynamicRegisterActivityOptions) {
	_ = "STUB: not implemented"
	return
}

// Support direct registration of activity

// Validate that it is a function

func (r *registry) RegisterNexusService(service *nexus.Service) { _ = "STUB: not implemented"; return }

func (r *registry) getWorkflowAlias(fnName string) (string, bool) {
	_ = "STUB: not implemented"
	return "", false
}

func (r *registry) getWorkflowFn(fnName string) (interface{}, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func (r *registry) getRegisteredWorkflowTypes() []string { _ = "STUB: not implemented"; return nil }

func (r *registry) getActivityAlias(fnName string) (string, bool) {
	_ = "STUB: not implemented"
	return "", false
}

func (r *registry) addActivityWithLock(fnName string, a activity) {
	_ = "STUB: not implemented"
	return
}

func (r *registry) GetActivity(fnName string) (activity, bool) {
	_ = "STUB: not implemented"
	return *new(activity), false
}

func (r *registry) getActivityNoLock(fnName string) (activity, bool) {
	_ = "STUB: not implemented"
	return *new(activity), false
}

func (r *registry) getRegisteredActivities() []activity { _ = "STUB: not implemented"; return nil }

func (r *registry) getRegisteredActivityTypes() []string { _ = "STUB: not implemented"; return nil }

func (r *registry) getWorkflowDefinition(wt WorkflowType) (WorkflowDefinition, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowDefinition), nil
}

func (r *registry) getWorkflowVersioningBehavior(wt WorkflowType) (VersioningBehavior, bool) {
	_ = "STUB: not implemented"
	return *new(VersioningBehavior), false
}

func (r *registry) getNexusService(service string) *nexus.Service {
	_ = "STUB: not implemented"
	return nil
}

func (r *registry) getRegisteredNexusServices() []*nexus.Service {
	_ = "STUB: not implemented"
	return nil
}

// Validate function parameters.
func validateFnFormat(fnType reflect.Type, isWorkflow, isDynamic bool) error {
	_ = "STUB: not implemented"
	return nil
}

// For activities, check that workflow context is not accidentally provided
// Activities registered with structs will have their receiver as the first argument so confirm it is not
// in the first two arguments

// Return values
// We expect either
// 	<result>, error
//	(or) just error

func newRegistry() *registry { _ = "STUB: not implemented"; return nil }

func newRegistryWithOptions(options registryOptions) *registry {
	_ = "STUB: not implemented"
	return nil
}

// Wrapper to execute workflow functions.
type workflowExecutor struct {
	workflowType string
	fn           interface{}
	interceptors []WorkerInterceptor
	dynamic      bool
}

func (we *workflowExecutor) Execute(ctx Context, input *commonpb.Payloads) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Dynamic workflows take in a single EncodedValues, encode all data into single EncodedValues

// Execute and serialize result

// Wrapper to execute activity functions.
type activityExecutor struct {
	name             string
	fn               interface{}
	skipInterceptors bool
	dynamic          bool
}

func (ae *activityExecutor) ActivityType() ActivityType {
	_ = "STUB: not implemented"
	return *new(ActivityType)
}

func (ae *activityExecutor) GetFunction() interface{} { _ = "STUB: not implemented"; return nil }

func (ae *activityExecutor) Execute(ctx context.Context, input *commonpb.Payloads) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Dynamic activities take in a single EncodedValues, encode all data into single EncodedValues

func (ae *activityExecutor) ExecuteWithActualArgs(ctx context.Context, args []interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Execute and serialize result

// As a special case, if the result is already a payload, just use it

func getDataConverterFromActivityCtx(ctx context.Context) converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

func getActivityEnvironmentFromCtx(ctx context.Context) *activityEnvironment {
	_ = "STUB: not implemented"
	return nil
}

// AggregatedWorker combines management of both workflowWorker and activityWorker worker lifecycle.
type AggregatedWorker struct {
	// Stored for creating a nexus worker on Start.
	executionParams workerExecutionParameters
	// Memoized start function. Ensures start runs once and returns the same error when called multiple times.
	memoizedStart func() error

	client         *WorkflowClient
	workflowWorker *workflowWorker
	activityWorker *activityWorker
	sessionWorker  *sessionWorker
	nexusWorker    *nexusWorker
	logger         log.Logger
	registry       *registry
	// Stores a boolean indicating whether the worker has already been started.
	started      atomic.Bool
	shuttingDown atomic.Bool
	stopC        chan struct{}
	fatalErr     error
	fatalErrLock sync.Mutex
	capabilities *workflowservice.GetSystemInfoResponse_Capabilities

	workerInstanceKey     string
	plugins               []WorkerPlugin
	pluginRegistryOptions *WorkerPluginConfigureWorkerRegistryOptions // Never nil

	heartbeatMetrics             *heartbeatMetricsHandler
	heartbeatCallback            func() *workerpb.WorkerHeartbeat
	workerPollCompleteOnShutdown *atomic.Bool
}

// RegisterWorkflow registers workflow implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterWorkflow(w interface{}) { _ = "STUB: not implemented"; return }

// RegisterWorkflowWithOptions registers workflow implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterWorkflowWithOptions(w interface{}, options RegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

// RegisterDynamicWorkflow registers dynamic workflow implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterDynamicWorkflow(w interface{}, options DynamicRegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

// RegisterActivity registers activity implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterActivity(a interface{}) { _ = "STUB: not implemented"; return }

// RegisterActivityWithOptions registers activity implementation with the AggregatedWorker
func (aw *AggregatedWorker) RegisterActivityWithOptions(a interface{}, options RegisterActivityOptions) {
	_ = "STUB: not implemented"
	return
}

// RegisterDynamicActivity registers the dynamic activity function with options.
// Registering activities via a structure is not supported for dynamic activities.
func (aw *AggregatedWorker) RegisterDynamicActivity(a interface{}, options DynamicRegisterActivityOptions) {
	_ = "STUB: not implemented"
	return
}

func (aw *AggregatedWorker) RegisterNexusService(service *nexus.Service) {
	_ = "STUB: not implemented"
	return
}

// Start the worker in a non-blocking fashion.
// The actual work is done in the memoized "start" function to ensure duplicate calls are returned a consistent error.
func (aw *AggregatedWorker) Start() error { _ = "STUB: not implemented"; return nil }

// start the worker. This method is memoized using sync.OnceValue in memoizedStart.
func (aw *AggregatedWorker) start() error { _ = "STUB: not implemented"; return nil }

// Populate the capabilities. This should be the only time it is written too.

// stop workflow worker.

// stop workflow worker and activity worker.

func (aw *AggregatedWorker) assertNotStopped() { _ = "STUB: not implemented"; return }

var (
	binaryChecksum     string
	binaryChecksumLock sync.Mutex
)

// SetBinaryChecksum sets the identifier of the binary(aka BinaryChecksum).
// The identifier is mainly used in recording reset points when respondWorkflowTaskCompleted. For each workflow, the very first
// workflow task completed by a binary will be associated as a auto-reset point for the binary. So that when a customer wants to
// mark the binary as bad, the workflow will be reset to that point -- which means workflow will forget all progress generated
// by the binary.
// On another hand, once the binary is marked as bad, the bad binary cannot poll workflow queue and make any progress any more.
func SetBinaryChecksum(checksum string) { _ = "STUB: not implemented"; return }

func initBinaryChecksum() error { _ = "STUB: not implemented"; return nil }

func getBinaryChecksum() string { _ = "STUB: not implemented"; return "" }

// Run the worker in a blocking fashion. Stop the worker when interruptCh receives signal.
// Pass worker.InterruptCh() to stop the worker with SIGINT or SIGTERM.
// Pass nil to stop the worker with external Stop() call.
// Pass any other `<-chan interface{}` and Run will wait for signal from that channel.
// Returns error if the worker fails to start or there is a fatal error
// during execution.
func (aw *AggregatedWorker) Run(interruptCh <-chan interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// This may be nil if this wasn't stopped due to fatal error

// Stop the worker.
func (aw *AggregatedWorker) Stop() {
	_ = "STUB: not implemented"
	// Only attempt stop if we haven't attempted before
	return
}

// Prevent pollers from re-polling before closing stopC. There is a race
// between stopC being closed and the ShutdownWorker RPC: a poll can
// complete naturally (e.g. long-poll timeout) right after stopC fires
// but before ShutdownWorker is sent, causing the poller to loop and
// re-poll.

// Issue stop through plugins

func (aw *AggregatedWorker) registerHeartbeatWorker() error { _ = "STUB: not implemented"; return nil }

func (aw *AggregatedWorker) unregisterHeartbeatWorker() { _ = "STUB: not implemented"; return }

// shutdownWorker sends a ShutdownWorker RPC to notify the server that this worker is shutting down.
// When StickyTaskQueue is non-empty, this is a best-effort attempt to indicate to Matching service
// that this workflow task poller's sticky queue will no longer be polled.
//
// NOTE: errors are logged but don't fail the shutdown.
func (aw *AggregatedWorker) shutdownWorker() { _ = "STUB: not implemented"; return }

// Ignore unimplemented (server doesn't support it)

func (aw *AggregatedWorker) activeTaskQueueTypes() []enumspb.TaskQueueType {
	_ = "STUB: not implemented"
	return nil
}

// WorkflowReplayer is used to replay workflow code from an event history
type WorkflowReplayer struct {
	registry                    *registry
	dataConverter               converter.DataConverter
	failureConverter            converter.FailureConverter
	contextPropagators          []ContextPropagator
	enableLoggingInReplay       bool
	disableDeadlockDetection    bool
	inboundPayloadVisitor       PayloadVisitor
	mu                          sync.Mutex
	workflowExecutionResults    map[string]*commonpb.Payloads
	workflowReplayerInstanceKey string
	plugins                     []WorkerPlugin
	pluginRegistryOptions       *WorkerPluginConfigureWorkflowReplayerRegistryOptions
}

// WorkflowReplayerOptions are options for creating a workflow replayer.
type WorkflowReplayerOptions struct {
	// Optional custom data converter to provide for replay. If not set, the
	// default converter is used.
	DataConverter converter.DataConverter

	FailureConverter converter.FailureConverter

	// Optional: Sets ContextPropagators that allows users to control the context information passed through a workflow
	//
	// default: nil
	ContextPropagators []ContextPropagator

	// Interceptors to apply to the worker. Earlier interceptors wrap later
	// interceptors.
	Interceptors []WorkerInterceptor

	// Disable aliasing during registration. This should be set if it was set on
	// worker.Options.DisableRegistrationAliasing when originally run. See
	// documentation for that field for more information.
	DisableRegistrationAliasing bool

	// Optional: Enable logging in replay.
	// In the workflow code you can use workflow.GetLogger(ctx) to write logs. By default, the logger will skip log
	// entry during replay mode so you won't see duplicate logs. This option will enable the logging in replay mode.
	// This is only useful for debugging purpose.
	//
	// default: false
	EnableLoggingInReplay bool

	// Optional: Disable the default 1 second deadlock detection timeout. This option can be used to step through
	// workflow code with multiple breakpoints in a debugger.
	DisableDeadlockDetection bool

	// Plugins that can configure options and intercept replays.
	//
	// Plugins themselves should never mutate this field, the behavior is
	// undefined.
	//
	// NOTE: Experimental
	Plugins []WorkerPlugin

	// ExternalStorage configures external payload storage for replay.
	// Set this to the same ExternalStorage used by the original worker so that
	// externally stored payloads in the history are resolved before being
	// passed to the workflow code.
	//
	// NOTE: Experimental
	ExternalStorage converter.ExternalStorage
}

// ReplayWorkflowHistoryOptions are options for replaying a workflow.
type ReplayWorkflowHistoryOptions struct {
	// OriginalExecution - Overide the workflow execution details used for replay.
	// Optional
	OriginalExecution WorkflowExecution
}

// NewWorkflowReplayer creates an instance of the WorkflowReplayer.
func NewWorkflowReplayer(options WorkflowReplayerOptions) (*WorkflowReplayer, error) {
	_ = "STUB: not implemented"
	// Configure replayer
	return nil, nil
}

// RegisterWorkflow registers workflow function to replay
func (aw *WorkflowReplayer) RegisterWorkflow(w interface{}) { _ = "STUB: not implemented"; return }

// RegisterWorkflowWithOptions registers workflow function with custom workflow name to replay
func (aw *WorkflowReplayer) RegisterWorkflowWithOptions(w interface{}, options RegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

// RegisterDynamicWorkflow registers a dynamic workflow function to replay
func (aw *WorkflowReplayer) RegisterDynamicWorkflow(w interface{}, options DynamicRegisterWorkflowOptions) {
	_ = "STUB: not implemented"
	return
}

// ReplayWorkflowHistoryWithOptions executes a single workflow task for the given history.
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayWorkflowHistoryWithOptions(logger log.Logger, history *historypb.History, options ReplayWorkflowHistoryOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// ReplayWorkflowHistory executes a single workflow task for the given history.
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayWorkflowHistory(logger log.Logger, history *historypb.History) error {
	_ = "STUB: not implemented"
	return nil
}

// ReplayWorkflowHistoryFromJSONFile executes a single workflow task for the given json history file.
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayWorkflowHistoryFromJSONFile(logger log.Logger, jsonfileName string) error {
	_ = "STUB: not implemented"
	return nil
}

// ReplayPartialWorkflowHistoryFromJSONFile executes a single workflow task for the given json history file upto provided
// lastEventID(inclusive).
// Use for testing the backwards compatibility of code changes and troubleshooting workflows in a debugger.
// The logger is an optional parameter. Defaults to the noop logger.
func (aw *WorkflowReplayer) ReplayPartialWorkflowHistoryFromJSONFile(logger log.Logger, jsonfileName string, lastEventID int64) error {
	_ = "STUB: not implemented"
	return nil
}

// ReplayWorkflowExecution replays workflow execution loading it from Temporal service.
func (aw *WorkflowReplayer) ReplayWorkflowExecution(ctx context.Context, service workflowservice.WorkflowServiceClient, logger log.Logger, namespace string, execution WorkflowExecution) error {
	_ = "STUB: not implemented"
	return nil
}

// GetWorkflowResult get the result of a succesfully replayed workflow.
func (aw *WorkflowReplayer) GetWorkflowResult(workflowID string, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (aw *WorkflowReplayer) replayWorkflowHistory(
	logger log.Logger,
	service workflowservice.WorkflowServiceClient,
	namespace string,
	originalExecution WorkflowExecution,
	history *historypb.History,
) error {
	_ = "STUB: not implemented"
	return nil
}

func (aw *WorkflowReplayer) replayWorkflowHistoryRoot(
	logger log.Logger,
	service workflowservice.WorkflowServiceClient,
	namespace string,
	originalExecution WorkflowExecution,
	history *historypb.History,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Hardcoding NopHandler avoids "No metrics handler configured for temporal worker"
// logs during replay.

// Resolve externally stored payloads in the history before passing to the
// task handler. This mirrors what processWorkflowTask does for live workers.

// HistoryFromJSON deserializes history from a reader of JSON bytes. This does
// not close the reader if it is closeable.
func HistoryFromJSON(r io.Reader, lastEventID int64) (*historypb.History, error) {
	_ = "STUB: not implemented"
	// We set DiscardUnknown here because the history may have been created by a previous
	// version of our protos
	return nil, nil
}

// If there is a last event ID, slice the rest off

// Inclusive

func extractHistoryFromFile(jsonfileName string, lastEventID int64) (hist *historypb.History, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// If there is a last event ID, slice the rest off

// Inclusive

// NewAggregatedWorker returns an instance to manage both activity and workflow workers
func NewAggregatedWorker(client *WorkflowClient, taskQueue string, options WorkerOptions) *AggregatedWorker {
	_ = "STUB: not implemented"
	return nil
}

// Combine client-provided worker plugins with current options set and apply to options

// No meaningful context to pass at this time, and all errors are panics when configuring worker

// If max-concurrent workflow pollers is 1, the worker will only do
// sticky-queue requests and never regular-queue requests. We disallow the
// value of 1 here.

// If max-concurrent workflow task execution size is 1, the worker will only do
// sticky-queue requests and never regular-queue requests. This is because we
// limit the number of running pollers to MaxConcurrentWorkflowTaskExecutionSize.
// 	We disallow the value of 1 here.

// Sessions are not currently compatible with worker versioning
// See: https://github.com/temporalio/sdk-go/issues/1227

// Need reference to result for fatal error handler

// Set the fatal error if not already set

// Only do the rest if not already set

// Invoke the callback if present

// Stop the worker if not already stopped

// Because of lazy clients we need to wait till the worker runs to fetch the capabilities.
// All worker systems that depend on the capabilities to process workflow/activity tasks
// should take a pointer to this struct and wait for it to be populated when the worker is run.

// Add worker build ID to the logs if it's set by user

// worker specific registry

// Build set of interceptors using the applicable client ones first (being
// careful not to append to the existing slice)

// workflow factory.

// activity types.

// Resolve the SysInfoProvider used for worker heartbeats. Prefer the explicit
// WorkerOptions.SysInfoProvider; otherwise fall back to the tuner's slot supplier if it
// implements HasSysInfoProvider. If both are set to different providers, that's a config
// error. If neither is set, heartbeats report 0 for CPU/memory usage.

// The callback can be invoked concurrently from the heartbeat worker goroutine and the shutdown path

// Set memoized start as a once-value that invokes plugins first

func processTestTags(wOptions *WorkerOptions, ep *workerExecutionParameters) {
	_ = "STUB: not implemented"
	return
}

func isWorkflowContext(inType reflect.Type) bool {
	_ = "STUB: not implemented"
	// NOTE: We don't expect any one to derive from workflow context.
	return false
}

func isValidResultType(inType reflect.Type) bool {
	_ = "STUB: not implemented"
	// https://golang.org/pkg/reflect/#Kind
	return false
}

func isError(inType reflect.Type) bool { _ = "STUB: not implemented"; return false }

func getFunctionName(i interface{}) (name string, isMethod bool) {
	_ = "STUB: not implemented"
	return "", false
}

// Full function name that has a struct pointer receiver has the following format
// <prefix>.(*<type>).<function>

// This allows to call activities by method pointer
// Compiler adds -fm suffix to a function name which has a receiver
// Note that this works even if struct pointer used to get the function is nil
// It is possible because nil receivers are allowed.
// For example:
// var a *Activities
// ExecuteActivity(ctx, a.Foo)
// will call this function which is going to return "Foo"

func getActivityFunctionName(r *registry, i interface{}) string {
	_ = "STUB: not implemented"
	return ""
}

func getWorkflowFunctionName(r *registry, workflowFunc interface{}) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func getReadOnlyChannel(c chan struct{}) <-chan struct{} { _ = "STUB: not implemented"; return nil }

func setWorkerOptionsDefaults(options *WorkerOptions) { _ = "STUB: not implemented"; return }

// Disable eager activities when the task queue rate limit is set because
// the server does not rate limit eager activities.

// Err cannot happen since these slot numbers are guaranteed valid

// setClientDefaults should be needed only in unit tests.
func setClientDefaults(client *WorkflowClient) { _ = "STUB: not implemented"; return }

// getTestTags returns the test tags in the context.
func getTestTags(ctx context.Context) map[string]map[string]string {
	_ = "STUB: not implemented"
	return nil
}

// Same as executeFunction but injects the workflow context as the first
// parameter if the function takes it (regardless of existing parameters).
func executeFunctionWithWorkflowContext(ctx Context, fn interface{}, args []interface{}) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Same as executeFunction but injects the context as the first parameter if the
// function takes it (regardless of existing parameters).
func executeFunctionWithContext(ctx context.Context, fn interface{}, args []interface{}) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Executes function and ensures that there is always 1 or 2 results and second
// result is error.
func executeFunction(fn interface{}, args []interface{}) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// If the argument is nil, use zero value

// Expect either error or (result, error)

// Convert error

// If there are two results, convert the first only if it's not a nil pointer

func workerDeploymentVersionFromProto(wd *deploymentpb.WorkerDeploymentVersion) WorkerDeploymentVersion {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentVersion)
}

func (wd *WorkerDeploymentVersion) toProto() *deploymentpb.WorkerDeploymentVersion {
	_ = "STUB: not implemented"
	return nil
}

func (wd *WorkerDeploymentVersion) toCanonicalString() string { _ = "STUB: not implemented"; return "" }

func workerDeploymentVersionFromString(version string) *WorkerDeploymentVersion {
	_ = "STUB: not implemented"
	return nil
}

func workerDeploymentVersionFromProtoOrString(wd *deploymentpb.WorkerDeploymentVersion, fallback string) *WorkerDeploymentVersion {
	_ = "STUB: not implemented"
	return nil
}

func getCpuUsage(supplier SysInfoProvider, logger log.Logger) float32 {
	_ = "STUB: not implemented"
	return 0
}

func getMemUsage(supplier SysInfoProvider, logger log.Logger) float32 {
	_ = "STUB: not implemented"
	return 0
}

// collectPluginInfos collects plugin names from client and worker plugins,
// deduplicates them, and returns a slice of PluginInfo for heartbeat reporting.
func collectPluginInfos(clientPluginNames []string, workerPlugins []WorkerPlugin) []*workerpb.PluginInfo {
	_ = "STUB: not implemented"
	return nil
}

func collectStorageDriverInfos(driverTypes []string) []*workerpb.StorageDriverInfo {
	_ = "STUB: not implemented"
	return nil
}
