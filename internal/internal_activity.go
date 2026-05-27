package internal

// All code in this file is private to the package.

import (
	"context"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

type (
	// activity is an interface of an activity implementation.
	activity interface {
		Execute(ctx context.Context, input *commonpb.Payloads) (*commonpb.Payloads, error)
		ActivityType() ActivityType
		GetFunction() interface{}
	}

	// ActivityID uniquely identifies an activity execution
	ActivityID struct {
		id string
	}

	// LocalActivityID uniquely identifies a local activity execution
	LocalActivityID struct {
		id string
	}

	// ExecuteActivityOptions option for executing an activity
	ExecuteActivityOptions struct {
		ActivityID             string // Users can choose IDs but our framework makes it optional to decrease the crust.
		TaskQueueName          string
		ScheduleToCloseTimeout time.Duration
		ScheduleToStartTimeout time.Duration
		StartToCloseTimeout    time.Duration
		HeartbeatTimeout       time.Duration
		WaitForCancellation    bool
		OriginalTaskQueueName  string
		RetryPolicy            *commonpb.RetryPolicy
		DisableEagerExecution  bool
		VersioningIntent       VersioningIntent
		Summary                string
		Priority               *commonpb.Priority
		// ScheduleID must be generated before serialization to give
		// codecs/converters access to ActivityID, while maintaining
		// backwards compatibility with how ActivityID is generated.
		ScheduleID int64
	}

	// ExecuteLocalActivityOptions options for executing a local activity
	ExecuteLocalActivityOptions struct {
		ScheduleToCloseTimeout time.Duration
		StartToCloseTimeout    time.Duration
		RetryPolicy            *RetryPolicy
		Summary                string
	}

	// ExecuteActivityParams parameters for executing an activity
	ExecuteActivityParams struct {
		ExecuteActivityOptions
		ActivityType     ActivityType
		Input            *commonpb.Payloads
		DataConverter    converter.DataConverter    // context-aware DC from ExecuteActivity
		FailureConverter converter.FailureConverter // context-aware FC from ExecuteActivity
		Header           *commonpb.Header
	}

	// ExecuteLocalActivityParams parameters for executing a local activity
	ExecuteLocalActivityParams struct {
		ExecuteLocalActivityOptions
		ActivityFn       interface{} // local activity function pointer
		ActivityType     string      // local activity type
		InputArgs        []interface{}
		WorkflowInfo     *WorkflowInfo
		DataConverter    converter.DataConverter    // context-aware DC from ExecuteLocalActivity
		FailureConverter converter.FailureConverter // context-aware FC from ExecuteLocalActivity
		Attempt          int32
		ScheduledTime    time.Time
		Header           *commonpb.Header
	}

	// AsyncActivityClient for requesting activity execution
	AsyncActivityClient interface {
		// The ExecuteActivity schedules an activity with a callback handler.
		// If the activity failed to complete the callback error would indicate the failure
		// and it can be one of ActivityTaskFailedError, ActivityTaskTimeoutError, ActivityTaskCanceledError
		ExecuteActivity(parameters ExecuteActivityParams, callback ResultHandler) ActivityID

		// This only initiates cancel request for activity. if the activity is configured to not WaitForCancellation then
		// it would invoke the callback handler immediately with error code ActivityTaskCanceledError.
		// If the activity is not running(either scheduled or started) then it is a no-operation.
		RequestCancelActivity(activityID ActivityID)
	}

	// LocalActivityClient for requesting local activity execution
	LocalActivityClient interface {
		ExecuteLocalActivity(params ExecuteLocalActivityParams, callback LocalActivityResultHandler) LocalActivityID

		RequestCancelLocalActivity(activityID LocalActivityID)
	}

	activityEnvironment struct {
		taskToken              []byte
		workflowExecution      WorkflowExecution
		activityID             string
		activityType           ActivityType
		serviceInvoker         ServiceInvoker
		logger                 log.Logger
		metricsHandler         metrics.Handler
		isLocalActivity        bool
		heartbeatTimeout       time.Duration
		scheduleToCloseTimeout time.Duration
		startToCloseTimeout    time.Duration
		deadline               time.Time
		scheduledTime          time.Time
		startedTime            time.Time
		taskQueue              string
		dataConverter          converter.DataConverter
		attempt                int32 // starts from 1.
		heartbeatDetails       *commonpb.Payloads
		workflowType           *WorkflowType
		namespace              string
		workerStopChannel      <-chan struct{}
		contextPropagators     []ContextPropagator
		client                 *WorkflowClient
		priority               *commonpb.Priority
		retryPolicy            *RetryPolicy
		activityRunID          string
	}

	// context.WithValue need this type instead of basic type string to avoid lint error
	contextKey string
)

const (
	activityEnvContextKey            contextKey = "activityEnv"
	activityOptionsContextKey        contextKey = "activityOptions"
	localActivityOptionsContextKey   contextKey = "localActivityOptions"
	activityInterceptorContextKey    contextKey = "activityInterceptor"
	activityEnvInterceptorContextKey contextKey = "activityEnvInterceptor"
)

func (i ActivityID) String() string {
	_ = "STUB: not implemented"

	// ParseActivityID returns ActivityID constructed from its string representation.
	// The string representation should be obtained through ActivityID.String()
	return ""
}

func ParseActivityID(id string) (ActivityID, error) {
	_ = "STUB: not implemented"
	return *new(ActivityID), nil
}

func (i LocalActivityID) String() string {
	_ = "STUB: not implemented"

	// ParseLocalActivityID returns LocalActivityID constructed from its string representation.
	// The string representation should be obtained through LocalActivityID.String()
	return ""
}

func ParseLocalActivityID(v string) (LocalActivityID, error) {
	_ = "STUB: not implemented"
	return *new(LocalActivityID), nil
}

func getActivityEnv(ctx context.Context) *activityEnvironment {
	_ = "STUB: not implemented"
	return nil
}

func getActivityOptions(ctx Context) *ExecuteActivityOptions { _ = "STUB: not implemented"; return nil }

func getLocalActivityOptions(ctx Context) *ExecuteLocalActivityOptions {
	_ = "STUB: not implemented"
	return nil
}

func getValidatedLocalActivityOptions(ctx Context) (*ExecuteLocalActivityOptions, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func validateFunctionArgs(workflowFunc interface{}, args []interface{}, isWorkflow bool) error {
	_ = "STUB: not implemented"
	return nil
}

// We can't validate function passed as string.

// Skip Context function argument.

// Validate provided args match with function order match.

func getValidatedActivityFunction(f interface{}, args []interface{}, registry *registry) (*ActivityType, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func getKind(fType reflect.Type) reflect.Kind { _ = "STUB: not implemented"; return *new(reflect.Kind) }

func isActivityContext(inType reflect.Type) bool { _ = "STUB: not implemented"; return false }

func setActivityParametersIfNotExist(ctx Context) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func setLocalActivityParametersIfNotExist(ctx Context) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

type activityEnvironmentInterceptor struct {
	env                 *activityEnvironment
	inboundInterceptor  ActivityInboundInterceptor
	outboundInterceptor ActivityOutboundInterceptor
	fn                  interface{}
}

func getActivityEnvironmentInterceptor(ctx context.Context) *activityEnvironmentInterceptor {
	_ = "STUB: not implemented"
	return nil
}

func getActivityOutboundInterceptor(ctx context.Context) ActivityOutboundInterceptor {
	_ = "STUB: not implemented"
	return *new(ActivityOutboundInterceptor)
}

func (a *activityEnvironmentInterceptor) Init(outbound ActivityOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

func (a *activityEnvironmentInterceptor) ExecuteActivity(
	ctx context.Context,
	in *ExecuteActivityInput,
) (interface{}, error) {
	_ = "STUB: not implemented"
	// Remove header from context
	return nil, nil
}

func (a *activityEnvironmentInterceptor) GetInfo(ctx context.Context) ActivityInfo {
	_ = "STUB: not implemented"
	return *new(ActivityInfo)
}

func (a *activityEnvironmentInterceptor) GetLogger(ctx context.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (a *activityEnvironmentInterceptor) GetMetricsHandler(ctx context.Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (a *activityEnvironmentInterceptor) RecordHeartbeat(ctx context.Context, details ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// no-op for local activity

// We would like to be able to pass in "nil" as part of details(that is no progress to report to)

// Heartbeat error is logged inside ServiceInvoker.internalHeartBeat

func (a *activityEnvironmentInterceptor) HasHeartbeatDetails(ctx context.Context) bool {
	_ = "STUB: not implemented"
	return false
}

func (a *activityEnvironmentInterceptor) GetHeartbeatDetails(ctx context.Context, d ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (a *activityEnvironmentInterceptor) GetWorkerStopChannel(ctx context.Context) <-chan struct{} {
	_ = "STUB: not implemented"
	return nil
}

func (a *activityEnvironmentInterceptor) GetClient(ctx context.Context) Client {
	_ = "STUB: not implemented"
	return *

	// Needed so this can properly be considered an inbound interceptor
	new(Client)
}

func (a *activityEnvironmentInterceptor) mustEmbedActivityInboundInterceptorBase() {
	_ = "STUB: not implemented"

	// Needed so this can properly be considered an outbound interceptor
	return
}

func (a *activityEnvironmentInterceptor) mustEmbedActivityOutboundInterceptorBase() {
	_ = "STUB: not implemented"
	return
}
