package internal

import (
	"errors"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"

	"go.temporal.io/sdk/converter"
)

/*
If activity fails then *ActivityError is returned to the workflow code. The error has important information about activity
and actual error which caused activity failure. This internal error can be unwrapped using errors.Unwrap() or checked using errors.As().
Below are the possible types of internal error:
1) *ApplicationError: (this should be the most common one)
	*ApplicationError can be returned in two cases:
		- If activity implementation returns *ApplicationError by using NewApplicationError()/NewNonRetryableApplicationError() API.
		  The error would contain a message and optional details. Workflow code could extract details to string typed variable, determine
		  what kind of error it was, and take actions based on it. The details are encoded payload therefore, workflow code needs to know what
          the types of the encoded details are before extracting them.
		- If activity implementation returns errors other than from NewApplicationError() API. In this case GetOriginalType()
		  will return original type of error represented as string. Workflow code could check this type to determine what kind of error it was
		  and take actions based on the type. These errors are retryable by default, unless error type is specified in retry policy.
2) *CanceledError:
	If activity was canceled, internal error will be an instance of *CanceledError. When activity cancels itself by
	returning NewCancelError() it would supply optional details which could be extracted by workflow code.
3) *TimeoutError:
	If activity was timed out (several timeout types), internal error will be an instance of *TimeoutError. The err contains
	details about what type of timeout it was.
4) *PanicError:
	If activity code panic while executing, temporal activity worker will report it as activity failure to temporal server.
	The SDK will present that failure as *PanicError. The error contains a string	representation of the panic message and
	the call stack when panic was happen.
Workflow code could handle errors based on different types of error. Below is sample code of how error handling looks like.

err := workflow.ExecuteActivity(ctx, MyActivity, ...).Get(ctx, nil)
if err != nil {
	var applicationErr *ApplicationError
	if errors.As(err, &applicationError) {
		// retrieve error message
		fmt.Println(applicationError.Error())

		// handle activity errors (created via NewApplicationError() API)
		var detailMsg string // assuming activity return error by NewApplicationError("message", true, "string details")
		applicationErr.Details(&detailMsg) // extract strong typed details

		// handle activity errors (errors created other than using NewApplicationError() API)
		switch err.Type() {
		case "CustomErrTypeA":
			// handle CustomErrTypeA
		case CustomErrTypeB:
			// handle CustomErrTypeB
		default:
			// newer version of activity could return new errors that workflow was not aware of.
		}
	}

	var canceledErr *CanceledError
	if errors.As(err, &canceledErr) {
		// handle cancellation
	}

	var timeoutErr *TimeoutError
	if errors.As(err, &timeoutErr) {
		// handle timeout, could check timeout type by timeoutErr.TimeoutType()
        switch err.TimeoutType() {
        case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
			// Handle ScheduleToStart timeout.
        case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
            // Handle StartToClose timeout.
        case enumspb.TIMEOUT_TYPE_HEARTBEAT:
            // Handle heartbeat timeout.
        default:
        }
	}

	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		// handle panic, message and stack trace are available by panicErr.Error() and panicErr.StackTrace()
	}
}
Errors from child workflow should be handled in a similar way, except that instance of *ChildWorkflowExecutionError is returned to
workflow code. It might contain *ActivityError in case if error comes from activity (which in turn will contain on of the errors above),
or *ApplicationError in case if error comes from child workflow itself.

When panic happen in workflow implementation code, SDK catches that panic and causing the workflow task timeout.
That workflow task will be retried at a later time (with exponential backoff retry intervals).
Workflow consumers will get an instance of *WorkflowExecutionError. This error will contain one of errors above.
*/

type (
	// ApplicationErrorOptions represents a combination of error attributes and additional requests.
	// All fields are optional, providing flexibility in error customization.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ApplicationErrorOptions]
	ApplicationErrorOptions struct {
		// NonRetryable indicates if the error should not be retried regardless of the retry policy.
		NonRetryable bool
		// Cause is the original error that caused this error.
		Cause error
		// Details is a list of arbitrary values that can be used to provide additional context to the error.
		Details []interface{}
		// NextRetryInterval is a request from server to override retry interval calculated by the
		// server according to the RetryPolicy set by the Workflow.
		// It is impossible to specify immediate retry as it is indistinguishable from the default value. As a
		// workaround you could set NextRetryDelay to some small value.
		//
		// NOTE: This option is supported by Temporal Server >= v1.24.2 older version will ignore this value.
		NextRetryDelay time.Duration
		// Category of the error. Maps to logging/metrics behaviors.
		Category ApplicationErrorCategory
	}

	// ApplicationError returned from activity implementations with message and optional details.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ApplicationError]
	ApplicationError struct {
		temporalError
		msg            string
		errType        string
		nonRetryable   bool
		cause          error
		details        converter.EncodedValues
		nextRetryDelay time.Duration
		category       ApplicationErrorCategory
	}

	// TimeoutError returned when activity or child workflow timed out.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.TimeoutError]
	TimeoutError struct {
		temporalError
		msg                  string
		timeoutType          enumspb.TimeoutType
		lastHeartbeatDetails converter.EncodedValues
		cause                error
	}

	// CanceledErrorOptions should be used to set all the desired attributes of a new CanceledError
	//
	// Exposed as: [go.temporal.io/sdk/temporal.CanceledErrorOptions]
	CanceledErrorOptions struct {
		// Message is the error message.
		// Defaults to "canceled" if not set.
		Message string
		// Details is a list of arbitrary values that can be used to provide additional context to the error.
		Details []any
		// Cause is the original error that caused this error.
		Cause error
	}

	// CanceledError returned when operation was canceled.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.CanceledError]
	CanceledError struct {
		temporalError
		msg     string
		cause   error
		details converter.EncodedValues
	}

	// TerminatedError returned when workflow was terminated.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.TerminatedError]
	TerminatedError struct {
		temporalError
	}

	// PanicError contains information about panicked workflow/activity.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.PanicError]
	PanicError struct {
		temporalError
		value      interface{}
		stackTrace string
	}

	// workflowPanicError contains information about panicked workflow.
	// Used to distinguish go panic in the workflow code from a PanicError returned from a workflow function.
	workflowPanicError struct {
		value      interface{}
		stackTrace string
	}

	// ContinueAsNewError contains information about how to continue the workflow as new.
	//
	// Exposed as: [go.temporal.io/sdk/workflow.ContinueAsNewError]
	ContinueAsNewError struct {
		// params *ExecuteWorkflowParams
		WorkflowType        *WorkflowType
		Input               *commonpb.Payloads
		Header              *commonpb.Header
		TaskQueueName       string
		WorkflowRunTimeout  time.Duration
		WorkflowTaskTimeout time.Duration

		// Deprecated: WorkflowExecutionTimeout is deprecated and is never set or
		// used internally.
		WorkflowExecutionTimeout time.Duration

		// VersioningIntent specifies whether the continued workflow should run on a worker with a
		// compatible build ID or not. See VersioningIntent.
		//
		// Deprecated: Use Worker Deployment Versioning instead. See https://docs.temporal.io/worker-versioning
		VersioningIntent VersioningIntent

		// InitialVersioningBehavior specifies the versioning behavior that the first task of the new run should use.
		// For example, choose to AutoUpgrade on continue-as-new instead of inheriting the pinned version of the previous run.
		// NOTE: Upgrade-on-Continue-as-New is currently experimental.
		InitialVersioningBehavior ContinueAsNewVersioningBehavior

		// This is by default nil but may be overridden using NewContinueAsNewErrorWithOptions.
		// It specifies the retry policy which gets carried over to the next run.
		// If not set, the current workflow's retry policy will be carried over automatically.
		//
		// NOTES:
		// 1. This is always nil when returned from a client as a workflow response.
		// 2. Unlike other options that can be overridden using WithWorkflowTaskQueue, WithWorkflowRunTimeout, etc.
		//    we can't introduce an option, say WithWorkflowRetryPolicy, for backward compatibility.
		//    See #676 or IntegrationTestSuite::TestContinueAsNewWithWithChildWF for more details.
		RetryPolicy *RetryPolicy
	}

	// ContinueAsNewErrorOptions specifies optional attributes to be carried over to the next run.
	//
	// Exposed as: [go.temporal.io/sdk/workflow.ContinueAsNewErrorOptions]
	ContinueAsNewErrorOptions struct {
		// RetryPolicy specifies the retry policy to be used for the next run.
		// If nil, the current workflow's retry policy will be used.
		RetryPolicy *RetryPolicy

		// InitialVersioningBehavior specifies the versioning behavior that the first task of the new run should use.
		// For example, choose to AutoUpgrade on continue-as-new instead of inheriting the pinned version of the previous run.
		// NOTE: Upgrade-on-Continue-as-New is currently experimental.
		InitialVersioningBehavior ContinueAsNewVersioningBehavior
	}

	// UnknownExternalWorkflowExecutionError can be returned when external workflow doesn't exist
	UnknownExternalWorkflowExecutionError struct{}

	// ServerError can be returned from server.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ServerError]
	ServerError struct {
		temporalError
		msg          string
		nonRetryable bool
		cause        error
	}

	// ActivityError is returned from workflow when activity returned an error.
	// Unwrap this error to get actual cause.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ActivityError]
	ActivityError struct {
		temporalError
		scheduledEventID int64
		startedEventID   int64
		identity         string
		activityType     *commonpb.ActivityType
		activityID       string
		retryState       enumspb.RetryState
		cause            error
	}

	// ChildWorkflowExecutionError is returned from workflow when child workflow returned an error.
	// Unwrap this error to get actual cause.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ChildWorkflowExecutionError]
	ChildWorkflowExecutionError struct {
		temporalError
		namespace        string
		workflowID       string
		runID            string
		workflowType     string
		initiatedEventID int64
		startedEventID   int64
		retryState       enumspb.RetryState
		cause            error
	}

	// NexusOperationError is an error returned when a Nexus Operation has failed.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.NexusOperationError]
	NexusOperationError struct {
		// The raw proto failure object this error was created from.
		Failure *failurepb.Failure
		// Error message.
		Message string
		// ID of the NexusOperationScheduled event.
		ScheduledEventID int64
		// Endpoint name.
		Endpoint string
		// Service name.
		Service string
		// Operation name.
		Operation string
		// Operation token - may be empty if the operation completed synchronously.
		OperationToken string
		// Chained cause - typically an ApplicationError or a CanceledError.
		Cause error
	}

	// ChildWorkflowExecutionAlreadyStartedError is set as the cause of
	// ChildWorkflowExecutionError when failure is due the child workflow having
	// already started.
	ChildWorkflowExecutionAlreadyStartedError struct{}

	// NamespaceNotFoundError is set as the cause when failure is due namespace not found.
	NamespaceNotFoundError struct{}

	// WorkflowExecutionError is returned from workflow.
	// Unwrap this error to get actual cause.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.WorkflowExecutionError]
	WorkflowExecutionError struct {
		workflowID   string
		runID        string
		workflowType string
		cause        error
	}

	// ActivityNotRegisteredError is returned if worker doesn't support activity type.
	ActivityNotRegisteredError struct {
		activityType   string
		supportedTypes []string
	}

	temporalError struct {
		messenger
		originalFailure *failurepb.Failure
	}

	failureHolder interface {
		setFailure(*failurepb.Failure)
		failure() *failurepb.Failure
	}

	messenger interface {
		message() string
	}
)

var (
	// Should be "errorString".
	goErrType = reflect.TypeOf(errors.New("")).Elem().Name()

	// ErrNoData is returned when trying to extract strong typed data while there is no data available.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ErrNoData]
	ErrNoData = errors.New("no data available")

	// ErrTooManyArg is returned when trying to extract strong typed data with more arguments than available data.
	ErrTooManyArg = errors.New("too many arguments")

	// ErrActivityResultPending is returned from activity's implementation to indicate the activity is not completed when
	// activity method returns. Activity needs to be completed by Client.CompleteActivity() separately. For example, if an
	// activity require human interaction (like approve an expense report), the activity could return activity.ErrResultPending
	// which indicate the activity is not done yet. Then, when the waited human action happened, it needs to trigger something
	// that could report the activity completed event to temporal server via Client.CompleteActivity() API.
	//
	// Exposed as: [go.temporal.io/sdk/activity.ErrResultPending]
	ErrActivityResultPending = errors.New("not error: do not autocomplete, using Client.CompleteActivity() to complete")

	// ErrScheduleAlreadyRunning is returned if there's already a running (not deleted) Schedule with the same ID
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ErrScheduleAlreadyRunning]
	ErrScheduleAlreadyRunning = errors.New("schedule with this ID is already registered")

	// ErrSkipScheduleUpdate is used by a user if they want to skip updating a schedule.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ErrSkipScheduleUpdate]
	ErrSkipScheduleUpdate = errors.New("skip schedule update")

	// ErrMissingWorkflowID is returned when trying to start an async Nexus operation but no workflow ID is set on the request.
	ErrMissingWorkflowID = errors.New("workflow ID is unset for Nexus operation")
)

// ApplicationErrorCategory sets the category of the error. The category of the error
// maps to logging/metrics behaviors.
//
// Exposed as: [go.temporal.io/sdk/temporal.ApplicationErrorCategory]
type ApplicationErrorCategory int

const (
	// ApplicationErrorCategoryUnspecified represents an error with an unspecified category.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ApplicationErrorCategoryUnspecified]
	ApplicationErrorCategoryUnspecified ApplicationErrorCategory = iota
	// ApplicationErrorCategoryBenign indicates an error that is expected under normal operation and should not trigger alerts.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.ApplicationErrorCategoryBenign]
	ApplicationErrorCategoryBenign
)

// NewApplicationError create new instance of *ApplicationError with message, type, and optional details.
func NewApplicationError(msg string, errType string, nonRetryable bool, cause error, details ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Exposed as: [go.temporal.io/sdk/temporal.NewApplicationError], [go.temporal.io/sdk/temporal.NewApplicationErrorWithOptions], [go.temporal.io/sdk/temporal.NewApplicationErrorWithCause], [go.temporal.io/sdk/temporal.NewNonRetryableApplicationError]
func NewApplicationErrorWithOptions(msg string, errType string, options ApplicationErrorOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// When return error to user, use EncodedValues as details and data is ready to be decoded by calling Get

// When create error for server, use ErrorDetailsValues as details to hold values and encode later

// NewTimeoutError creates TimeoutError instance.
// Use NewHeartbeatTimeoutError to create heartbeat TimeoutError.
//
// Exposed as: [go.temporal.io/sdk/temporal.NewTimeoutError]
func NewTimeoutError(msg string, timeoutType enumspb.TimeoutType, cause error, lastHeartbeatDetails ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// NewHeartbeatTimeoutError creates TimeoutError instance.
//
// Exposed as: [go.temporal.io/sdk/temporal.NewHeartbeatTimeoutError]
func NewHeartbeatTimeoutError(details ...interface{}) error { _ = "STUB: not implemented"; return nil }

// NewCanceledError creates CanceledError instance.
//
// Exposed as: [go.temporal.io/sdk/temporal.NewCanceledError]
func NewCanceledError(details ...interface{}) error { _ = "STUB: not implemented"; return nil }

// NewCanceledErrorWithOptions creates CanceledError instance.
//
// Exposed as: [go.temporal.io/sdk/temporal.NewCanceledErrorWithOptions]
func NewCanceledErrorWithOptions(options CanceledErrorOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// NewServerError create new instance of *ServerError with message.
func NewServerError(msg string, nonRetryable bool, cause error) error {
	_ = "STUB: not implemented"
	return nil
}

// NewActivityError creates ActivityError instance.
func NewActivityError(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	activityType *commonpb.ActivityType,
	activityID string,
	retryState enumspb.RetryState,
	cause error,
) *ActivityError {
	_ = "STUB: not implemented"
	return nil
}

// NewChildWorkflowExecutionError creates ChildWorkflowExecutionError instance.
func NewChildWorkflowExecutionError(
	namespace string,
	workflowID string,
	runID string,
	workflowType string,
	initiatedEventID int64,
	startedEventID int64,
	retryState enumspb.RetryState,
	cause error,
) *ChildWorkflowExecutionError {
	_ = "STUB: not implemented"
	return nil
}

// NewWorkflowExecutionError creates WorkflowExecutionError instance.
func NewWorkflowExecutionError(
	workflowID string,
	runID string,
	workflowType string,
	cause error,
) *WorkflowExecutionError {
	_ = "STUB: not implemented"
	return nil
}

func (e *temporalError) setFailure(f *failurepb.Failure) { _ = "STUB: not implemented"; return }

func (e *temporalError) failure() *failurepb.Failure { _ = "STUB: not implemented"; return nil }

// IsCanceledError returns whether error in CanceledError.
func IsCanceledError(err error) bool { _ = "STUB: not implemented"; return false }

// NewContinueAsNewError creates ContinueAsNewError instance
// If the workflow main function returns this error then the current execution is ended and
// the new execution with same workflow ID is started automatically with options
// provided to this function.
//
//	 ctx - use context to override any options for the new workflow like run timeout, task timeout, task queue.
//		  if not mentioned it would use the defaults that the current workflow is using.
//	       ctx := WithWorkflowRunTimeout(ctx, 30 * time.Minute)
//	       ctx := WithWorkflowTaskTimeout(ctx, 5 * time.Second)
//		  ctx := WithWorkflowTaskQueue(ctx, "example-group")
//	 wfn - workflow function. for new execution it can be different from the currently running.
//	 args - arguments for the new workflow.
//
// Exposed as: [go.temporal.io/sdk/workflow.NewContinueAsNewError]
func NewContinueAsNewError(ctx Context, wfn interface{}, args ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Put header on context before executing

// NewContinueAsNewErrorWithOptions creates ContinueAsNewError instance with additional options.
//
// Exposed as: [go.temporal.io/sdk/workflow.NewContinueAsNewErrorWithOptions]
func NewContinueAsNewErrorWithOptions(ctx Context, options ContinueAsNewErrorOptions, wfn interface{}, args ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (wc *workflowEnvironmentInterceptor) NewContinueAsNewError(
	ctx Context,
	wfn interface{},
	args ...interface{},
) error {
	_ = "STUB: not implemented"
	// Validate type and its arguments.
	return nil
}

// The retry policy can't be propagated like other options due to #676.

// NewActivityNotRegisteredError creates a new ActivityNotRegisteredError.
func NewActivityNotRegisteredError(activityType string, supportedTypes []string) error {
	_ = "STUB: not implemented"
	return nil
}

// Error from error interface.
func (e *ApplicationError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *ApplicationError) message() string {
	_ = "STUB: not implemented"

	// Message contains just the message string without extras added by Error().
	return ""
}

func (e *ApplicationError) Message() string {
	_ = "STUB: not implemented"

	// Type returns error type represented as string.
	// This type can be passed explicitly to ApplicationError constructor.
	// Also any other Go error is converted to ApplicationError and type is set automatically using reflection.
	// For example instance of "MyCustomError struct" will be converted to ApplicationError and Type() will return "MyCustomError" string.
	return ""
}

func (e *ApplicationError) Type() string {
	_ = "STUB: not implemented"

	// HasDetails return if this error has strong typed detail data.
	return ""
}

func (e *ApplicationError) HasDetails() bool { _ = "STUB: not implemented"; return false }

// Details extracts strong typed detail data of this custom error. If there is no details, it will return ErrNoData.
func (e *ApplicationError) Details(d ...interface{}) error { _ = "STUB: not implemented"; return nil }

// NonRetryable indicated if error is not retryable.
func (e *ApplicationError) NonRetryable() bool { _ = "STUB: not implemented"; return false }

func (e *ApplicationError) Unwrap() error {
	_ = "STUB: not implemented"

	// NextRetryDelay returns the delay to wait before retrying the activity.
	// a zero value means to use the activities retry policy.
	return nil
}

func (e *ApplicationError) NextRetryDelay() time.Duration {
	_ = "STUB: not implemented"
	return *

	// Category returns the ApplicationErrorCategory of the error.
	new(time.Duration)
}

func (e *ApplicationError) Category() ApplicationErrorCategory {
	_ = "STUB: not implemented"

	// Error from error interface
	return *new(ApplicationErrorCategory)
}

func (e *TimeoutError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *TimeoutError) message() string {
	_ = "STUB: not implemented"

	// Message contains just the message string without extras added by Error().
	return ""
}

func (e *TimeoutError) Message() string { _ = "STUB: not implemented"; return "" }

func (e *TimeoutError) Unwrap() error {
	_ = "STUB: not implemented"

	// TimeoutType return timeout type of this error
	return nil
}

func (e *TimeoutError) TimeoutType() enumspb.TimeoutType {
	_ = "STUB: not implemented"
	return *

	// HasLastHeartbeatDetails return if this error has strong typed detail data.
	new(enumspb.TimeoutType)
}

func (e *TimeoutError) HasLastHeartbeatDetails() bool { _ = "STUB: not implemented"; return false }

// LastHeartbeatDetails extracts strong typed detail data of this error. If there is no details, it will return ErrNoData.
func (e *TimeoutError) LastHeartbeatDetails(d ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Error from error interface
func (e *CanceledError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *CanceledError) message() string { _ = "STUB: not implemented"; return "" }

func (e *CanceledError) Unwrap() error {
	_ = "STUB: not implemented"

	// HasDetails return if this error has strong typed detail data.
	return nil
}

func (e *CanceledError) HasDetails() bool { _ = "STUB: not implemented"; return false }

// Details extracts strong typed detail data of this error.
func (e *CanceledError) Details(d ...interface{}) error { _ = "STUB: not implemented"; return nil }

func newPanicError(value interface{}, stackTrace string) error {
	_ = "STUB: not implemented"
	return nil
}

func newWorkflowPanicError(value interface{}, stackTrace string) error {
	_ = "STUB: not implemented"
	return nil
}

// Error from error interface
func (e *PanicError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *PanicError) message() string { _ = "STUB: not implemented"; return "" }

// StackTrace return stack trace of the panic
func (e *PanicError) StackTrace() string { _ = "STUB: not implemented"; return "" }

// Error from error interface
func (e *workflowPanicError) Error() string { _ = "STUB: not implemented"; return "" }

// StackTrace return stack trace of the panic
func (e *workflowPanicError) StackTrace() string { _ = "STUB: not implemented"; return "" }

// Error from error interface
func (e *ContinueAsNewError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *ContinueAsNewError) message() string { _ = "STUB: not implemented"; return "" }

// newTerminatedError creates NewTerminatedError instance
func newTerminatedError() *TerminatedError { _ = "STUB: not implemented"; return nil }

// Error from error interface
func (e *TerminatedError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *TerminatedError) message() string { _ = "STUB: not implemented"; return "" }

// newUnknownExternalWorkflowExecutionError creates UnknownExternalWorkflowExecutionError instance
func newUnknownExternalWorkflowExecutionError() *UnknownExternalWorkflowExecutionError {
	_ = "STUB: not implemented"
	return nil
}

// Error from error interface
func (e *UnknownExternalWorkflowExecutionError) Error() string {
	_ = "STUB: not implemented"
	return ""
}

// Error from error interface
func (e *ServerError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *ServerError) message() string {
	_ = "STUB: not implemented"

	// Message contains just the message string without extras added by Error().
	return ""
}

func (e *ServerError) Message() string { _ = "STUB: not implemented"; return "" }

func (e *ServerError) Unwrap() error { _ = "STUB: not implemented"; return nil }

func (e *ActivityError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *ActivityError) message() string { _ = "STUB: not implemented"; return "" }

func (e *ActivityError) Unwrap() error {
	_ = "STUB: not implemented"

	// ScheduledEventID returns event id of the scheduled workflow task corresponding to the activity.
	return nil
}

func (e *ActivityError) ScheduledEventID() int64 { _ = "STUB: not implemented"; return 0 }

// StartedEventID returns event id of the started workflow task corresponding to the activity.
func (e *ActivityError) StartedEventID() int64 { _ = "STUB: not implemented"; return 0 }

// Identity returns identity of the worker that attempted activity execution.
func (e *ActivityError) Identity() string {
	_ = "STUB: not implemented"

	// ActivityType returns declared type of the activity.
	return ""
}

func (e *ActivityError) ActivityType() *commonpb.ActivityType {
	_ = "STUB: not implemented"
	return nil

	// ActivityID return assigned identifier for the activity.
}

func (e *ActivityError) ActivityID() string { _ = "STUB: not implemented"; return "" }

// RetryState returns details on why activity failed.
func (e *ActivityError) RetryState() enumspb.RetryState {
	_ = "STUB: not implemented"
	return *

	// Error from error interface
	new(enumspb.RetryState)
}

func (e *ChildWorkflowExecutionError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *ChildWorkflowExecutionError) message() string { _ = "STUB: not implemented"; return "" }

func (e *ChildWorkflowExecutionError) Unwrap() error {
	_ = "STUB: not implemented"

	// Namespace returns namespace of the child workflow.
	return nil
}

func (e *ChildWorkflowExecutionError) Namespace() string {
	_ = "STUB: not implemented"

	// WorkflowId returns workflow ID of the child workflow.
	return ""
}

func (e *ChildWorkflowExecutionError) WorkflowID() string { _ = "STUB: not implemented"; return "" }

// RunID returns run ID of the child workflow.
func (e *ChildWorkflowExecutionError) RunID() string {
	_ = "STUB: not implemented"

	// WorkflowType returns type of the child workflow.
	return ""
}

func (e *ChildWorkflowExecutionError) WorkflowType() string { _ = "STUB: not implemented"; return "" }

// InitiatedEventID returns event ID of the child workflow initiated event.
func (e *ChildWorkflowExecutionError) InitiatedEventID() int64 { _ = "STUB: not implemented"; return 0 }

// StartedEventID returns event ID of the child workflow started event.
func (e *ChildWorkflowExecutionError) StartedEventID() int64 { _ = "STUB: not implemented"; return 0 }

// RetryState returns details on why child workflow failed.
func (e *ChildWorkflowExecutionError) RetryState() enumspb.RetryState {
	_ = "STUB: not implemented"
	return *

	// Error implements the error interface.
	new(enumspb.RetryState)
}

func (e *NexusOperationError) Error() string { _ = "STUB: not implemented"; return "" }

// setFailure implements the failureHolder interface for consistency with other failure based errors..
func (e *NexusOperationError) setFailure(f *failurepb.Failure) {
	_ = "STUB: not implemented"

	// failure implements the failureHolder interface for consistency with other failure based errors.
	return
}

func (e *NexusOperationError) failure() *failurepb.Failure {
	_ = "STUB: not implemented"

	// Unwrap returns the Cause associated with this error.
	return nil
}

func (e *NexusOperationError) Unwrap() error {
	_ = "STUB: not implemented"

	// Error from error interface
	return nil
}

func (*NamespaceNotFoundError) Error() string { _ = "STUB: not implemented"; return "" }

// Error from error interface
func (*ChildWorkflowExecutionAlreadyStartedError) Error() string {
	_ = "STUB: not implemented"
	return ""
}

// Error from error interface
func (e *WorkflowExecutionError) Error() string { _ = "STUB: not implemented"; return "" }

func (e *WorkflowExecutionError) Unwrap() error { _ = "STUB: not implemented"; return nil }

func (e *ActivityNotRegisteredError) Error() string { _ = "STUB: not implemented"; return "" }

func convertErrDetailsToPayloads(details converter.EncodedValues, dc converter.DataConverter) *commonpb.Payloads {
	_ = "STUB: not implemented"
	return nil
}

// IsRetryable returns if error retryable or not.
func IsRetryable(err error, nonRetryableTypes []string) bool {
	_ = "STUB: not implemented"
	return false
}

// If it is generic Go error.

func getErrType(err error) string { _ = "STUB: not implemented"; return "" }

func isBenignApplicationError(err error) bool { _ = "STUB: not implemented"; return false }

func isBenignProtoApplicationFailure(failure *failurepb.Failure) bool {
	_ = "STUB: not implemented"
	return false
}
