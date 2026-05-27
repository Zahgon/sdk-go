package internal

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/common/util"
	"go.temporal.io/sdk/internal/extstore"
	"go.temporal.io/sdk/log"
)

// Assert that structs do indeed implement the interfaces
var (
	_ Client          = (*WorkflowClient)(nil)
	_ NamespaceClient = (*namespaceClient)(nil)
)

var (
	errUnsupportedOperation              = fmt.Errorf("unsupported operation")
	errInvalidServerResponse             = fmt.Errorf("invalid server response")
	errInvalidWithStartWorkflowOperation = fmt.Errorf("invalid WithStartWorkflowOperation")
)

const (
	defaultGetHistoryTimeout       = 65 * time.Second
	defaultGetSystemInfoTimeout    = 5 * time.Second
	pollUpdateTimeout              = 60 * time.Second
	maxListArchivedWorkflowTimeout = 3 * time.Minute
)

type (
	// WorkflowClient is the client for starting a workflow execution.
	WorkflowClient struct {
		workflowService          workflowservice.WorkflowServiceClient
		conn                     *grpc.ClientConn
		namespace                string
		registry                 *registry
		logger                   log.Logger
		metricsHandler           metrics.Handler
		identity                 string
		dataConverter            converter.DataConverter
		failureConverter         converter.FailureConverter
		contextPropagators       []ContextPropagator
		workerPlugins            []WorkerPlugin
		workerInterceptors       []WorkerInterceptor
		clientPluginNames        []string
		interceptor              ClientOutboundInterceptor
		excludeInternalFromRetry *atomic.Bool
		capabilities             *workflowservice.GetSystemInfoResponse_Capabilities
		capabilitiesLock         sync.RWMutex
		namespaceData            *namespaceData
		namespaceDataLock        sync.RWMutex
		eagerDispatcher          *eagerWorkflowDispatcher
		getSystemInfoTimeout     time.Duration
		workerHeartbeatInterval  time.Duration
		workerGroupingKey        string
		heartbeatManager         *heartbeatManager

		// The pointer value is shared across multiple clients. If non-nil, only
		// access/mutate atomically.
		unclosedClients      *int32
		storageParams        extstore.StorageParameters
		storageDriverTypes   []string
		payloadWarningLimits payloadLimits
	}

	// namespaceData holds cached namespace capabilities and limits.
	namespaceData struct {
		capabilities *namespacepb.NamespaceInfo_Capabilities
		limits       *namespacepb.NamespaceInfo_Limits
	}

	// namespaceClient is the client for managing namespaces.
	namespaceClient struct {
		workflowService  workflowservice.WorkflowServiceClient
		connectionCloser io.Closer
		metricsHandler   metrics.Handler
		logger           log.Logger
		identity         string
	}

	// WorkflowRun represents a started non child workflow
	WorkflowRun interface {
		// GetID return workflow ID, which will be same as StartWorkflowOptions.ID if provided.
		GetID() string

		// GetRunID return the first started workflow run ID (please see below) -
		// empty string if no such run. Note, this value may change after Get is
		// called if there was a later run for this run.
		GetRunID() string

		// Get will fill the workflow execution result to valuePtr, if workflow
		// execution is a success, or return corresponding error. If valuePtr is
		// nil, valuePtr will be ignored and only the corresponding error of the
		// workflow will be returned (nil on workflow execution success).
		// This is a blocking API.
		//
		// This call will follow execution runs to the latest result for this run
		// instead of strictly returning this run's result. This means that if the
		// workflow returned ContinueAsNewError, has a more recent cron execution,
		// or has a new run ID on failure (i.e. a retry), this will wait and return
		// the result for the latest run in the chain. To strictly get the result
		// for this run without following to the latest, use GetWithOptions and set
		// the DisableFollowingRuns option to true.
		//
		// Note, values should not be reused for extraction here because merging on
		// top of existing values may result in unexpected behavior similar to
		// json.Unmarshal.
		Get(ctx context.Context, valuePtr interface{}) error

		// GetWithOptions will fill the workflow execution result to valuePtr, if
		// workflow execution is a success, or return corresponding error. If
		// valuePtr is nil, valuePtr will be ignored and only the corresponding
		// error of the workflow will be returned (nil on workflow execution success).
		// This is a blocking API.
		//
		// Note, values should not be reused for extraction here because merging on
		// top of existing values may result in unexpected behavior similar to
		// json.Unmarshal.
		GetWithOptions(ctx context.Context, valuePtr interface{}, options WorkflowRunGetOptions) error
	}

	// WorkflowRunGetOptions are options for WorkflowRun.GetWithOptions.
	WorkflowRunGetOptions struct {
		// DisableFollowingRuns, if true, will not follow execution chains to the
		// latest run. By default when this is false, getting the result of a
		// workflow may not use the literal run ID but instead follow to later runs
		// if the workflow returned a ContinueAsNewError, has a later cron, or is
		// retried on failure.
		DisableFollowingRuns bool
	}

	// workflowRunImpl is an implementation of WorkflowRun
	workflowRunImpl struct {
		workflowType          string
		workflowID            string
		firstRunID            string
		currentRunID          *util.OnceCell
		iterFn                func(ctx context.Context, runID string) HistoryEventIterator
		dataConverter         converter.DataConverter
		failureConverter      converter.FailureConverter
		registry              *registry
		inboundPayloadVisitor PayloadVisitor
	}

	// HistoryEventIterator represents the interface for
	// history event iterator
	HistoryEventIterator interface {
		// HasNext return whether this iterator has next value
		HasNext() bool
		// Next returns the next history events and error
		// The errors it can return:
		//	- serviceerror.NotFound
		//	- serviceerror.InvalidArgument
		//	- serviceerror.Internal
		//	- serviceerror.Unavailable
		Next() (*historypb.HistoryEvent, error)
	}

	// historyEventIteratorImpl is the implementation of HistoryEventIterator
	historyEventIteratorImpl struct {
		// whether this iterator is initialized
		initialized bool
		// local cached history events and corresponding consuming index
		nextEventIndex int
		events         []*historypb.HistoryEvent
		// token to get next page of history events
		nexttoken []byte
		// err when getting next page of history events
		err error
		// func which use a next token to get next page of history events
		paginate func(nexttoken []byte) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
	}

	// QueryRejectedError is a wrapper for QueryRejected
	QueryRejectedError struct {
		queryRejected *querypb.QueryRejected
	}
)

// ExecuteWorkflow starts a workflow execution and returns a WorkflowRun that will allow you to wait until this workflow
// reaches the end state, such as workflow finished successfully or timeout.
// The user can use this to start using a functor like below and get the workflow execution result, as EncodedValue
// Either by
//
//	ExecuteWorkflow(options, "workflowTypeName", arg1, arg2, arg3)
//	or
//	ExecuteWorkflow(options, workflowExecuteFn, arg1, arg2, arg3)
//
// The current timeout resolution implementation is in seconds and uses math.Ceil(d.Seconds()) as the duration. But is
// subjected to change in the future.
//
// NOTE: the context.Context should have a fairly large timeout, since workflow execution may take a while to be finished
func (wc *WorkflowClient) ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// Set header before interceptor run

// Run via interceptor

// GetWorkflow gets a workflow execution and returns a WorkflowRun that will allow you to wait until this workflow
// reaches the end state, such as workflow finished successfully or timeout.
// The current timeout resolution implementation is in seconds and uses math.Ceil(d.Seconds()) as the duration. But is
// subjected to change in the future.
func (wc *WorkflowClient) GetWorkflow(ctx context.Context, workflowID string, runID string) WorkflowRun {
	_ = "STUB: not implemented"
	// We intentionally don't "ensureIntialized" here because there is no direct
	// error return path. Rather we let GetWorkflowHistory do it.
	return *new(WorkflowRun)
}

// The ID may not actually have been set - if not, we have to (lazily) ask the server for info about the workflow
// execution and extract run id from there. This is definitely less efficient than it could be if there was a more
// specific rpc method for this, or if there were more granular history filters - in which case it could be
// extracted from the `iterFn` inside of `workflowRunImpl`

// SignalWorkflow signals a workflow in execution.
func (wc *WorkflowClient) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Set header before interceptor run

// SignalWithStartWorkflow sends a signal to a running workflow.
// If the workflow is not running or not found, it starts the workflow and then sends the signal in transaction.
func (wc *WorkflowClient) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{},
	options StartWorkflowOptions, workflowFunc interface{}, workflowArgs ...interface{},
) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// Due to the ambiguous way to provide workflow IDs, if options contains an
// ID, it must match the parameter

// Default workflow ID to UUID

// Validate function and get name

// Set header before interceptor run

// Run via interceptor

func (wc *WorkflowClient) NewWithStartWorkflowOperation(options StartWorkflowOptions, workflow interface{}, args ...interface{}) WithStartWorkflowOperation {
	_ = "STUB: not implemented"
	return *new(WithStartWorkflowOperation)
}

// CancelWorkflow cancels a workflow in execution.  It allows workflow to properly clean up and gracefully close.
// workflowID is required, other parameters are optional.
// If runID is omit, it will terminate currently running workflow (if there is one) based on the workflowID.
func (wc *WorkflowClient) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	_ = "STUB: not implemented"
	return nil
}

// TerminateWorkflow terminates a workflow execution.
// workflowID is required, other parameters are optional.
// If runID is omit, it will terminate currently running workflow (if there is one) based on the workflowID.
func (wc *WorkflowClient) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// GetWorkflowHistory return a channel which contains the history events of a given workflow
func (wc *WorkflowClient) GetWorkflowHistory(
	ctx context.Context,
	workflowID string,
	runID string,
	isLongPoll bool,
	filterType enumspb.HistoryEventFilterType,
) HistoryEventIterator {
	_ = "STUB: not implemented"
	return *new(HistoryEventIterator)
}

func (wc *WorkflowClient) getWorkflowHistory(
	ctx context.Context,
	workflowID string,
	runID string,
	isLongPoll bool,
	filterType enumspb.HistoryEventFilterType,
	rpcMetricsHandler metrics.Handler,
) HistoryEventIterator {
	_ = "STUB: not implemented"
	return *new(HistoryEventIterator)
}

func (wc *WorkflowClient) getWorkflowExecutionHistory(ctx context.Context, rpcMetricsHandler metrics.Handler, isLongPoll bool,
	request *workflowservice.GetWorkflowExecutionHistoryRequest, filterType enumspb.HistoryEventFilterType,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// CompleteActivity reports activity completed. activity Execute method can return activity.ErrResultPending to
// indicate the activity is not completed when it's Execute method returns. In that case, this CompleteActivity() method
// should be called when that activity is completed with the actual result and error. If err is nil, activity task
// completed event will be reported; if err is CanceledError, activity task canceled event will be reported; otherwise,
// activity task failed event will be reported.
func (wc *WorkflowClient) CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityWithOptions reports activity completed with full context options.
func (wc *WorkflowClient) CompleteActivityWithOptions(ctx context.Context, opts CompleteActivityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// async completion is only for non-local activities

// We do allow canceled error to be passed here

// CompleteActivityByID reports workflow activity completed. Similar to CompleteActivity
// It takes namespace name, workflowID, runID, activityID as arguments.
func (wc *WorkflowClient) CompleteActivityByID(ctx context.Context, namespace, workflowID, runID, activityID string,
	result interface{}, err error,
) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityByIDWithOptions reports workflow activity completed with full context options.
func (wc *WorkflowClient) CompleteActivityByIDWithOptions(ctx context.Context, opts CompleteActivityByIDOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// async completion is only for non-local activities

// We do allow canceled error to be passed here

// CompleteActivityByActivityID reports standalone activity completed. Similar to CompleteActivity
func (wc *WorkflowClient) CompleteActivityByActivityID(ctx context.Context, namespace, activityID, activityRunID string,
	result interface{}, err error,
) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityByActivityIDWithOptions reports standalone activity completed with full context options.
func (wc *WorkflowClient) CompleteActivityByActivityIDWithOptions(ctx context.Context, opts CompleteActivityByActivityIDOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// async completion is only for non-local activities

// We do allow canceled error to be passed here

// RecordActivityHeartbeat records heartbeat for an activity.
func (wc *WorkflowClient) RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// RecordActivityHeartbeatWithOptions records heartbeat for an activity with full context options.
func (wc *WorkflowClient) RecordActivityHeartbeatWithOptions(ctx context.Context, opts RecordActivityHeartbeatOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// async heartbeat is only for non-local activities

// RecordActivityHeartbeatByID records heartbeat for an activity.
func (wc *WorkflowClient) RecordActivityHeartbeatByID(ctx context.Context,
	namespace, workflowID, runID, activityID string, details ...interface{},
) error {
	_ = "STUB: not implemented"
	return nil
}

// RecordActivityHeartbeatByIDWithOptions records heartbeat for an activity with full context options.
func (wc *WorkflowClient) RecordActivityHeartbeatByIDWithOptions(ctx context.Context, opts RecordActivityHeartbeatByIDOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// async heartbeat is only for non-local activities

// ListClosedWorkflow gets closed workflow executions based on request filters
// The errors it can throw:
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
//   - serviceerror.NamespaceNotFound
func (wc *WorkflowClient) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ListOpenWorkflow gets open workflow executions based on request filters
// The errors it can throw:
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
//   - serviceerror.NamespaceNotFound
func (wc *WorkflowClient) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ListWorkflow implementation
func (wc *WorkflowClient) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ListArchivedWorkflow implementation
func (wc *WorkflowClient) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ScanWorkflow implementation
//
//lint:ignore SA1019 the server API was deprecated.
func (wc *WorkflowClient) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

//lint:ignore SA1019 the server API was deprecated.

// CountWorkflow implementation
func (wc *WorkflowClient) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetSearchAttributes implementation
func (wc *WorkflowClient) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
// The errors it can return:
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
//   - serviceerror.NotFound
func (wc *WorkflowClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// DescribeWorkflow returns information about the specified workflow execution.
func (wc *WorkflowClient) DescribeWorkflow(ctx context.Context, workflowID, runID string) (*WorkflowExecutionDescription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// QueryWorkflow queries a given workflow execution
// workflowID and queryType are required, other parameters are optional.
//   - workflow ID of the workflow.
//   - runID can be default(empty string). if empty string then it will pick the running execution of that workflow ID.
//   - taskQueue can be default(empty string). If empty string then it will pick the taskQueue of the running execution of that workflow ID.
//   - queryType is the type of the query.
//   - args... are the optional query parameters.
//
// The errors it can return:
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
//   - serviceerror.NotFound
//   - serviceerror.QueryFailed
func (wc *WorkflowClient) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// Set header before interceptor run

// UpdateWorkflowOptions is the request to UpdateWorkflow
type UpdateWorkflowOptions struct {
	// UpdateID is an application-layer identifier for the requested update. It
	// must be unique within the scope of a Namespace+WorkflowID+RunID.
	UpdateID string

	// WorkflowID is a required field indicating the workflow which should be
	// updated. However, it is optional when using UpdateWithStartWorkflowOperation.
	WorkflowID string

	// RunID is an optional field used to identify a specific run of the target
	// workflow.  If RunID is not provided the latest run will be used.
	// Note that it is incompatible with UpdateWithStartWorkflowOperation.
	RunID string

	// UpdateName is a required field which specifies the update you want to run.
	// See comments at workflow.SetUpdateHandler(ctx Context, updateName string, handler interface{}, opts UpdateHandlerOptions)
	// for more details on how to setup update handlers within the target workflow.
	UpdateName string

	// Args is an optional field used to identify the arguments passed to the
	// update.
	Args []interface{}

	// WaitForStage is a required field which specifies which stage to wait until returning.
	// See https://docs.temporal.io/develop/go/message-passing#send-update-from-client for more details.
	//
	// NOTE: Specifying WorkflowUpdateStageAdmitted is not supported.
	WaitForStage WorkflowUpdateStage

	// FirstExecutionRunID specifies the RunID expected to identify the first
	// run in the workflow execution chain. If this expectation does not match
	// then the server will reject the update request with an error.
	// Note that it is incompatible with UpdateWithStartWorkflowOperation.
	FirstExecutionRunID string
}

// UpdateWithStartWorkflowOptions encapsulates the parameters used by UpdateWithStartWorkflow.
// See UpdateWithStartWorkflow and NewWithStartWorkflowOperation.
type UpdateWithStartWorkflowOptions struct {
	StartWorkflowOperation WithStartWorkflowOperation
	UpdateOptions          UpdateWorkflowOptions
}

// WorkflowUpdateHandle is a handle to a workflow execution update process. The
// update may or may not have completed so an instance of this type functions
// similar to a Future with respect to the outcome of the update. If the update
// is rejected or returns an error, the Get function on this type will return
// that error through the output valuePtr.
type WorkflowUpdateHandle interface {
	// WorkflowID observes the update's workflow ID.
	WorkflowID() string

	// RunID observes the update's run ID.
	RunID() string

	// UpdateID observes the update's ID.
	UpdateID() string

	// Get blocks on the outcome of the update.
	Get(ctx context.Context, valuePtr interface{}) error
}

// GetWorkflowUpdateHandleOptions encapsulates the parameters needed to unambiguously
// refer to a Workflow Update.
type GetWorkflowUpdateHandleOptions struct {
	// WorkflowID of the target update
	WorkflowID string

	// RunID of the target workflow. If blank, use the most recent run
	RunID string

	// UpdateID of the target update
	UpdateID string
}

type baseUpdateHandle struct {
	ref *updatepb.UpdateRef
}

// completedUpdateHandle is an UpdateHandle impelementation for use when the outcome
// of the update is already known and the Get call can return immediately.
type completedUpdateHandle struct {
	baseUpdateHandle
	value converter.EncodedValue
	err   error
}

// lazyUpdateHandle represents and update that is not known to have completed
// yet (i.e. the associated updatepb.Outcome is not known) and thus calling Get
// will poll the server for the outcome.
type lazyUpdateHandle struct {
	baseUpdateHandle
	client *WorkflowClient
}

// QueryWorkflowWithOptionsRequest is the request to QueryWorkflowWithOptions
type QueryWorkflowWithOptionsRequest struct {
	// WorkflowID is a required field indicating the workflow which should be queried.
	WorkflowID string

	// RunID is an optional field used to identify a specific run of the queried workflow.
	// If RunID is not provided the latest run will be used.
	RunID string

	// QueryType is a required field which specifies the query you want to run.
	// By default, temporal supports "__stack_trace" as a standard query type, which will return string value
	// representing the call stack of the target workflow. The target workflow could also setup different query handler to handle custom query types.
	// See comments at workflow.SetQueryHandler(ctx Context, queryType string, handler interface{}) for more details on how to setup query handler within the target workflow.
	QueryType string

	// Args is an optional field used to identify the arguments passed to the query.
	Args []interface{}

	// QueryRejectCondition is an optional field used to reject queries based on workflow state.
	// QUERY_REJECT_CONDITION_NONE indicates that query should not be rejected.
	// QUERY_REJECT_CONDITION_NOT_OPEN indicates that query should be rejected if workflow is not open.
	// QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY indicates that query should be rejected if workflow did not complete cleanly (e.g. terminated, canceled timeout etc...).
	QueryRejectCondition enumspb.QueryRejectCondition

	// Header is an optional header to include with the query.
	Header *commonpb.Header
}

// QueryWorkflowWithOptionsResponse is the response to QueryWorkflowWithOptions
type QueryWorkflowWithOptionsResponse struct {
	// QueryResult contains the result of executing the query.
	// This will only be set if the query was completed successfully and not rejected.
	QueryResult converter.EncodedValue

	// QueryRejected contains information about the query rejection.
	QueryRejected *querypb.QueryRejected
}

// WorkflowExecutionMetadata contains common information about a workflow execution.
type WorkflowExecutionMetadata struct {
	// WorkflowExecution is the unique identifier for the workflow execution
	WorkflowExecution WorkflowExecution
	// WorkflowType is the type of the workflow execution
	WorkflowType WorkflowType
	// TaskQueueName is the name of the task queue
	TaskQueueName string
	// Status is the status of the workflow execution
	Status enumspb.WorkflowExecutionStatus
	// Memo is the current memo of the workflow execution
	// Values can be decoded using data converter (defaultDataConverter, or custom one if set).
	Memo *commonpb.Memo
	// TypedSearchAttributes is the current search attributes of the workflow execution
	TypedSearchAttributes SearchAttributes
	// ParentWorkflowExecution is the parent workflow execution
	// This field is only set if the workflow execution is a child of another workflow execution
	ParentWorkflowExecution *WorkflowExecution
	// RootWorkflowExecution is the root workflow execution
	RootWorkflowExecution *WorkflowExecution
	// WorkflowStartTime is the time when the workflow execution started
	WorkflowStartTime time.Time
	// WorkflowCloseTime is the time when the workflow execution closed
	// This field is only set if the workflow execution is closed
	WorkflowCloseTime *time.Time
	// ExecutionTime is the time when the workflow execution started or should start
	ExecutionTime *time.Time
	// HistoryLength is the number of history events in the workflow execution
	HistoryLength int
}

// WorkflowExecutionDescription defines the response to DescribeWorkflow.
type WorkflowExecutionDescription struct {
	WorkflowExecutionMetadata
	dc                    converter.DataConverter
	inboundPayloadVisitor PayloadVisitor
	staticSummaryPayload  *commonpb.Payload
	staticDetailsPayload  *commonpb.Payload
	staticSummary         string
	staticDetails         string
}

// GetStaticSummary returns the summary set on workflow start.
//
// NOTE: Experimental
func (w *WorkflowExecutionDescription) GetStaticSummary() (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// GetStaticDetails returns the details set on workflow start.
//
// NOTE: Experimental
func (w *WorkflowExecutionDescription) GetStaticDetails() (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// GetMemoValue decodes a memo value by key into valuePtr.
// Returns ErrNoData if the memo is nil or the key is not present.
//
// NOTE: Experimental
func (w *WorkflowExecutionDescription) GetMemoValue(key string, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// QueryWorkflowWithOptions queries a given workflow execution and returns the query result synchronously.
// See QueryWorkflowWithOptionsRequest and QueryWorkflowWithOptionsResult for more information.
// The errors it can return:
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
//   - serviceerror.NotFound
//   - serviceerror.QueryFailed
func (wc *WorkflowClient) QueryWorkflowWithOptions(ctx context.Context, request *QueryWorkflowWithOptionsRequest) (*QueryWorkflowWithOptionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Set header before interceptor run

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes.
//   - taskqueue name of taskqueue
//   - taskqueueType type of taskqueue, can be workflow or activity
//
// The errors it can return:
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
//   - serviceerror.NotFound
func (wc *WorkflowClient) DescribeTaskQueue(ctx context.Context, taskQueue string, taskQueueType enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ResetWorkflowExecution reset an existing workflow execution to WorkflowTaskFinishEventId(exclusive).
// And it will immediately terminating the current execution instance.
// RequestId is used to deduplicate requests. It will be autogenerated if not set.
func (wc *WorkflowClient) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// UpdateWorkerBuildIdCompatibility allows you to update the worker-build-id based version sets for a particular
// task queue. This is used in conjunction with workers who specify their build id and thus opt into the
// feature.
func (wc *WorkflowClient) UpdateWorkerBuildIdCompatibility(ctx context.Context, options *UpdateWorkerBuildIdCompatibilityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// GetWorkerBuildIdCompatibility returns the worker-build-id based version sets for a particular task queue.
func (wc *WorkflowClient) GetWorkerBuildIdCompatibility(ctx context.Context, options *GetWorkerBuildIdCompatibilityOptions) (*WorkerBuildIDVersionSets, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkerTaskReachability returns which versions are is still in use by open or closed workflows.
func (wc *WorkflowClient) GetWorkerTaskReachability(ctx context.Context, options *GetWorkerTaskReachabilityOptions) (*WorkerTaskReachability, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// UpdateWorkflowExecutionOptions partially overrides the [WorkflowExecutionOptions] of an existing workflow execution,
// and returns the new [WorkflowExecutionOptions] after applying the changes.
// It is intended for building tools that can selectively apply ad-hoc workflow configuration changes.
//
// NOTE: Experimental
func (wc *WorkflowClient) UpdateWorkflowExecutionOptions(ctx context.Context, request UpdateWorkflowExecutionOptionsRequest) (WorkflowExecutionOptions, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowExecutionOptions), nil
}

// DescribeTaskQueueEnhanced returns information about the target task queue, broken down by Build Id:
//   - List of pollers
//   - Workflow Reachability status
//   - Backlog info for Workflow and/or Activity tasks
//
// WARNING: Worker versioning is currently experimental, and requires server 1.24+
func (wc *WorkflowClient) DescribeTaskQueueEnhanced(ctx context.Context, options DescribeTaskQueueEnhancedOptions) (TaskQueueDescription, error) {
	_ = "STUB: not implemented"
	return *new(TaskQueueDescription), nil
}

// UpdateWorkerVersioningRules allows updating the worker-build-id based assignment and redirect rules for a given
// task queue. This is used in conjunction with workers who specify their build id and thus opt into the feature.
// The errors it can return:
//   - serviceerror.FailedPrecondition when the conflict token is invalid
func (wc *WorkflowClient) UpdateWorkerVersioningRules(ctx context.Context, options UpdateWorkerVersioningRulesOptions) (*WorkerVersioningRules, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkerVersioningRules returns the worker-build-id assignment and redirect rules for a task queue.
func (wc *WorkflowClient) GetWorkerVersioningRules(ctx context.Context, options GetWorkerVersioningOptions) (*WorkerVersioningRules, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *WorkflowClient) GetWorkflowUpdateHandle(ref GetWorkflowUpdateHandleOptions) WorkflowUpdateHandle {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle)
}

// PollWorkflowUpdate sends a request for the outcome of the specified update
// through the interceptor chain.
func (wc *WorkflowClient) PollWorkflowUpdate(
	ctx context.Context,
	ref *updatepb.UpdateRef,
) (*ClientPollWorkflowUpdateOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *WorkflowClient) UpdateWorkflow(
	ctx context.Context,
	options UpdateWorkflowOptions,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

func (wc *WorkflowClient) UpdateWithStartWorkflow(
	ctx context.Context,
	options UpdateWithStartWorkflowOptions,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// CheckHealthRequest is a request for Client.CheckHealth.
type CheckHealthRequest struct{}

// CheckHealthResponse is a response for Client.CheckHealth.
type CheckHealthResponse struct{}

// CheckHealth performs a server health check using the gRPC health check
// API. If the check fails, an error is returned.
func (wc *WorkflowClient) CheckHealth(ctx context.Context, request *CheckHealthRequest) (*CheckHealthResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Ignore request/response for now, they are empty

// WorkflowService implements Client.WorkflowService.
func (wc *WorkflowClient) WorkflowService() workflowservice.WorkflowServiceClient {
	_ = "STUB: not implemented"
	return *new(workflowservice.WorkflowServiceClient)
}

// OperatorService implements Client.OperatorService.
func (wc *WorkflowClient) OperatorService() operatorservice.OperatorServiceClient {
	_ = "STUB: not implemented"
	return *new(operatorservice.OperatorServiceClient)
}

// Get capabilities, lazily fetching from server if not already obtained.
func (wc *WorkflowClient) loadCapabilities(ctx context.Context) (*workflowservice.GetSystemInfoResponse_Capabilities, error) {
	_ = "STUB: not implemented"
	// While we want to memoize the result here, we take care not to lock during
	// the call. This means that in racy situations where this is called multiple
	// times at once, it may result in multiple calls. This is far more preferable
	// than locking on the call itself.
	return nil, nil
}

// Fetch the capabilities

// We ignore unimplemented

// Store and return. We intentionally don't check if we're overwriting as we
// accept last-success-wins.

// Also set whether we exclude internal from retry

// Get namespace capabilities, lazily fetching from server if not already obtained.
func (wc *WorkflowClient) loadNamespaceData(metricsHandler metrics.Handler) (namespaceData, error) {
	_ = "STUB: not implemented"
	return *new(namespaceData), nil
}

func (wc *WorkflowClient) ensureInitialized(ctx context.Context) error {
	_ = "STUB: not implemented"
	// Just loading the capabilities is enough
	return nil
}

// ScheduleClient implements Client.ScheduleClient.
func (wc *WorkflowClient) ScheduleClient() ScheduleClient {
	_ = "STUB: not implemented"
	return *new(ScheduleClient)
}

// DeploymentClient implements [Client.DeploymentClient].
func (wc *WorkflowClient) DeploymentClient() DeploymentClient {
	_ = "STUB: not implemented"
	return *new(DeploymentClient)
}

// WorkerDeploymentClient implements [Client.WorkerDeploymentClient].
func (wc *WorkflowClient) WorkerDeploymentClient() WorkerDeploymentClient {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentClient)
}

func (wc *WorkflowClient) recordWorkerHeartbeat(ctx context.Context, request *workflowservice.RecordWorkerHeartbeatRequest) (*workflowservice.RecordWorkerHeartbeatResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Close client and clean up underlying resources.
func (wc *WorkflowClient) Close() {
	_ = "STUB: not implemented"
	// If there's a set of unclosed clients, we have to decrement it and then
	// set it to a new pointer of max to prevent decrementing on repeated Close
	// calls to this client. If the count has not reached zero, this close call is
	// ignored.
	return
}

// Set the unclosed clients to max value so we never try this again

// If there are any remaining, do not close

func (wc *WorkflowClient) newOutboundPayloadVisitor() PayloadVisitor {
	_ = "STUB: not implemented"
	return *new(PayloadVisitor)
}

// Register a namespace with temporal server
// The errors it can throw:
//   - NamespaceAlreadyExistsError
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
func (nc *namespaceClient) Register(ctx context.Context, request *workflowservice.RegisterNamespaceRequest) error {
	_ = "STUB: not implemented"
	return nil
}

// Describe a namespace. The namespace has 3 part of information
// NamespaceInfo - Which has Name, Status, Description, Owner Email
// NamespaceConfiguration - Configuration like Workflow Execution Retention Period In Days, Whether to emit metrics.
// ReplicationConfiguration - replication config like clusters and active cluster name
// The errors it can throw:
//   - serviceerror.NamespaceNotFound
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
func (nc *namespaceClient) Describe(ctx context.Context, namespace string) (*workflowservice.DescribeNamespaceResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Update a namespace.
// The errors it can throw:
//   - serviceerror.NamespaceNotFound
//   - serviceerror.InvalidArgument
//   - serviceerror.Internal
//   - serviceerror.Unavailable
func (nc *namespaceClient) Update(ctx context.Context, request *workflowservice.UpdateNamespaceRequest) error {
	_ = "STUB: not implemented"
	return nil
}

// Close client and clean up underlying resources.
func (nc *namespaceClient) Close() { _ = "STUB: not implemented"; return }

func (iter *historyEventIteratorImpl) HasNext() bool { _ = "STUB: not implemented"; return false }

// Next returns the next history event.
// If next is called with not more events, it will panic.
// Call [historyEventIteratorImpl.HasNext] to check if there are more events.
func (iter *historyEventIteratorImpl) Next() (*historypb.HistoryEvent, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// we have cached events

// we have err, clear that iter.err and return err

func (workflowRun *workflowRunImpl) GetRunID() string { _ = "STUB: not implemented"; return "" }

func (workflowRun *workflowRunImpl) GetID() string { _ = "STUB: not implemented"; return "" }

func (workflowRun *workflowRunImpl) Get(ctx context.Context, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (workflowRun *workflowRunImpl) GetWithOptions(
	ctx context.Context,
	valuePtr interface{},
	options WorkflowRunGetOptions,
) error {
	_ = "STUB: not implemented"
	return nil
}

// follow is used by Get to follow a chain of executions linked by NewExecutionRunId, so that Get
// doesn't return until the chain finishes. These can be ContinuedAsNew events, Completed events
// (for workflows with a cron schedule), or Failed or TimedOut events (for workflows with a retry
// policy or cron schedule).
func (workflowRun *workflowRunImpl) follow(
	ctx context.Context,
	valuePtr interface{},
	newRunID string,
	options WorkflowRunGetOptions,
) error {
	_ = "STUB: not implemented"
	return nil
}

// encodeMemoValue encodes a single memo value. useUserDC controls whether the user's data converter
// is attempted first. Client-side callers should pass sdkFlagsAllowed[SDKFlagMemoUserDCEncode];
// workflow-side callers should pass the result of TryUse(SDKFlagMemoUserDCEncode) for replay safety.
func encodeMemoValue(value interface{}, dc converter.DataConverter, useUserDC bool) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// If fallback default data converter fails, return original user data converter error

// getWorkflowMemo encodes a memo map into a proto Memo. useUserDC controls whether the user's
// data converter is attempted first. Client-side callers should pass sdkFlagsAllowed[SDKFlagMemoUserDCEncode];
// workflow-side callers should pass the result of TryUse(SDKFlagMemoUserDCEncode) for replay safety.
func getWorkflowMemo(input map[string]interface{}, dc converter.DataConverter, useUserDC bool) (*commonpb.Memo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type workflowClientInterceptor struct {
	client                 *WorkflowClient
	inboundPayloadVisitor  PayloadVisitor
	outboundPayloadVisitor PayloadVisitor
}

func createStartWorkflowInput(
	options StartWorkflowOptions,
	workflow interface{},
	args []interface{},
	registry *registry,
) (*ClientExecuteWorkflowInput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (w *workflowClientInterceptor) createStartWorkflowRequest(
	ctx context.Context,
	in *ClientExecuteWorkflowInput,
) (*workflowservice.StartWorkflowExecutionRequest, error) {
	_ = "STUB: not implemented"
	// This is always set before interceptor is invoked
	return nil, nil
}

// Encode input

// get workflow headers from the context

// run propagators to extract information about tracing and other stuff, store in headers field

func (w *workflowClientInterceptor) ExecuteWorkflow(
	ctx context.Context,
	in *ClientExecuteWorkflowInput,
) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// Allow already-started error

func (w *workflowClientInterceptor) UpdateWithStartWorkflow(
	ctx context.Context,
	in *ClientUpdateWithStartWorkflowInput,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// Create start request

// Create update request

// Perform update-with-start using the MultiOperation API. As with
// UpdateWorkflow, we issue the request repeatedly until the update is durable.
// The `onStart` callback is called once, the first time that a valid start
// response is received.
func (w *workflowClientInterceptor) updateWithStartWorkflow(
	ctx context.Context,
	startRequest *workflowservice.StartWorkflowExecutionRequest,
	updateRequest *workflowservice.UpdateWorkflowExecutionRequest,
	onStart func(*workflowservice.StartWorkflowExecutionResponse),
	rpcMetricsHandler metrics.Handler,
) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// if an operation error is of type MultiOperationAborted, it means it was only aborted because
// of another operation's error and is therefore not interesting or helpful

// this would only happen if a case statement for a newly added operation is missing above

// this should never happen

// this would only happen if a case statement for a newly added operation is missing above

func (w *workflowClientInterceptor) SignalWorkflow(ctx context.Context, in *ClientSignalWorkflowInput) error {
	_ = "STUB: not implemented"
	return nil
}

// get workflow headers from the context

func (w *workflowClientInterceptor) SignalWithStartWorkflow(
	ctx context.Context,
	in *ClientSignalWithStartWorkflowInput,
) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// Encode input

// get workflow headers from the context

// Start creating workflow request.

func (w *workflowClientInterceptor) CancelWorkflow(ctx context.Context, in *ClientCancelWorkflowInput) error {
	_ = "STUB: not implemented"
	return nil
}

func (w *workflowClientInterceptor) TerminateWorkflow(ctx context.Context, in *ClientTerminateWorkflowInput) error {
	_ = "STUB: not implemented"
	return nil
}

func (w *workflowClientInterceptor) DescribeWorkflow(
	ctx context.Context,
	in *ClientDescribeWorkflowInput,
) (*ClientDescribeWorkflowOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (w *workflowClientInterceptor) QueryWorkflow(
	ctx context.Context,
	in *ClientQueryWorkflowInput,
) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// get workflow headers from the context

func (w *workflowClientInterceptor) UpdateWorkflow(
	ctx context.Context,
	in *ClientUpdateWorkflowInput,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// Here we know the update is at least accepted

func (w *workflowClientInterceptor) updateIsDurable(resp *workflowservice.UpdateWorkflowExecutionResponse) bool {
	_ = "STUB: not implemented"
	// Once the update is past admitted we know it is durable
	// Note: old server version may return UNSPECIFIED if the update request
	// did not reach the desired lifecycle stage.
	return false
}

func createUpdateWorkflowInput(options *UpdateWorkflowOptions) (*ClientUpdateWorkflowInput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (w *workflowClientInterceptor) createUpdateWorkflowRequest(
	ctx context.Context,
	in *ClientUpdateWorkflowInput,
) (*workflowservice.UpdateWorkflowExecutionRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (w *workflowClientInterceptor) PollWorkflowUpdate(
	parentCtx context.Context,
	in *ClientPollWorkflowUpdateInput,
) (*ClientPollWorkflowUpdateOutput, error) {
	_ = "STUB: not implemented"
	// header, _ = headerPropagated(ctx, w.client.contextPropagators)
	// todo header not in PollWorkflowUpdate
	return nil, nil
}

// Required to implement ClientOutboundInterceptor
func (*workflowClientInterceptor) mustEmbedClientOutboundInterceptorBase() {
	_ = "STUB: not implemented"
	return
}

func (w *workflowClientInterceptor) updateHandleFromResponse(
	ctx context.Context,
	desiredLifecycleStage enumspb.UpdateWorkflowExecutionLifecycleStage,
	resp *workflowservice.UpdateWorkflowExecutionResponse,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// TODO(https://github.com/temporalio/features/issues/428) replace with handle wait for stage once implemented

func (uh *baseUpdateHandle) WorkflowID() string { _ = "STUB: not implemented"; return "" }

func (uh *baseUpdateHandle) RunID() string { _ = "STUB: not implemented"; return "" }

func (uh *baseUpdateHandle) UpdateID() string { _ = "STUB: not implemented"; return "" }

func (ch *completedUpdateHandle) Get(ctx context.Context, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (luh *lazyUpdateHandle) Get(ctx context.Context, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (q *QueryRejectedError) QueryRejected() *querypb.QueryRejected {
	_ = "STUB: not implemented"
	return nil
}

func (q *QueryRejectedError) Error() string { _ = "STUB: not implemented"; return "" }

func buildUserMetadata(
	summary string,
	details string,
	dataConverter converter.DataConverter,
) (*sdk.UserMetadata, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
