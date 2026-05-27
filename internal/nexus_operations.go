package internal

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

// NexusOperationInfo contains information about a currently executing Nexus operation.
//
// Exposed as: [go.temporal.io/sdk/temporalnexus.OperationInfo]
type NexusOperationInfo struct {
	// The namespace of the worker handling this Nexus operation.
	Namespace string
	// The task queue of the worker handling this Nexus operation.
	TaskQueue string
	// The endpoint this request was addressed to before forwarding to the worker.
	// Supported from server version 1.30.0.
	Endpoint string
}

// NexusOperationContext is an internal only struct that holds fields used by the temporalnexus functions.
type NexusOperationContext struct {
	client         Client
	Namespace      string
	TaskQueue      string
	Endpoint       string
	metricsHandler metrics.Handler
	log            log.Logger
	registry       *registry
}

func (nc *NexusOperationContext) ResolveWorkflowName(wf any) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

type nexusOperationEnvironment struct {
	NexusOperationOutboundInterceptorBase
}

func (nc *nexusOperationEnvironment) GetOperationInfo(ctx context.Context) NexusOperationInfo {
	_ = "STUB: not implemented"
	return *new(NexusOperationInfo)
}

func (nc *nexusOperationEnvironment) GetMetricsHandler(ctx context.Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// GetLogger returns a logger to be used in a Nexus operation's context.
func (nc *nexusOperationEnvironment) GetLogger(ctx context.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

// GetClient returns a client to be used in a Nexus operation's context, this is the same client that the worker was
// created with. Client methods will panic when called from the test environment.
func (nc *nexusOperationEnvironment) GetClient(ctx context.Context) Client {
	_ = "STUB: not implemented"
	return *new(Client)
}

type nexusOperationOutboundInterceptorKeyType struct{}

// nexusOperationOutboundInterceptorKey is a key for associating a [NexusOperationOutboundInterceptor] with a [context.Context].
var nexusOperationOutboundInterceptorKey = nexusOperationOutboundInterceptorKeyType{}

// nexusOperationOutboundInterceptorFromGoContext gets the [NexusOperationOutboundInterceptor] associated with the given [context.Context].
func nexusOperationOutboundInterceptorFromGoContext(ctx context.Context) (nctx NexusOperationOutboundInterceptor, ok bool) {
	_ = "STUB: not implemented"
	return *new(NexusOperationOutboundInterceptor), false
}

// IsNexusOperation checks if the provided context is a Nexus operation context.
//
// Exposed as: [go.temporal.io/sdk/temporalnexus.IsNexusOperation]
func IsNexusOperation(ctx context.Context) bool { _ = "STUB: not implemented"; return false }

// GetNexusOperationInfo returns information about the currently executing Nexus operation.
//
// Exposed as: [go.temporal.io/sdk/temporalnexus.GetOperationInfo]
func GetNexusOperationInfo(ctx context.Context) NexusOperationInfo {
	_ = "STUB: not implemented"
	return *new(NexusOperationInfo)
}

// GetNexusOperationMetricsHandler returns a metrics handler to be used in a Nexus operation's context.
//
// Exposed as: [go.temporal.io/sdk/temporalnexus.GetMetricsHandler]
func GetNexusOperationMetricsHandler(ctx context.Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// GetNexusOperationLogger returns a logger to be used in a Nexus operation's context.
//
// Exposed as: [go.temporal.io/sdk/temporalnexus.GetLogger]
func GetNexusOperationLogger(ctx context.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

// GetNexusOperationClient returns a client to be used in a Nexus operation's context, this is the same client that the
// worker was created with. Client methods will panic when called from the test environment.
//
// Exposed as: [go.temporal.io/sdk/temporalnexus.GetClient]
func GetNexusOperationClient(ctx context.Context) Client {
	_ = "STUB: not implemented"
	return *new(Client)
}

type nexusOperationContextKeyType struct{}

// nexusOperationContextKey is a key for associating a [NexusOperationContext] with a [context.Context].
var nexusOperationContextKey = nexusOperationContextKeyType{}

type isWorkflowRunOpContextKeyType struct{}

// IsWorkflowRunOpContextKey is a key to mark that the current context is used within a workflow run operation.
// The fake test env client verifies this key is set on the context to decide whether it should execute a method or
// panic as we don't want to expose a partial client to sync operations.
var IsWorkflowRunOpContextKey = isWorkflowRunOpContextKeyType{}

type nexusOperationRequestIDKeyType struct{}

var NexusOperationRequestIDKey = nexusOperationRequestIDKeyType{}

type nexusOperationLinksKeyType struct{}

var NexusOperationLinksKey = nexusOperationLinksKeyType{}

// NexusOperationContextFromGoContext gets the [NexusOperationContext] associated with the given [context.Context].
func NexusOperationContextFromGoContext(ctx context.Context) (nctx *NexusOperationContext, ok bool) {
	_ = "STUB: not implemented"
	return nil, false
}

// nexusMiddleware constructs an adapter from Temporal WorkerInterceptors to a Nexus MiddlewareFunc.
func nexusMiddleware(interceptors []WorkerInterceptor) nexus.MiddlewareFunc {
	_ = "STUB: not implemented"
	return *new(nexus.MiddlewareFunc)
}

// nexusMiddlewareToInterceptorAdapter is an adapter from the Nexus Handler interface to the Temporal interceptor interface.
type nexusMiddlewareToInterceptorAdapter struct {
	nexus.UnimplementedOperation[any, any]
	inboundInterceptor  NexusOperationInboundInterceptor
	outboundInterceptor NexusOperationOutboundInterceptor
}

func newNexusHandler(inbound NexusOperationInboundInterceptor, outbound NexusOperationOutboundInterceptor) nexus.OperationHandler[any, any] {
	_ = "STUB: not implemented"
	return nil
}

func (h *nexusMiddlewareToInterceptorAdapter) Start(ctx context.Context, input any, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (h *nexusMiddlewareToInterceptorAdapter) Cancel(ctx context.Context, token string, options nexus.CancelOperationOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// nexusInterceptorToMiddlewareAdapter is an adapter from the Temporal interceptor interface to the Nexus Handler interface.
type nexusInterceptorToMiddlewareAdapter struct {
	NexusOperationInboundInterceptorBase
	handler             nexus.OperationHandler[any, any]
	outboundInterceptor NexusOperationOutboundInterceptor
}

// CancelOperation implements NexusOperationInboundInterceptor.
func (n *nexusInterceptorToMiddlewareAdapter) CancelOperation(ctx context.Context, input NexusCancelOperationInput) error {
	_ = "STUB: not implemented"
	return nil
}

// Init implements NexusOperationInboundInterceptor.
func (n *nexusInterceptorToMiddlewareAdapter) Init(ctx context.Context, outbound NexusOperationOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

// StartOperation implements NexusOperationInboundInterceptor.
func (n *nexusInterceptorToMiddlewareAdapter) StartOperation(ctx context.Context, input NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// Most of the helpers in this section were duplicated from the server codebase at common/nexus/failure.go.
///////////////////////////////////////////////////////////////////////////////////////////////////////////

var failureTypeString = string((&failurepb.Failure{}).ProtoReflect().Descriptor().FullName())

// ProtoFailureToNexusFailure converts a proto Nexus Failure to a Nexus SDK Failure.
func protoFailureToNexusFailure(failure *nexuspb.Failure) nexus.Failure {
	_ = "STUB: not implemented"
	return *new(nexus.Failure)
}

// nexusOperationFailure is a utility in use by the test environment.
func nexusOperationFailure(params ExecuteNexusOperationParams, token string, cause *failurepb.Failure) *failurepb.Failure {
	_ = "STUB: not implemented"
	return nil
}

// Also populate ID for backwards compatibility.

// temporalFailureToNexusFailure converts an API proto Failure to a Nexus SDK Failure setting the metadata "type" field to
// the proto fullname of the temporal API Failure message or the standard Nexus SDK failure types.
// Returns an error if the failure cannot be converted.
func temporalFailureToNexusFailure(failure *failurepb.Failure) (*nexus.Failure, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// nexusFailureToTemporalFailure converts a Nexus Failure to an API proto Failure.
// If the failure metadata "type" field is set to the fullname of the temporal API Failure message, the failure is
// reconstructed using protojson.Unmarshal on the failure details field.
func nexusFailureToTemporalFailure(failure nexus.Failure, retryable bool) (*failurepb.Failure, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Make up a type here, it's not part of the Nexus Failure spec.

// Ensure this always gets written.

func nexusFailureMetadataToPayloads(failure nexus.Failure) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Delete before serializing.

func apiOperationErrorToNexusOperationError(opErr *nexuspb.UnsuccessfulOperationError) *nexus.OperationError {
	_ = "STUB: not implemented"
	return nil
}

func apiHandlerErrorToNexusHandlerError(apiErr *nexuspb.HandlerError, failureConverter converter.FailureConverter) (*nexus.HandlerError, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// nolint:exhaustive // unspecified is the default

func operationErrorToTemporalFailure(opErr *nexus.OperationError) (*failurepb.Failure, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Canceled must be translated into a CanceledFailure to match the SDK expectation.

// We already have a CanceledFailure, use it.

// Fallback to encoding the Nexus failure into a Temporal canceled failure, we expect operations that end up
// as canceled to have a CanceledFailureInfo object.

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// END Nexus failure section.
///////////////////////////////////////////////////////////////////////////////////////////////////////////

// testSuiteClientForNexusOperations is a partial [Client] implementation for the test workflow environment used to
// support running the workflow run operation - and only this operation, all methods will panic when this client is
// passed to sync operations.
type testSuiteClientForNexusOperations struct {
	env *testWorkflowEnvironmentImpl
}

// DescribeWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) DescribeWorkflow(ctx context.Context, workflowID string, runID string) (*WorkflowExecutionDescription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// CancelWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	_ = "STUB: not implemented"
	return nil
}

// CheckHealth implements Client.
func (t *testSuiteClientForNexusOperations) CheckHealth(ctx context.Context, request *CheckHealthRequest) (*CheckHealthResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Close implements Client.
func (t *testSuiteClientForNexusOperations) Close() {
	_ = "STUB: not implemented"
	// No op.

	// CompleteActivity implements Client.
	return
}

func (t *testSuiteClientForNexusOperations) CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityWithOptions implements Client.
func (t *testSuiteClientForNexusOperations) CompleteActivityWithOptions(ctx context.Context, opts CompleteActivityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityByID implements Client.
func (t *testSuiteClientForNexusOperations) CompleteActivityByID(ctx context.Context, namespace string, workflowID string, runID string, activityID string, result interface{}, err error) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityByIDWithOptions implements Client.
func (t *testSuiteClientForNexusOperations) CompleteActivityByIDWithOptions(ctx context.Context, opts CompleteActivityByIDOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityByActivityID implements Client.
func (t *testSuiteClientForNexusOperations) CompleteActivityByActivityID(ctx context.Context, namespace string, activityID string, activityRunID string, result interface{}, err error) error {
	_ = "STUB: not implemented"
	return nil
}

// CompleteActivityByActivityIDWithOptions implements Client.
func (t *testSuiteClientForNexusOperations) CompleteActivityByActivityIDWithOptions(ctx context.Context, opts CompleteActivityByActivityIDOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// CountWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// DescribeTaskQueue implements Client.
func (t *testSuiteClientForNexusOperations) DescribeTaskQueue(ctx context.Context, taskqueue string, taskqueueType enums.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// DescribeTaskQueueEnhanced implements Client.
func (t *testSuiteClientForNexusOperations) DescribeTaskQueueEnhanced(ctx context.Context, options DescribeTaskQueueEnhancedOptions) (TaskQueueDescription, error) {
	_ = "STUB: not implemented"
	return *new(TaskQueueDescription), nil
}

// DescribeWorkflowExecution implements Client.
func (t *testSuiteClientForNexusOperations) DescribeWorkflowExecution(ctx context.Context, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ExecuteWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// Not propagating Header as this client does not support context propagation.

// This callback handles async completion of Nexus operations. If there was an error when
// starting the workflow, then the operation failed synchronously and this callback doesn't
// need to be executed.

// Send the operation token to account for a race when the completion comes in before the response to the
// StartOperation call is recorded.
// The token is extracted from the callback header which is attached in ExecuteUntypedWorkflow.

func (t *testSuiteClientForNexusOperations) NewWithStartWorkflowOperation(options StartWorkflowOptions, workflow interface{}, args ...interface{}) WithStartWorkflowOperation {
	_ = "STUB: not implemented"
	return *new(WithStartWorkflowOperation)
}

// GetSearchAttributes implements Client.
func (t *testSuiteClientForNexusOperations) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkerBuildIdCompatibility implements Client.
func (t *testSuiteClientForNexusOperations) GetWorkerBuildIdCompatibility(ctx context.Context, options *GetWorkerBuildIdCompatibilityOptions) (*WorkerBuildIDVersionSets, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkerTaskReachability implements Client.
func (t *testSuiteClientForNexusOperations) GetWorkerTaskReachability(ctx context.Context, options *GetWorkerTaskReachabilityOptions) (*WorkerTaskReachability, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkerVersioningRules implements Client.
func (t *testSuiteClientForNexusOperations) GetWorkerVersioningRules(ctx context.Context, options GetWorkerVersioningOptions) (*WorkerVersioningRules, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) GetWorkflow(ctx context.Context, workflowID string, runID string) WorkflowRun {
	_ = "STUB: not implemented"
	return *new(WorkflowRun)
}

// GetWorkflowHistory implements Client.
func (t *testSuiteClientForNexusOperations) GetWorkflowHistory(ctx context.Context, workflowID string, runID string, isLongPoll bool, filterType enums.HistoryEventFilterType) HistoryEventIterator {
	_ = "STUB: not implemented"
	return *new(HistoryEventIterator)
}

// GetWorkflowUpdateHandle implements Client.
func (t *testSuiteClientForNexusOperations) GetWorkflowUpdateHandle(GetWorkflowUpdateHandleOptions) WorkflowUpdateHandle {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle)
}

// ListArchivedWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ListClosedWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ListOpenWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ListWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// OperatorService implements Client.
func (t *testSuiteClientForNexusOperations) OperatorService() operatorservice.OperatorServiceClient {
	_ = "STUB: not implemented"
	return *new(operatorservice.OperatorServiceClient)
}

// QueryWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// QueryWorkflowWithOptions implements Client.
func (t *testSuiteClientForNexusOperations) QueryWorkflowWithOptions(ctx context.Context, request *QueryWorkflowWithOptionsRequest) (*QueryWorkflowWithOptionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// RecordActivityHeartbeat implements Client.
func (t *testSuiteClientForNexusOperations) RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// RecordActivityHeartbeatWithOptions implements Client.
func (t *testSuiteClientForNexusOperations) RecordActivityHeartbeatWithOptions(ctx context.Context, opts RecordActivityHeartbeatOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// RecordActivityHeartbeatByID implements Client.
func (t *testSuiteClientForNexusOperations) RecordActivityHeartbeatByID(ctx context.Context, namespace string, workflowID string, runID string, activityID string, details ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// RecordActivityHeartbeatByIDWithOptions implements Client.
func (t *testSuiteClientForNexusOperations) RecordActivityHeartbeatByIDWithOptions(ctx context.Context, opts RecordActivityHeartbeatByIDOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// ResetWorkflowExecution implements Client.
func (t *testSuiteClientForNexusOperations) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ScanWorkflow implements Client.
//
//lint:ignore SA1019 the server API was deprecated.
func (t *testSuiteClientForNexusOperations) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ScheduleClient implements Client.
func (t *testSuiteClientForNexusOperations) ScheduleClient() ScheduleClient {
	_ = "STUB: not implemented"
	return *new(ScheduleClient)
}

// SignalWithStartWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{}, options StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// SignalWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// TerminateWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// UpdateWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) UpdateWorkflow(ctx context.Context, options UpdateWorkflowOptions) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// UpdateWithStartWorkflow implements Client.
func (t *testSuiteClientForNexusOperations) UpdateWithStartWorkflow(ctx context.Context, options UpdateWithStartWorkflowOptions) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// UpdateWorkerBuildIdCompatibility implements Client.
func (t *testSuiteClientForNexusOperations) UpdateWorkerBuildIdCompatibility(ctx context.Context, options *UpdateWorkerBuildIdCompatibilityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// UpdateWorkerVersioningRules implements Client.
func (t *testSuiteClientForNexusOperations) UpdateWorkerVersioningRules(ctx context.Context, options UpdateWorkerVersioningRulesOptions) (*WorkerVersioningRules, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (t *testSuiteClientForNexusOperations) ExecuteActivity(ctx context.Context, options ClientStartActivityOptions, activity any, args ...any) (ClientActivityHandle, error) {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle), nil
}

func (t *testSuiteClientForNexusOperations) GetActivityHandle(options ClientGetActivityHandleOptions) ClientActivityHandle {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle)
}

func (t *testSuiteClientForNexusOperations) ListActivities(ctx context.Context, options ClientListActivitiesOptions) (ClientListActivitiesResult, error) {
	_ = "STUB: not implemented"
	return *new(ClientListActivitiesResult), nil
}

func (t *testSuiteClientForNexusOperations) CountActivities(ctx context.Context, options ClientCountActivitiesOptions) (*ClientCountActivitiesResult, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// WorkflowService implements Client.
func (t *testSuiteClientForNexusOperations) WorkflowService() workflowservice.WorkflowServiceClient {
	_ = "STUB: not implemented"
	return *new(workflowservice.WorkflowServiceClient)
}

// DeploymentClient implements Client.
func (t *testSuiteClientForNexusOperations) DeploymentClient() DeploymentClient {
	_ = "STUB: not implemented"
	return *new(DeploymentClient)
}

// WorkerDeploymentClient implements Client.
func (t *testSuiteClientForNexusOperations) WorkerDeploymentClient() WorkerDeploymentClient {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentClient)
}

// UpdateWorkflowExecutionOptions implements Client.
func (t *testSuiteClientForNexusOperations) UpdateWorkflowExecutionOptions(ctx context.Context, options UpdateWorkflowExecutionOptionsRequest) (WorkflowExecutionOptions, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowExecutionOptions), nil
}

func (t *testSuiteClientForNexusOperations) NewNexusClient(options ClientNexusClientOptions) (ClientNexusClient, error) {
	_ = "STUB: not implemented"
	return *new(ClientNexusClient), nil
}

func (t *testSuiteClientForNexusOperations) GetNexusOperationHandle(options ClientGetNexusOperationHandleOptions) ClientNexusOperationHandle {
	_ = "STUB: not implemented"
	return *new(ClientNexusOperationHandle)
}

func (t *testSuiteClientForNexusOperations) ListNexusOperations(ctx context.Context, options ClientListNexusOperationsOptions) (ClientListNexusOperationsResult, error) {
	_ = "STUB: not implemented"
	return *new(ClientListNexusOperationsResult), nil
}

func (t *testSuiteClientForNexusOperations) CountNexusOperations(ctx context.Context, options ClientCountNexusOperationsOptions) (*ClientCountNexusOperationsResult, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

var _ Client = &testSuiteClientForNexusOperations{}

// testEnvWorkflowRunForNexusOperations is a partial [WorkflowRun] implementation for the test workflow environment used
// to support basic Nexus functionality.
type testEnvWorkflowRunForNexusOperations struct {
	WorkflowExecution
}

// Get implements WorkflowRun.
func (t *testEnvWorkflowRunForNexusOperations) Get(ctx context.Context, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// GetID implements WorkflowRun.
func (t *testEnvWorkflowRunForNexusOperations) GetID() string {
	_ = "STUB: not implemented"

	// GetRunID implements WorkflowRun.
	return ""
}

func (t *testEnvWorkflowRunForNexusOperations) GetRunID() string {
	_ = "STUB: not implemented"

	// GetWithOptions implements WorkflowRun.
	return ""
}

func (t *testEnvWorkflowRunForNexusOperations) GetWithOptions(ctx context.Context, valuePtr interface{}, options WorkflowRunGetOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// Exposed as: [go.temporal.io/sdk/client.WorkflowRun]
var _ WorkflowRun = &testEnvWorkflowRunForNexusOperations{}
