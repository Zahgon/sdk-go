package internal

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

// InterceptorBase is a default implementation of Interceptor meant for
// embedding. See documentation in the interceptor package for more details.
//
// Exposed as: [go.temporal.io/sdk/interceptor.InterceptorBase]
type InterceptorBase struct {
	ClientInterceptorBase
	WorkerInterceptorBase
}

// WorkerInterceptorBase is a default implementation of WorkerInterceptor meant
// for embedding. See documentation in the interceptor package for more details.
//
// Exposed as: [go.temporal.io/sdk/interceptor.WorkerInterceptorBase]
type WorkerInterceptorBase struct{}

// Exposed as: [go.temporal.io/sdk/interceptor.WorkerInterceptor]
var _ WorkerInterceptor = &WorkerInterceptorBase{}

// InterceptActivity implements WorkerInterceptor.InterceptActivity.
func (*WorkerInterceptorBase) InterceptActivity(
	ctx context.Context,
	next ActivityInboundInterceptor,
) ActivityInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(ActivityInboundInterceptor)
}

// InterceptWorkflow implements WorkerInterceptor.InterceptWorkflow.
func (*WorkerInterceptorBase) InterceptWorkflow(
	ctx Context,
	next WorkflowInboundInterceptor,
) WorkflowInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(WorkflowInboundInterceptor)
}

// InterceptNexusOperation implements WorkerInterceptor.
func (w *WorkerInterceptorBase) InterceptNexusOperation(ctx context.Context, next NexusOperationInboundInterceptor) NexusOperationInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(NexusOperationInboundInterceptor)
}

func (*WorkerInterceptorBase) mustEmbedWorkerInterceptorBase() {
	_ = "STUB: not implemented"

	// ActivityInboundInterceptorBase is a default implementation of
	// ActivityInboundInterceptor meant for embedding. See documentation in the
	// interceptor package for more details.
	//
	// Exposed as: [go.temporal.io/sdk/interceptor.ActivityInboundInterceptorBase]
	return
}

type ActivityInboundInterceptorBase struct {
	Next ActivityInboundInterceptor
}

// Exposed as: [go.temporal.io/sdk/interceptor.ActivityInboundInterceptor]
var _ ActivityInboundInterceptor = &ActivityInboundInterceptorBase{}

// Init implements ActivityInboundInterceptor.Init.
func (a *ActivityInboundInterceptorBase) Init(outbound ActivityOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

// ExecuteActivity implements ActivityInboundInterceptor.ExecuteActivity.
func (a *ActivityInboundInterceptorBase) ExecuteActivity(
	ctx context.Context,
	in *ExecuteActivityInput,
) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (*ActivityInboundInterceptorBase) mustEmbedActivityInboundInterceptorBase() {
	_ = "STUB: not implemented"

	// ActivityOutboundInterceptorBase is a default implementation of
	// ActivityOutboundInterceptor meant for embedding. See documentation in the
	// interceptor package for more details.
	//
	// Exposed as: [go.temporal.io/sdk/interceptor.ActivityOutboundInterceptorBase]
	return
}

type ActivityOutboundInterceptorBase struct {
	Next ActivityOutboundInterceptor
}

// Exposed as: [go.temporal.io/sdk/interceptor.ActivityOutboundInterceptor]
var _ ActivityOutboundInterceptor = &ActivityOutboundInterceptorBase{}

// GetInfo implements ActivityOutboundInterceptor.GetInfo.
func (a *ActivityOutboundInterceptorBase) GetInfo(ctx context.Context) ActivityInfo {
	_ = "STUB: not implemented"
	return *new(ActivityInfo)
}

// GetLogger implements ActivityOutboundInterceptor.GetLogger.
func (a *ActivityOutboundInterceptorBase) GetLogger(ctx context.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

// GetMetricsHandler implements ActivityOutboundInterceptor.GetMetricsHandler.
func (a *ActivityOutboundInterceptorBase) GetMetricsHandler(ctx context.Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// RecordHeartbeat implements ActivityOutboundInterceptor.RecordHeartbeat.
func (a *ActivityOutboundInterceptorBase) RecordHeartbeat(ctx context.Context, details ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// HasHeartbeatDetails implements
// ActivityOutboundInterceptor.HasHeartbeatDetails.
func (a *ActivityOutboundInterceptorBase) HasHeartbeatDetails(ctx context.Context) bool {
	_ = "STUB: not implemented"
	return false
}

// GetHeartbeatDetails implements
// ActivityOutboundInterceptor.GetHeartbeatDetails.
func (a *ActivityOutboundInterceptorBase) GetHeartbeatDetails(ctx context.Context, d ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// GetWorkerStopChannel implements
// ActivityOutboundInterceptor.GetWorkerStopChannel.
func (a *ActivityOutboundInterceptorBase) GetWorkerStopChannel(ctx context.Context) <-chan struct{} {
	_ = "STUB: not implemented"
	return nil
}

// GetClient implements
// ActivityOutboundInterceptor.GetClient
func (a *ActivityOutboundInterceptorBase) GetClient(ctx context.Context) Client {
	_ = "STUB: not implemented"
	return *new(Client)
}

func (*ActivityOutboundInterceptorBase) mustEmbedActivityOutboundInterceptorBase() {
	_ = "STUB: not implemented"

	// WorkflowInboundInterceptorBase is a default implementation of
	// WorkflowInboundInterceptor meant for embedding. See documentation in the
	// interceptor package for more details.
	//
	// Exposed as: [go.temporal.io/sdk/interceptor.WorkflowInboundInterceptorBase]
	return
}

type WorkflowInboundInterceptorBase struct {
	Next WorkflowInboundInterceptor
}

// Exposed as: [go.temporal.io/sdk/interceptor.WorkflowInboundInterceptor]
var _ WorkflowInboundInterceptor = &WorkflowInboundInterceptorBase{}

// Init implements WorkflowInboundInterceptor.Init.
func (w *WorkflowInboundInterceptorBase) Init(outbound WorkflowOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

// ExecuteWorkflow implements WorkflowInboundInterceptor.ExecuteWorkflow.
func (w *WorkflowInboundInterceptorBase) ExecuteWorkflow(ctx Context, in *ExecuteWorkflowInput) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// HandleSignal implements WorkflowInboundInterceptor.HandleSignal.
func (w *WorkflowInboundInterceptorBase) HandleSignal(ctx Context, in *HandleSignalInput) error {
	_ = "STUB: not implemented"
	return nil
}

// ExecuteUpdate implements WorkflowInboundInterceptor.ExecuteUpdate.
func (w *WorkflowInboundInterceptorBase) ExecuteUpdate(ctx Context, in *UpdateInput) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ValidateUpdate implements WorkflowInboundInterceptor.ValidateUpdate.
func (w *WorkflowInboundInterceptorBase) ValidateUpdate(ctx Context, in *UpdateInput) error {
	_ = "STUB: not implemented"
	return nil
}

// HandleQuery implements WorkflowInboundInterceptor.HandleQuery.
func (w *WorkflowInboundInterceptorBase) HandleQuery(ctx Context, in *HandleQueryInput) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (*WorkflowInboundInterceptorBase) mustEmbedWorkflowInboundInterceptorBase() {
	_ = "STUB: not implemented"

	// WorkflowOutboundInterceptorBase is a default implementation of
	// WorkflowOutboundInterceptor meant for embedding. See documentation in the
	// interceptor package for more details.
	//
	// Exposed as: [go.temporal.io/sdk/interceptor.WorkflowOutboundInterceptorBase]
	return
}

type WorkflowOutboundInterceptorBase struct {
	Next WorkflowOutboundInterceptor
}

// Exposed as: [go.temporal.io/sdk/interceptor.WorkflowOutboundInterceptor]
var _ WorkflowOutboundInterceptor = &WorkflowOutboundInterceptorBase{}

// Go implements WorkflowOutboundInterceptor.Go.
func (w *WorkflowOutboundInterceptorBase) Go(ctx Context, name string, f func(ctx Context)) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

// ExecuteActivity implements WorkflowOutboundInterceptor.ExecuteActivity.
func (w *WorkflowOutboundInterceptorBase) ExecuteActivity(ctx Context, activityType string, args ...interface{}) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// Await implements WorkflowOutboundInterceptor.Await.
func (w *WorkflowOutboundInterceptorBase) Await(ctx Context, condition func() bool) error {
	_ = "STUB: not implemented"
	return nil
}

// AwaitWithTimeout implements WorkflowOutboundInterceptor.AwaitWithTimeout.
func (w *WorkflowOutboundInterceptorBase) AwaitWithTimeout(ctx Context, timeout time.Duration, condition func() bool) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

// AwaitWithOptions implements WorkflowOutboundInterceptor.AwaitWithOptions.
//
// NOTE: Experimental
func (w *WorkflowOutboundInterceptorBase) AwaitWithOptions(ctx Context, options AwaitOptions, condition func() bool) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

// ExecuteLocalActivity implements WorkflowOutboundInterceptor.ExecuteLocalActivity.
func (w *WorkflowOutboundInterceptorBase) ExecuteLocalActivity(
	ctx Context,
	activityType string,
	args ...interface{},
) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// ExecuteChildWorkflow implements WorkflowOutboundInterceptor.ExecuteChildWorkflow.
func (w *WorkflowOutboundInterceptorBase) ExecuteChildWorkflow(
	ctx Context,
	childWorkflowType string,
	args ...interface{},
) ChildWorkflowFuture {
	_ = "STUB: not implemented"
	return *new(ChildWorkflowFuture)
}

// GetInfo implements WorkflowOutboundInterceptor.GetInfo.
func (w *WorkflowOutboundInterceptorBase) GetInfo(ctx Context) *WorkflowInfo {
	_ = "STUB: not implemented"
	return nil

	// GetTypedSearchAttributes implements WorkflowOutboundInterceptor.GetTypedSearchAttributes.
}

func (w *WorkflowOutboundInterceptorBase) GetTypedSearchAttributes(ctx Context) SearchAttributes {
	_ = "STUB: not implemented"
	return *new(SearchAttributes)
}

// GetCurrentUpdateInfo implements WorkflowOutboundInterceptor.GetCurrentUpdateInfo.
func (w *WorkflowOutboundInterceptorBase) GetCurrentUpdateInfo(ctx Context) *UpdateInfo {
	_ = "STUB: not implemented"
	return nil
}

// GetLogger implements WorkflowOutboundInterceptor.GetLogger.
func (w *WorkflowOutboundInterceptorBase) GetLogger(ctx Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

// GetMetricsHandler implements WorkflowOutboundInterceptor.GetMetricsHandler.
func (w *WorkflowOutboundInterceptorBase) GetMetricsHandler(ctx Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// Now implements WorkflowOutboundInterceptor.Now.
func (w *WorkflowOutboundInterceptorBase) Now(ctx Context) time.Time {
	_ = "STUB: not implemented"
	return *

	// NewTimer implements WorkflowOutboundInterceptor.NewTimer.
	new(time.Time)
}

func (w *WorkflowOutboundInterceptorBase) NewTimer(ctx Context, d time.Duration) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// NewTimerWithOptions implements WorkflowOutboundInterceptor.NewTimerWithOptions.
//
// NOTE: Experimental
func (w *WorkflowOutboundInterceptorBase) NewTimerWithOptions(
	ctx Context,
	d time.Duration,
	options TimerOptions,
) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// Sleep implements WorkflowOutboundInterceptor.Sleep.
func (w *WorkflowOutboundInterceptorBase) Sleep(ctx Context, d time.Duration) (err error) {
	_ = "STUB: not implemented"
	return nil
}

// RequestCancelExternalWorkflow implements
// WorkflowOutboundInterceptor.RequestCancelExternalWorkflow.
func (w *WorkflowOutboundInterceptorBase) RequestCancelExternalWorkflow(
	ctx Context,
	workflowID string,
	runID string,
) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// SignalExternalWorkflow implements
// WorkflowOutboundInterceptor.SignalExternalWorkflow.
func (w *WorkflowOutboundInterceptorBase) SignalExternalWorkflow(
	ctx Context,
	workflowID string,
	runID string,
	signalName string,
	arg interface{},
) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// SignalChildWorkflow implements
// WorkflowOutboundInterceptor.SignalChildWorkflow.
func (w *WorkflowOutboundInterceptorBase) SignalChildWorkflow(
	ctx Context,
	workflowID string,
	signalName string,
	arg interface{},
) Future {
	_ = "STUB: not implemented"
	return *new(Future)
}

// UpsertSearchAttributes implements
// WorkflowOutboundInterceptor.UpsertSearchAttributes.
func (w *WorkflowOutboundInterceptorBase) UpsertSearchAttributes(ctx Context, attributes map[string]interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// UpsertTypedSearchAttributes implements
// WorkflowOutboundInterceptor.UpsertTypedSearchAttributes.
func (w *WorkflowOutboundInterceptorBase) UpsertTypedSearchAttributes(ctx Context, attributes ...SearchAttributeUpdate) error {
	_ = "STUB: not implemented"
	return nil
}

// UpsertMemo implements
// WorkflowOutboundInterceptor.UpsertMemo.
func (w *WorkflowOutboundInterceptorBase) UpsertMemo(ctx Context, memo map[string]interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// GetSignalChannel implements WorkflowOutboundInterceptor.GetSignalChannel.
func (w *WorkflowOutboundInterceptorBase) GetSignalChannel(ctx Context, signalName string) ReceiveChannel {
	_ = "STUB: not implemented"
	return *new(ReceiveChannel)
}

// GetSignalChannelWithOptions implements WorkflowOutboundInterceptor.GetSignalChannelWithOptions.
//
// NOTE: Experimental
func (w *WorkflowOutboundInterceptorBase) GetSignalChannelWithOptions(
	ctx Context,
	signalName string,
	options SignalChannelOptions,
) ReceiveChannel {
	_ = "STUB: not implemented"
	return *new(ReceiveChannel)
}

// SideEffect implements WorkflowOutboundInterceptor.SideEffect.
func (w *WorkflowOutboundInterceptorBase) SideEffect(
	ctx Context,
	f func(ctx Context) interface{},
) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

// SideEffectWithOptions implements WorkflowOutboundInterceptor.SideEffectWithOptions.
func (w *WorkflowOutboundInterceptorBase) SideEffectWithOptions(
	ctx Context,
	options SideEffectOptions,
	f func(ctx Context) interface{},
) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

// MutableSideEffect implements WorkflowOutboundInterceptor.MutableSideEffect.
func (w *WorkflowOutboundInterceptorBase) MutableSideEffect(
	ctx Context,
	id string,
	f func(ctx Context) interface{},
	equals func(a, b interface{}) bool,
) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

// MutableSideEffectWithOptions implements WorkflowOutboundInterceptor.MutableSideEffectWithOptions.
func (w *WorkflowOutboundInterceptorBase) MutableSideEffectWithOptions(
	ctx Context,
	id string,
	options MutableSideEffectOptions,
	f func(ctx Context) interface{},
	equals func(a, b interface{}) bool,
) converter.EncodedValue {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

// GetVersion implements WorkflowOutboundInterceptor.GetVersion.
func (w *WorkflowOutboundInterceptorBase) GetVersion(
	ctx Context,
	changeID string,
	minSupported Version,
	maxSupported Version,
) Version {
	_ = "STUB: not implemented"
	return *new(Version)
}

// SetQueryHandler implements WorkflowOutboundInterceptor.SetQueryHandler.
func (w *WorkflowOutboundInterceptorBase) SetQueryHandler(ctx Context, queryType string, handler interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// SetQueryHandlerWithOptions implements WorkflowOutboundInterceptor.SetQueryHandlerWithOptions.
//
// NOTE: Experimental
func (w *WorkflowOutboundInterceptorBase) SetQueryHandlerWithOptions(
	ctx Context,
	queryType string,
	handler interface{},
	options QueryHandlerOptions,
) error {
	_ = "STUB: not implemented"
	return nil
}

// SetUpdateHandler implements WorkflowOutboundInterceptor.SetUpdateHandler.
func (w *WorkflowOutboundInterceptorBase) SetUpdateHandler(ctx Context, updateName string, handler interface{}, opts UpdateHandlerOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// IsReplaying implements WorkflowOutboundInterceptor.IsReplaying.
func (w *WorkflowOutboundInterceptorBase) IsReplaying(ctx Context) bool {
	_ = "STUB: not implemented"
	return false
}

// HasLastCompletionResult implements
// WorkflowOutboundInterceptor.HasLastCompletionResult.
func (w *WorkflowOutboundInterceptorBase) HasLastCompletionResult(ctx Context) bool {
	_ = "STUB: not implemented"
	return false
}

// GetLastCompletionResult implements
// WorkflowOutboundInterceptor.GetLastCompletionResult.
func (w *WorkflowOutboundInterceptorBase) GetLastCompletionResult(ctx Context, d ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// GetLastError implements WorkflowOutboundInterceptor.GetLastError.
func (w *WorkflowOutboundInterceptorBase) GetLastError(ctx Context) error {
	_ = "STUB: not implemented"
	return nil
}

// NewContinueAsNewError implements
// WorkflowOutboundInterceptor.NewContinueAsNewError.
func (w *WorkflowOutboundInterceptorBase) NewContinueAsNewError(
	ctx Context,
	wfn interface{},
	args ...interface{},
) error {
	_ = "STUB: not implemented"
	return nil
}

// ExecuteNexusOperation implements
// WorkflowOutboundInterceptor.ExecuteNexusOperation.
func (w *WorkflowOutboundInterceptorBase) ExecuteNexusOperation(
	ctx Context,
	input ExecuteNexusOperationInput,
) NexusOperationFuture {
	_ = "STUB: not implemented"
	return *new(NexusOperationFuture)
}

// RequestCancelNexusOperation implements
// WorkflowOutboundInterceptor.RequestCancelNexusOperation.
func (w *WorkflowOutboundInterceptorBase) RequestCancelNexusOperation(ctx Context, input RequestCancelNexusOperationInput) {
	_ = "STUB: not implemented"
	return
}

func (*WorkflowOutboundInterceptorBase) mustEmbedWorkflowOutboundInterceptorBase() {
	_ = "STUB: not implemented"

	// ClientInterceptorBase is a default implementation of ClientInterceptor meant
	// for embedding. See documentation in the interceptor package for more details.
	//
	// Exposed as: [go.temporal.io/sdk/interceptor.ClientInterceptorBase]
	return
}

type ClientInterceptorBase struct{}

// Exposed as: [go.temporal.io/sdk/interceptor.ClientInterceptor]
var _ ClientInterceptor = &ClientInterceptorBase{}

// InterceptClient implements ClientInterceptor.InterceptClient.
func (*ClientInterceptorBase) InterceptClient(
	next ClientOutboundInterceptor,
) ClientOutboundInterceptor {
	_ = "STUB: not implemented"
	return *new(ClientOutboundInterceptor)
}

func (*ClientInterceptorBase) mustEmbedClientInterceptorBase() {
	_ = "STUB: not implemented"

	// ClientOutboundInterceptorBase is a default implementation of
	// ClientOutboundInterceptor meant for embedding. See documentation in the
	// interceptor package for more details.
	//
	// Exposed as: [go.temporal.io/sdk/interceptor.ClientOutboundInterceptorBase]
	return
}

type ClientOutboundInterceptorBase struct {
	Next ClientOutboundInterceptor
}

// Exposed as: [go.temporal.io/sdk/interceptor.ClientOutboundInterceptor]
var _ ClientOutboundInterceptor = &ClientOutboundInterceptorBase{}

func (c *ClientOutboundInterceptorBase) UpdateWorkflow(
	ctx context.Context,
	in *ClientUpdateWorkflowInput,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

func (c *ClientOutboundInterceptorBase) PollWorkflowUpdate(
	ctx context.Context,
	in *ClientPollWorkflowUpdateInput,
) (*ClientPollWorkflowUpdateOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *ClientOutboundInterceptorBase) UpdateWithStartWorkflow(
	ctx context.Context,
	in *ClientUpdateWithStartWorkflowInput,
) (WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowUpdateHandle), nil
}

// ExecuteWorkflow implements ClientOutboundInterceptor.ExecuteWorkflow.
func (c *ClientOutboundInterceptorBase) ExecuteWorkflow(
	ctx context.Context,
	in *ClientExecuteWorkflowInput,
) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// SignalWorkflow implements ClientOutboundInterceptor.SignalWorkflow.
func (c *ClientOutboundInterceptorBase) SignalWorkflow(ctx context.Context, in *ClientSignalWorkflowInput) error {
	_ = "STUB: not implemented"
	return nil
}

// SignalWithStartWorkflow implements
// ClientOutboundInterceptor.SignalWithStartWorkflow.
func (c *ClientOutboundInterceptorBase) SignalWithStartWorkflow(
	ctx context.Context,
	in *ClientSignalWithStartWorkflowInput,
) (WorkflowRun, error) {
	_ = "STUB: not implemented"
	return *new(WorkflowRun), nil
}

// CancelWorkflow implements ClientOutboundInterceptor.CancelWorkflow.
func (c *ClientOutboundInterceptorBase) CancelWorkflow(ctx context.Context, in *ClientCancelWorkflowInput) error {
	_ = "STUB: not implemented"
	return nil
}

// TerminateWorkflow implements ClientOutboundInterceptor.TerminateWorkflow.
func (c *ClientOutboundInterceptorBase) TerminateWorkflow(ctx context.Context, in *ClientTerminateWorkflowInput) error {
	_ = "STUB: not implemented"
	return nil
}

// QueryWorkflow implements ClientOutboundInterceptor.QueryWorkflow.
func (c *ClientOutboundInterceptorBase) QueryWorkflow(
	ctx context.Context,
	in *ClientQueryWorkflowInput,
) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

// DescribeWorkflow implements ClientOutboundInterceptor.DescribeWorkflow.
func (c *ClientOutboundInterceptorBase) DescribeWorkflow(
	ctx context.Context,
	in *ClientDescribeWorkflowInput,
) (*ClientDescribeWorkflowOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// CreateSchedule implements ClientOutboundInterceptor.CreateSchedule.
func (c *ClientOutboundInterceptorBase) CreateSchedule(ctx context.Context, in *ScheduleClientCreateInput) (ScheduleHandle, error) {
	_ = "STUB: not implemented"
	return *new(ScheduleHandle), nil
}

// ExecuteActivity implements ClientOutboundInterceptor.ExecuteActivity.
func (c *ClientOutboundInterceptorBase) ExecuteActivity(
	ctx context.Context,
	in *ClientExecuteActivityInput,
) (ClientActivityHandle, error) {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle), nil
}

// GetActivityHandle implements ClientOutboundInterceptor.GetActivityHandle.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) GetActivityHandle(
	in *ClientGetActivityHandleInput,
) ClientActivityHandle {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle)
}

// CancelActivity implements ClientOutboundInterceptor.CancelActivity.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) CancelActivity(
	ctx context.Context,
	in *ClientCancelActivityInput,
) error {
	_ = "STUB: not implemented"
	return nil
}

// TerminateActivity implements ClientOutboundInterceptor.TerminateActivity.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) TerminateActivity(
	ctx context.Context,
	in *ClientTerminateActivityInput,
) error {
	_ = "STUB: not implemented"
	return nil
}

// DescribeActivity implements ClientOutboundInterceptor.DescribeActivity.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) DescribeActivity(
	ctx context.Context,
	in *ClientDescribeActivityInput,
) (*ClientDescribeActivityOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// PollActivityResult implements ClientOutboundInterceptor.PollActivityResult.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) PollActivityResult(
	ctx context.Context,
	in *ClientPollActivityResultInput,
) (*ClientPollActivityResultOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ExecuteNexusOperation implements ClientOutboundInterceptor.ExecuteNexusOperation.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) ExecuteNexusOperation(
	ctx context.Context,
	in *ClientExecuteNexusOperationInput,
) (ClientNexusOperationHandle, error) {
	_ = "STUB: not implemented"
	return *new(ClientNexusOperationHandle), nil
}

// GetNexusOperationHandle implements ClientOutboundInterceptor.GetNexusOperationHandle.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) GetNexusOperationHandle(
	in *ClientGetNexusOperationHandleInput,
) ClientNexusOperationHandle {
	_ = "STUB: not implemented"
	return *new(ClientNexusOperationHandle)
}

// CancelNexusOperation implements ClientOutboundInterceptor.CancelNexusOperation.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) CancelNexusOperation(
	ctx context.Context,
	in *ClientCancelNexusOperationInput,
) error {
	_ = "STUB: not implemented"
	return nil
}

// TerminateNexusOperation implements ClientOutboundInterceptor.TerminateNexusOperation.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) TerminateNexusOperation(
	ctx context.Context,
	in *ClientTerminateNexusOperationInput,
) error {
	_ = "STUB: not implemented"
	return nil
}

// DescribeNexusOperation implements ClientOutboundInterceptor.DescribeNexusOperation.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) DescribeNexusOperation(
	ctx context.Context,
	in *ClientDescribeNexusOperationInput,
) (*ClientDescribeNexusOperationOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// PollNexusOperationResult implements ClientOutboundInterceptor.PollNexusOperationResult.
//
// NOTE: Experimental
func (c *ClientOutboundInterceptorBase) PollNexusOperationResult(
	ctx context.Context,
	in *ClientPollNexusOperationResultInput,
) (*ClientPollNexusOperationResultOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (*ClientOutboundInterceptorBase) mustEmbedClientOutboundInterceptorBase() {
	_ = "STUB: not implemented"

	// NexusOperationInboundInterceptorBase is a default implementation of [NexusOperationInboundInterceptor] that
	// forwards calls to the next inbound interceptor.
	//
	// Note: Experimental
	return
}

type NexusOperationInboundInterceptorBase struct {
	Next NexusOperationInboundInterceptor
}

// CancelOperation implements NexusOperationInboundInterceptor.
func (n *NexusOperationInboundInterceptorBase) CancelOperation(ctx context.Context, input NexusCancelOperationInput) error {
	_ = "STUB: not implemented"
	return nil
}

// Init implements NexusOperationInboundInterceptor.
func (n *NexusOperationInboundInterceptorBase) Init(ctx context.Context, outbound NexusOperationOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

// StartOperation implements NexusOperationInboundInterceptor.
func (n *NexusOperationInboundInterceptorBase) StartOperation(ctx context.Context, input NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// mustEmbedNexusOperationInboundInterceptorBase implements NexusOperationInboundInterceptor.
func (n *NexusOperationInboundInterceptorBase) mustEmbedNexusOperationInboundInterceptorBase() {
	_ = "STUB: not implemented"
	return
}

var _ NexusOperationInboundInterceptor = &NexusOperationInboundInterceptorBase{}

// NexusOperationOutboundInterceptorBase is a default implementation of [NexusOperationOutboundInterceptor] that
// forwards calls to the next outbound interceptor.
//
// Note: Experimental
type NexusOperationOutboundInterceptorBase struct {
	Next NexusOperationOutboundInterceptor
}

// GetOperationInfo implements NexusOperationOutboundInterceptor.
func (n *NexusOperationOutboundInterceptorBase) GetOperationInfo(ctx context.Context) NexusOperationInfo {
	_ = "STUB: not implemented"
	return *new(NexusOperationInfo)
}

// GetClient implements NexusOperationOutboundInterceptor.
func (n *NexusOperationOutboundInterceptorBase) GetClient(ctx context.Context) Client {
	_ = "STUB: not implemented"
	return *new(Client)
}

// GetLogger implements NexusOperationOutboundInterceptor.
func (n *NexusOperationOutboundInterceptorBase) GetLogger(ctx context.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

// GetMetricsHandler implements NexusOperationOutboundInterceptor.
func (n *NexusOperationOutboundInterceptorBase) GetMetricsHandler(ctx context.Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// mustEmbedNexusOperationOutboundInterceptorBase implements NexusOperationOutboundInterceptor.
func (n *NexusOperationOutboundInterceptorBase) mustEmbedNexusOperationOutboundInterceptorBase() {
	_ = "STUB: not implemented"
	return
}

var _ NexusOperationOutboundInterceptor = &NexusOperationOutboundInterceptorBase{}
