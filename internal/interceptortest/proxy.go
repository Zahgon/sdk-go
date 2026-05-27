// Package interceptortest contains internal utilities for testing interceptors.
package interceptortest

import (
	"context"
	"reflect"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// ProxyCall represents a call made to the proxy interceptor.
type ProxyCall struct {
	Interface reflect.Type
	Next      reflect.Value
	Method    reflect.Method
	Args      []reflect.Value
}

// Call invokes this proxied call.
func (p *ProxyCall) Call() []reflect.Value {
	_ = "STUB: not implemented"
	// Put receiver before args
	return nil
}

// If call is variadic, have to use call slice

// Invoker is an interface that is called for every intercepted call by a proxy.
type Invoker interface {
	// Invoke is called for every intercepted call. This may be called
	// concurrently from separate goroutines.
	Invoke(*ProxyCall) []reflect.Value
}

// InvokerFunc implements Invoker for a single function.
type InvokerFunc func(*ProxyCall) []reflect.Value

var _ Invoker = (InvokerFunc)(nil)

// InvokerFunc implements Invoker.Invoke.
func (i InvokerFunc) Invoke(p *ProxyCall) []reflect.Value { _ = "STUB: not implemented"; return nil }

type proxy struct {
	interceptor.InterceptorBase
	nextProxy
}

// NewProxy creates a proxy interceptor that calls the given invoker.
func NewProxy(invoker Invoker) interceptor.Interceptor {
	_ = "STUB: not implemented"
	return *new(interceptor.Interceptor)
}

// CallRecordingInvoker is an Invoker that records all calls made to it before
// continuing normal invocation.
type CallRecordingInvoker struct {
	calls     []*RecordedCall
	callsLock sync.RWMutex
}

// Calls provides a copy of the currently recorded calls.
func (c *CallRecordingInvoker) Calls() []*RecordedCall { _ = "STUB: not implemented"; return nil }

// Invoke implements Invoker.Invoke to record calls.
func (c *CallRecordingInvoker) Invoke(p *ProxyCall) []reflect.Value {
	_ = "STUB: not implemented"
	return nil
}

// RecordedCall is a ProxyCall that also has results.
type RecordedCall struct {
	*ProxyCall
	// Results of the call. This will not be set if still running and may be set
	// asynchronously in a non-concurrency-safe way once the call completes.
	Results []reflect.Value
}

type nextProxy struct {
	iface   reflect.Type
	next    reflect.Value
	invoker Invoker
}

func (n *nextProxy) proxyWithNext(ifacePtr interface{}, next interface{}) *nextProxy {
	_ = "STUB: not implemented"
	return nil
}

func (n *nextProxy) invoke(args ...interface{}) []reflect.Value {
	_ = "STUB: not implemented"
	// Grab caller function name
	return nil
}

// Get method and args

// If it's not valid, make a new instance of the type

func (p *proxy) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(interceptor.ActivityInboundInterceptor)
}

func (p *proxy) InterceptWorkflow(
	ctx workflow.Context,
	next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(interceptor.WorkflowInboundInterceptor)
}

func (p *proxy) InterceptClient(
	next interceptor.ClientOutboundInterceptor,
) interceptor.ClientOutboundInterceptor {
	_ = "STUB: not implemented"
	return *new(interceptor.ClientOutboundInterceptor)
}

type proxyActivityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	*nextProxy
}

func (p *proxyActivityInbound) Init(outbound interceptor.ActivityOutboundInterceptor) (err error) {
	_ = "STUB: not implemented"
	// Wrap outbound first
	return nil
}

func (p *proxyActivityInbound) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (ret interface{}, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type proxyActivityOutbound struct {
	interceptor.ActivityOutboundInterceptorBase
	*nextProxy
}

func (p *proxyActivityOutbound) GetInfo(ctx context.Context) (ret activity.Info) {
	_ = "STUB: not implemented"
	return *new(activity.Info)
}

func (p *proxyActivityOutbound) GetLogger(ctx context.Context) (ret log.Logger) {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (p *proxyActivityOutbound) GetMetricsHandler(ctx context.Context) (ret metrics.Handler) {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (p *proxyActivityOutbound) RecordHeartbeat(ctx context.Context, details ...interface{}) {
	_ = "STUB: not implemented"
	return
}

func (p *proxyActivityOutbound) HasHeartbeatDetails(ctx context.Context) (ret bool) {
	_ = "STUB: not implemented"
	return false
}

func (p *proxyActivityOutbound) GetHeartbeatDetails(ctx context.Context, d ...interface{}) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyActivityOutbound) GetWorkerStopChannel(ctx context.Context) (ret <-chan struct{}) {
	_ = "STUB: not implemented"
	return nil
}

type proxyWorkflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	*nextProxy
}

func (p *proxyWorkflowInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) (err error) {
	_ = "STUB: not implemented"
	// Wrap outbound first
	return nil
}

func (p *proxyWorkflowInbound) ExecuteWorkflow(
	ctx workflow.Context,
	in *interceptor.ExecuteWorkflowInput,
) (ret interface{}, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (p *proxyWorkflowInbound) HandleSignal(ctx workflow.Context, in *interceptor.HandleSignalInput) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowInbound) HandleQuery(
	ctx workflow.Context,
	in *interceptor.HandleQueryInput,
) (ret interface{}, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type proxyWorkflowOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	*nextProxy
}

func (p *proxyWorkflowOutbound) Go(
	ctx workflow.Context,
	name string,
	f func(ctx workflow.Context),
) (ret workflow.Context) {
	_ = "STUB: not implemented"
	return *new(workflow.Context)
}

func (p *proxyWorkflowOutbound) Await(ctx workflow.Context, condition func() bool) (ret error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) AwaitWithTimeout(ctx workflow.Context, timeout time.Duration, condition func() bool) (ret bool, err error) {
	_ = "STUB: not implemented"
	return false, nil
}

func (p *proxyWorkflowOutbound) ExecuteActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) (ret workflow.Future) {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

func (p *proxyWorkflowOutbound) ExecuteLocalActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) (ret workflow.Future) {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

func (p *proxyWorkflowOutbound) ExecuteChildWorkflow(
	ctx workflow.Context,
	childWorkflowType string,
	args ...interface{},
) (ret workflow.ChildWorkflowFuture) {
	_ = "STUB: not implemented"
	return *new(workflow.ChildWorkflowFuture)
}

func (p *proxyWorkflowOutbound) GetInfo(ctx workflow.Context) (ret *workflow.Info) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) GetLogger(ctx workflow.Context) (ret log.Logger) {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (p *proxyWorkflowOutbound) GetMetricsHandler(ctx workflow.Context) (ret metrics.Handler) {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

func (p *proxyWorkflowOutbound) Now(ctx workflow.Context) (ret time.Time) {
	_ = "STUB: not implemented"
	return *new(time.Time)
}

func (p *proxyWorkflowOutbound) NewTimer(ctx workflow.Context, d time.Duration) (ret workflow.Future) {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

func (p *proxyWorkflowOutbound) Sleep(ctx workflow.Context, d time.Duration) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) RequestCancelExternalWorkflow(
	ctx workflow.Context,
	workflowID string,
	runID string,
) (ret workflow.Future) {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

func (p *proxyWorkflowOutbound) SignalExternalWorkflow(
	ctx workflow.Context,
	workflowID string,
	runID string,
	signalName string,
	arg interface{},
) (ret workflow.Future) {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

func (p *proxyWorkflowOutbound) UpsertSearchAttributes(
	ctx workflow.Context,
	attributes map[string]interface{},
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) UpsertMemo(
	ctx workflow.Context,
	memo map[string]interface{},
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) GetSignalChannel(
	ctx workflow.Context,
	signalName string,
) (ret workflow.ReceiveChannel) {
	_ = "STUB: not implemented"
	return *new(workflow.ReceiveChannel)
}

func (p *proxyWorkflowOutbound) SideEffect(
	ctx workflow.Context,
	f func(ctx workflow.Context) interface{},
) (ret converter.EncodedValue) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

func (p *proxyWorkflowOutbound) SideEffectWithOptions(
	ctx workflow.Context,
	options workflow.SideEffectOptions,
	f func(ctx workflow.Context) interface{},
) (ret converter.EncodedValue) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

func (p *proxyWorkflowOutbound) MutableSideEffect(
	ctx workflow.Context,
	id string,
	f func(ctx workflow.Context) interface{},
	equals func(a, b interface{}) bool,
) (ret converter.EncodedValue) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

func (p *proxyWorkflowOutbound) MutableSideEffectWithOptions(
	ctx workflow.Context,
	id string,
	options workflow.MutableSideEffectOptions,
	f func(ctx workflow.Context) interface{},
	equals func(a, b interface{}) bool,
) (ret converter.EncodedValue) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue)
}

func (p *proxyWorkflowOutbound) GetVersion(
	ctx workflow.Context,
	changeID string,
	minSupported workflow.Version,
	maxSupported workflow.Version,
) (ret workflow.Version) {
	_ = "STUB: not implemented"
	return *new(workflow.Version)
}

func (p *proxyWorkflowOutbound) SetQueryHandler(
	ctx workflow.Context,
	queryType string,
	handler interface{},
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) IsReplaying(ctx workflow.Context) (ret bool) {
	_ = "STUB: not implemented"
	return false
}

func (p *proxyWorkflowOutbound) HasLastCompletionResult(ctx workflow.Context) (ret bool) {
	_ = "STUB: not implemented"
	return false
}

func (p *proxyWorkflowOutbound) GetLastCompletionResult(ctx workflow.Context, d ...interface{}) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) GetLastError(ctx workflow.Context) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyWorkflowOutbound) NewContinueAsNewError(
	ctx workflow.Context,
	wfn interface{},
	args ...interface{},
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

type proxyClientOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	*nextProxy
}

func (p *proxyClientOutbound) ExecuteWorkflow(
	ctx context.Context,
	in *interceptor.ClientExecuteWorkflowInput,
) (ret client.WorkflowRun, err error) {
	_ = "STUB: not implemented"
	return *new(client.WorkflowRun), nil
}

func (p *proxyClientOutbound) SignalWorkflow(
	ctx context.Context,
	in *interceptor.ClientSignalWorkflowInput,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyClientOutbound) SignalWithStartWorkflow(
	ctx context.Context,
	in *interceptor.ClientSignalWithStartWorkflowInput,
) (ret client.WorkflowRun, err error) {
	_ = "STUB: not implemented"
	return *new(client.WorkflowRun), nil
}

func (p *proxyClientOutbound) CancelWorkflow(
	ctx context.Context,
	in *interceptor.ClientCancelWorkflowInput,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyClientOutbound) TerminateWorkflow(
	ctx context.Context,
	in *interceptor.ClientTerminateWorkflowInput,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyClientOutbound) QueryWorkflow(
	ctx context.Context,
	in *interceptor.ClientQueryWorkflowInput,
) (ret converter.EncodedValue, err error) {
	_ = "STUB: not implemented"
	return *new(converter.EncodedValue), nil
}

func (p *proxyClientOutbound) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ClientExecuteActivityInput,
) (ret client.ActivityHandle, err error) {
	_ = "STUB: not implemented"
	return *new(client.ActivityHandle), nil
}

func (p *proxyClientOutbound) GetActivityHandle(
	in *interceptor.ClientGetActivityHandleInput,
) (ret client.ActivityHandle) {
	_ = "STUB: not implemented"
	return *new(client.ActivityHandle)
}

func (p *proxyClientOutbound) CancelActivity(
	ctx context.Context,
	in *interceptor.ClientCancelActivityInput,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyClientOutbound) TerminateActivity(
	ctx context.Context,
	in *interceptor.ClientTerminateActivityInput,
) (err error) {
	_ = "STUB: not implemented"
	return nil
}

func (p *proxyClientOutbound) DescribeActivity(
	ctx context.Context,
	in *interceptor.ClientDescribeActivityInput,
) (ret *interceptor.ClientDescribeActivityOutput, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (p *proxyClientOutbound) PollActivityResult(
	ctx context.Context,
	in *interceptor.ClientPollActivityResultInput,
) (ret *interceptor.ClientPollActivityResultOutput, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}
