package interceptor

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

const (
	workflowIDTagKey = "temporalWorkflowID"
	runIDTagKey      = "temporalRunID"
	activityIDTagKey = "temporalActivityID"
	updateIDTagKey   = "temporalUpdateID"
)

// Tracer is an interface for tracing implementations as used by
// NewTracingInterceptor. Most callers do not use this directly, but rather use
// the opentracing or opentelemetry packages.
//
// All implementations must embed BaseTracer to safely
// handle future changes.
type Tracer interface {
	// Options returns the options for the tracer. This is only called once on
	// initialization.
	Options() TracerOptions

	// UnmarshalSpan unmarshals the given map into a span reference.
	UnmarshalSpan(map[string]string) (TracerSpanRef, error)

	// MarshalSpan marshals the given span into a map. If the map is empty with no
	// error, the span is simply not set.
	MarshalSpan(TracerSpan) (map[string]string, error)

	// SpanFromContext returns the span from the general Go context or nil if not
	// present.
	SpanFromContext(context.Context) TracerSpan

	// ContextWithSpan creates a general Go context with the given span set.
	ContextWithSpan(context.Context, TracerSpan) context.Context

	// StartSpan starts and returns a span with the given options.
	StartSpan(*TracerStartSpanOptions) (TracerSpan, error)

	// GetLogger returns a log.Logger which may include additional fields in its
	// output in order to support correlation of tracing and log data.
	GetLogger(log.Logger, TracerSpanRef) log.Logger
	// SpanName can be used to give a custom name to a Span according to the input TracerStartSpanOptions,
	// or the decision can be deferred to the BaseTracer implementation.
	SpanName(options *TracerStartSpanOptions) string

	mustEmbedBaseTracer()
}

// BaseTracer is a default implementation of Tracer meant for embedding.
type BaseTracer struct{}

func (BaseTracer) GetLogger(logger log.Logger, ref TracerSpanRef) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (BaseTracer) SpanName(options *TracerStartSpanOptions) string {
	_ = "STUB: not implemented"
	return ""
}

//lint:ignore U1000 Ignore unused method; it is only required to implement the Tracer interface but will never be called.
func (BaseTracer) mustEmbedBaseTracer() {
	_ = "STUB: not implemented"

	// TracerOptions are options returned from Tracer.Options.
	return
}

type TracerOptions struct {
	// SpanContextKey provides a key to put a span on a context unrelated to how a
	// span might otherwise be put on a context by ContextWithSpan. This should
	// never be nil.
	//
	// This is used internally to set the span on contexts not natively supported
	// by tracing systems such as [workflow.Context].
	SpanContextKey interface{}

	// HeaderKey is the key name on the Temporal header to serialize the span to.
	// This should never be empty.
	HeaderKey string

	// DisableSignalTracing can be set to disable signal tracing.
	DisableSignalTracing bool

	// DisableQueryTracing can be set to disable query tracing.
	DisableQueryTracing bool

	// DisableUpdateTracing can be set to disable update tracing.
	DisableUpdateTracing bool

	// AllowInvalidParentSpans will swallow errors interpreting parent
	// spans from headers. Useful when migrating from one tracing library
	// to another, while workflows/activities may be in progress.
	AllowInvalidParentSpans bool
}

// TracerStartSpanOptions are options for Tracer.StartSpan.
type TracerStartSpanOptions struct {
	// Parent is the optional parent reference of the span.
	Parent TracerSpanRef
	// Operation is the general operation name without the specific name.
	Operation string

	// Name is the specific activity, workflow, etc for the operation.
	Name string

	// Time indicates the start time of the span.
	//
	// For RunWorkflow and RunActivity operation types, this will match workflow.Info.WorkflowStartTime and
	// activity.Info.StartedTime respectively. All other operations use time.Now().
	Time time.Time

	// DependedOn is true if the parent depends on this span or false if it just
	// is related to the parent. In OpenTracing terms, this is true for "ChildOf"
	// reference types and false for "FollowsFrom" reference types.
	DependedOn bool

	// Tags are a set of span tags.
	Tags map[string]string

	// FromHeader is used internally, not by tracer implementations, to determine
	// whether the parent span can be retrieved from the Temporal header.
	FromHeader bool

	// ToHeader is used internally, not by tracer implementations, to determine
	// whether the span should be placed on the Temporal header.
	ToHeader bool

	// IdempotencyKey may optionally be used by tracing implementations to generate
	// deterministic span IDs.
	//
	// This is useful in workflow contexts where spans may need to be "resumed" before
	// ultimately being reported. Generating a deterministic span ID ensures that any
	// child spans created before the parent span is resumed do not become orphaned.
	//
	// IdempotencyKey is not guaranteed to be set for all operations; Tracer
	// implementations MUST therefore ignore zero values for this field.
	//
	// IdempotencyKey should be treated as opaque data by Tracer implementations.
	// Do not attempt to parse it, as the format is subject to change.
	IdempotencyKey string
}

// TracerSpanRef represents a span reference such as a parent.
type TracerSpanRef interface {
}

// TracerSpan represents a span.
type TracerSpan interface {
	TracerSpanRef

	// Finish is called when the span is complete.
	Finish(*TracerFinishSpanOptions)
}

// TracerFinishSpanOptions are options for TracerSpan.Finish.
type TracerFinishSpanOptions struct {
	// Error is present if there was an error in the code traced by this specific
	// span.
	Error error
}

type tracingInterceptor struct {
	InterceptorBase
	tracer  Tracer
	options TracerOptions
}

// NewTracingInterceptor creates a new interceptor using the given tracer. Most
// callers do not use this directly, but rather use the opentracing or
// opentelemetry packages. This panics if options are not set as expected.
func NewTracingInterceptor(tracer Tracer) Interceptor {
	_ = "STUB: not implemented"
	return *new(Interceptor)
}

func (t *tracingInterceptor) InterceptClient(next ClientOutboundInterceptor) ClientOutboundInterceptor {
	_ = "STUB: not implemented"
	return *new(ClientOutboundInterceptor)
}

func (t *tracingInterceptor) InterceptActivity(
	ctx context.Context,
	next ActivityInboundInterceptor,
) ActivityInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(ActivityInboundInterceptor)
}

func (t *tracingInterceptor) InterceptWorkflow(
	ctx workflow.Context,
	next WorkflowInboundInterceptor,
) WorkflowInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(WorkflowInboundInterceptor)
}

func (t *tracingInterceptor) InterceptNexusOperation(
	ctx context.Context,
	next NexusOperationInboundInterceptor,
) NexusOperationInboundInterceptor {
	_ = "STUB: not implemented"
	return *new(NexusOperationInboundInterceptor)
}

type tracingClientOutboundInterceptor struct {
	ClientOutboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingClientOutboundInterceptor) CreateSchedule(ctx context.Context, in *ScheduleClientCreateInput) (client.ScheduleHandle, error) {
	_ = "STUB: not implemented"
	// Start span and write to header
	return *new(client.ScheduleHandle), nil
}

func (t *tracingClientOutboundInterceptor) ExecuteWorkflow(
	ctx context.Context,
	in *ClientExecuteWorkflowInput,
) (client.WorkflowRun, error) {
	_ = "STUB: not implemented"
	// Start span and write to header
	return *new(client.WorkflowRun), nil
}

func (t *tracingClientOutboundInterceptor) SignalWorkflow(ctx context.Context, in *ClientSignalWorkflowInput) error {
	_ = "STUB: not implemented"
	// Only add tracing if enabled
	return nil
}

// Start span and write to header

func (t *tracingClientOutboundInterceptor) SignalWithStartWorkflow(
	ctx context.Context,
	in *ClientSignalWithStartWorkflowInput,
) (client.WorkflowRun, error) {
	_ = "STUB: not implemented"
	// Start span and write to header
	return *new(client.WorkflowRun), nil
}

func (t *tracingClientOutboundInterceptor) QueryWorkflow(
	ctx context.Context,
	in *ClientQueryWorkflowInput,
) (converter.EncodedValue, error) {
	_ = "STUB: not implemented"
	// Only add tracing if enabled
	return *new(converter.EncodedValue), nil
}

// Start span and write to header

func (t *tracingClientOutboundInterceptor) UpdateWorkflow(
	ctx context.Context,
	in *ClientUpdateWorkflowInput,
) (client.WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	// Only add tracing if enabled
	return *new(client.WorkflowUpdateHandle), nil
}

// Start span and write to header

func (t *tracingClientOutboundInterceptor) UpdateWithStartWorkflow(
	ctx context.Context,
	in *ClientUpdateWithStartWorkflowInput,
) (client.WorkflowUpdateHandle, error) {
	_ = "STUB: not implemented"
	// Only add tracing if enabled
	return *new(client.WorkflowUpdateHandle), nil
}

// Start span and write to header

type tracingActivityOutboundInterceptor struct {
	ActivityOutboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingActivityOutboundInterceptor) GetLogger(ctx context.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

type tracingActivityInboundInterceptor struct {
	ActivityInboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingActivityInboundInterceptor) Init(outbound ActivityOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingActivityInboundInterceptor) ExecuteActivity(
	ctx context.Context,
	in *ExecuteActivityInput,
) (interface{}, error) {
	_ = "STUB: not implemented"
	// Start span reading from header
	return nil, nil
}

type tracingWorkflowInboundInterceptor struct {
	WorkflowInboundInterceptorBase
	root        *tracingInterceptor
	spanCounter uint16
	info        *workflow.Info
}

// newIdempotencyKey returns a new idempotency key by incrementing the span counter and interpolating
// this new value into a string that includes the workflow namespace/id/run id and the interceptor type.
func (t *tracingWorkflowInboundInterceptor) newIdempotencyKey() string {
	_ = "STUB: not implemented"
	return ""
}

func (t *tracingWorkflowInboundInterceptor) Init(outbound WorkflowOutboundInterceptor) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingWorkflowInboundInterceptor) ExecuteWorkflow(
	ctx workflow.Context,
	in *ExecuteWorkflowInput,
) (interface{}, error) {
	_ = "STUB: not implemented"
	// Start span reading from header
	return nil, nil
}

func (t *tracingWorkflowInboundInterceptor) HandleSignal(ctx workflow.Context, in *HandleSignalInput) error {
	_ = "STUB: not implemented"
	// Only add tracing if enabled and not replaying
	return nil
}

// Start span reading from header

func (t *tracingWorkflowInboundInterceptor) HandleQuery(
	ctx workflow.Context,
	in *HandleQueryInput,
) (interface{}, error) {
	_ = "STUB: not implemented"
	// Only add tracing if enabled and not replaying
	return nil, nil
}

// Start span reading from header

// We intentionally do not set IdempotencyKey here because queries are not recorded in
// workflow history. When the tracing interceptor's span counter is reset between workflow
// replays, old queries will not be processed which could result in idempotency key
// collisions with other queries or signals.

func (t *tracingWorkflowInboundInterceptor) ValidateUpdate(
	ctx workflow.Context,
	in *UpdateInput,
) error {
	_ = "STUB: not implemented"
	// Only add tracing if enabled and not replaying
	return nil
}

// Start span reading from header

// We intentionally do not set IdempotencyKey here because validation is not run on
// replay. When the tracing interceptor's span counter is reset between workflow
// replays, the validator will not be processed which could result in impotency key
// collisions with other requests.

func (t *tracingWorkflowInboundInterceptor) ExecuteUpdate(
	ctx workflow.Context,
	in *UpdateInput,
) (interface{}, error) {
	_ = "STUB: not implemented"
	// Only add tracing if enabled and not replaying
	return nil, nil
}

// Start span reading from header

// Using operation name "HandleUpdate" to match other SDKs and by consistence with other operations

type tracingWorkflowOutboundInterceptor struct {
	WorkflowOutboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) workflow.Future {
	_ = "STUB: not implemented"
	// Start span writing to header
	return *new(workflow.Future)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteLocalActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) workflow.Future {
	_ = "STUB: not implemented"
	// Start span writing to header
	return *new(workflow.Future)
}

func (t *tracingWorkflowOutboundInterceptor) GetLogger(ctx workflow.Context) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteChildWorkflow(
	ctx workflow.Context,
	childWorkflowType string,
	args ...interface{},
) workflow.ChildWorkflowFuture {
	_ = "STUB: not implemented"
	// Start span writing to header
	return *new(workflow.ChildWorkflowFuture)
}

func (t *tracingWorkflowOutboundInterceptor) SignalExternalWorkflow(
	ctx workflow.Context,
	workflowID string,
	runID string,
	signalName string,
	arg interface{},
) workflow.Future {
	_ = "STUB: not implemented"
	// Start span writing to header if enabled
	return *new(workflow.Future)
}

func (t *tracingWorkflowOutboundInterceptor) SignalChildWorkflow(
	ctx workflow.Context,
	workflowID string,
	signalName string,
	arg interface{},
) workflow.Future {
	_ = "STUB: not implemented"
	// Start span writing to header if enabled
	return *new(workflow.Future)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteNexusOperation(ctx workflow.Context, input ExecuteNexusOperationInput) workflow.NexusOperationFuture {
	_ = "STUB: not implemented"
	// Start span writing to header
	return *new(workflow.NexusOperationFuture)
}

func (t *tracingWorkflowOutboundInterceptor) NewContinueAsNewError(
	ctx workflow.Context,
	wfn interface{},
	args ...interface{},
) error {
	_ = "STUB: not implemented"
	return nil
}

// Get the current span and write header

type nopSpan struct{}

func (nopSpan) Finish(*TracerFinishSpanOptions) {
	_ = "STUB: not implemented"

	// Span always returned, even in replay. futErr is non-nil on error.
	return
}

func (t *tracingWorkflowOutboundInterceptor) startNonReplaySpan(
	ctx workflow.Context,
	operation string,
	name string,
	dependedOn bool,
	headerWriter func(TracerSpan) error,
) (span TracerSpan, newCtx workflow.Context, futErr workflow.Future) {
	_ = "STUB: not implemented"
	// Noop span if replaying
	return *new(TracerSpan), *new(workflow.Context), *new(workflow.Future)
}

type tracingNexusOperationInboundInterceptor struct {
	NexusOperationInboundInterceptorBase
	root *tracingInterceptor
}

// CancelOperation implements internal.NexusOperationInboundInterceptor.
func (t *tracingNexusOperationInboundInterceptor) CancelOperation(ctx context.Context, input NexusCancelOperationInput) error {
	_ = "STUB: not implemented"
	return nil
}

// Start span reading from header

// StartOperation implements internal.NexusOperationInboundInterceptor.
func (t *tracingNexusOperationInboundInterceptor) StartOperation(ctx context.Context, input NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Start span reading from header

func (t *tracingInterceptor) startSpanFromContext(
	ctx context.Context,
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, context.Context, error) {
	_ = "STUB: not implemented"
	// Try to get parent from context
	return *new(TracerSpan), *new(context.Context), nil
}

func (t *tracingInterceptor) startSpanFromWorkflowContext(
	ctx workflow.Context,
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, workflow.Context, error) {
	_ = "STUB: not implemented"
	return *new(TracerSpan), *new(workflow.Context), nil
}

// Note, this does not put the span on the context
func (t *tracingInterceptor) startSpan(
	ctx interface{ Value(interface{}) interface{} },
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, error) {
	_ = "STUB: not implemented"
	// Get parent span from header if not already present and allowed
	return *new(TracerSpan), nil
}

// If no parent span, try to get from context

// Start the span

// Put span in header if wanted

func (t *tracingInterceptor) headerReader(ctx context.Context) func() (TracerSpanRef, error) {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingInterceptor) headerWriter(ctx context.Context) func(TracerSpan) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingInterceptor) workflowHeaderReader(ctx workflow.Context) func() (TracerSpanRef, error) {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingInterceptor) workflowHeaderWriter(ctx workflow.Context) func(TracerSpan) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingInterceptor) nexusHeaderReader(header nexus.Header) func() (TracerSpanRef, error) {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingInterceptor) nexusHeaderWriter(header nexus.Header) func(TracerSpan) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *tracingInterceptor) readSpanFromHeader(header map[string]*commonpb.Payload) (TracerSpanRef, error) {
	_ = "STUB: not implemented"
	// Get from map
	return *new(TracerSpanRef), nil
}

// Convert from the payload

// Unmarshal

func (t *tracingInterceptor) writeSpanToHeader(span TracerSpan, header map[string]*commonpb.Payload) error {
	_ = "STUB: not implemented"
	// Serialize span to map
	return nil
}

// Convert to payload

// Put on header

func (t *tracingInterceptor) writeSpanToNexusHeader(span TracerSpan, header nexus.Header) error {
	_ = "STUB: not implemented"
	// Serialize span to map
	return nil
}

// Put on header

func (t *tracingInterceptor) readSpanFromNexusHeader(header nexus.Header) (TracerSpanRef, error) {
	_ = "STUB: not implemented"
	return *new(TracerSpanRef), nil
}

func workflowFutureFromErr(ctx workflow.Context, err error) workflow.Future {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

type nexusOperationFuture struct{ workflow.Future }

func (e nexusOperationFuture) GetNexusOperationExecution() workflow.Future {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

type childWorkflowFuture struct{ workflow.Future }

func (e childWorkflowFuture) GetChildWorkflowExecution() workflow.Future {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}

func (e childWorkflowFuture) SignalChildWorkflow(ctx workflow.Context, signalName string, data interface{}) workflow.Future {
	_ = "STUB: not implemented"
	return *new(workflow.Future)
}
