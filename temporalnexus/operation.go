// Package temporalnexus provides utilities for exposing Temporal constructs as Nexus Operations.
//
// Nexus RPC is a modern open-source service framework for arbitrary-length operations whose lifetime may extend beyond
// a traditional RPC. Nexus was designed with durable execution in mind, as an underpinning to connect durable
// executions within and across namespaces, clusters and regions – with a clean API contract to streamline multi-team
// collaboration. Any service can be exposed as a set of sync or async Nexus operations – the latter provides an
// operation identity and a uniform interface to get the status of an operation or its result, receive a completion
// callback, or cancel the operation.
//
// Temporal leverages the Nexus RPC protocol to facilitate calling across namespace and cluster and boundaries.
//
// See also:
//
// Nexus over HTTP Spec: https://github.com/nexus-rpc/api/blob/main/SPEC.md
//
// Nexus Go SDK: https://github.com/nexus-rpc/sdk-go
package temporalnexus

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// OperationInfo contains information about a currently executing Nexus operation.
type OperationInfo = internal.NexusOperationInfo

// IsNexusOperation checks if the context is a Nexus operation context.
func IsNexusOperation(ctx context.Context) bool { _ = "STUB: not implemented"; return false }

// GetOperationInfo returns information about the currently executing Nexus operation.
func GetOperationInfo(ctx context.Context) OperationInfo {
	_ = "STUB: not implemented"
	return *new(OperationInfo)
}

// GetMetricsHandler returns a metrics handler to be used in a Nexus operation's context.
func GetMetricsHandler(ctx context.Context) metrics.Handler {
	_ = "STUB: not implemented"
	return *new(metrics.Handler)
}

// GetLogger returns a logger to be used in a Nexus operation's context.
func GetLogger(ctx context.Context) log.Logger { _ = "STUB: not implemented"; return *new(log.Logger) }

// GetClient returns a client to be used in a Nexus operation's context, this is the same client that the worker was
// created with. Client methods will panic when called from the test environment.
func GetClient(ctx context.Context) client.Client {
	_ = "STUB: not implemented"
	return *new(client.Client)
}

// WorkflowRunOperationOptions are options for [NewWorkflowRunOperationWithOptions].
type WorkflowRunOperationOptions[I, O any] struct {
	// Operation name.
	Name string
	// Workflow function to map this operation to. The operation input maps directly to workflow input.
	// The workflow name is resolved as it would when using this function in client.ExecuteOperation.
	// GetOptions must be provided when setting this option. Mutually exclusive with Handler.
	Workflow func(workflow.Context, I) (O, error)
	// Options for starting the workflow. Must be set if Workflow is set. Mutually exclusive with Handler.
	// The options returned must include a workflow ID that is deterministically generated from the input in order
	// for the operation to be idempotent as the request to start the operation may be retried.
	// TaskQueue is optional and defaults to the current worker's task queue.
	// WorkflowExecutionErrorWhenAlreadyStarted is ignored and always set to true.
	// WorkflowIDConflictPolicy is by default set to fail if a workflow is already running. That is,
	// if a caller executes another operation that starts the same workflow, it will fail. You can set
	// it to WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING to attach the caller's callback to the existing
	// running workflow. This way, all attached callers will be notified when the workflow completes.
	GetOptions func(context.Context, I, nexus.StartOperationOptions) (client.StartWorkflowOptions, error)
	// Handler for starting a workflow with a different input than the operation. Mutually exclusive with Workflow
	// and GetOptions.
	Handler func(context.Context, I, nexus.StartOperationOptions) (WorkflowHandle[O], error)
}

// NOTE: not implementing GetInfo and GetResult just yet, they're not part of the supported methods in Temporal.
type workflowRunOperation[I, O any] struct {
	nexus.UnimplementedOperation[I, O]

	options WorkflowRunOperationOptions[I, O]
}

// NewWorkflowRunOperation maps an operation to a workflow run.
func NewWorkflowRunOperation[I, O any](
	name string,
	workflow func(workflow.Context, I) (O, error),
	getOptions func(context.Context, I, nexus.StartOperationOptions) (client.StartWorkflowOptions, error),
) nexus.Operation[I, O] {
	_ = "STUB: not implemented"
	return nil
}

// NewWorkflowRunOperationWithOptions maps an operation to a workflow run with the given options.
// Returns an error if invalid options are provided.
func NewWorkflowRunOperationWithOptions[I, O any](options WorkflowRunOperationOptions[I, O]) (nexus.Operation[I, O], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// MustNewWorkflowRunOperationWithOptions maps an operation to a workflow run with the given options.
// Panics if invalid options are provided.
func MustNewWorkflowRunOperationWithOptions[I, O any](options WorkflowRunOperationOptions[I, O]) nexus.Operation[I, O] {
	_ = "STUB: not implemented"
	return nil
}

func (*workflowRunOperation[I, O]) Cancel(ctx context.Context, token string, options nexus.CancelOperationOptions) error {
	_ = "STUB: not implemented"
	// Prevent the test env client from panicking when we try to use it from a workflow run operation.
	return nil
}

func (o *workflowRunOperation[I, O]) Name() string { _ = "STUB: not implemented"; return "" }

// Start begins an async Nexus operation backed by a workflow.
// The Operation ID returned in the response should not be modified because it is used for cancelation and reporting
// completion.
func (o *workflowRunOperation[I, O]) Start(
	ctx context.Context,
	input I,
	options nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[O], error) {
	_ = "STUB: not implemented"
	// Prevent the test env client from panicking when we try to use it from a workflow run operation.
	return nil, nil
}

// WorkflowHandle is a readonly representation of a workflow run backing a Nexus operation.
// It's created via the [ExecuteWorkflow] and [ExecuteUntypedWorkflow] methods.
type WorkflowHandle[T any] interface {
	// ID is the workflow's ID.
	ID() string
	// ID is the workflow's run ID.
	RunID() string

	/* Methods below intentionally not exposed, interface is not meant to be implementable outside of this package */

	// Link to the WorkflowExecutionStarted event of the workflow represented by this handle.
	link() nexus.Link
	token() string // Cached operation token

	// typeMarker is a no-op method to associate the generic type T with the interface.
	typeMarker(T)
}

type workflowHandle[T any] struct {
	namespace   string
	id          string
	runID       string
	wfEventLink *common.Link
	cachedToken string
}

func (h workflowHandle[T]) ID() string { _ = "STUB: not implemented"; return "" }

func (h workflowHandle[T]) RunID() string { _ = "STUB: not implemented"; return "" }

func (h workflowHandle[T]) link() nexus.Link {
	_ = "STUB: not implemented"
	// Create the link information about the workflow and return to the caller.
	return *new(nexus.Link)
}

func (h workflowHandle[T]) token() string { _ = "STUB: not implemented"; return "" }

func (h workflowHandle[T]) typeMarker(T) {
	_ = "STUB: not implemented"

	// ExecuteWorkflow starts a workflow run for a [WorkflowRunOperationOptions] Handler, linking the execution chain to a
	// Nexus operation (subsequent runs started from continue-as-new and retries).
	// Automatically propagates the callback and request ID from the nexus options to the workflow.
	return
}

func ExecuteWorkflow[I, O any, WF func(workflow.Context, I) (O, error)](
	ctx context.Context,
	nexusOptions nexus.StartOperationOptions,
	startWorkflowOptions client.StartWorkflowOptions,
	workflow WF,
	arg I,
) (WorkflowHandle[O], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ExecuteUntypedWorkflow starts a workflow with by function reference or string name, linking the execution chain to a
// Nexus operation.
// Useful for invoking workflows that don't follow the single argument - single return type signature.
// See [ExecuteWorkflow] for more information.
func ExecuteUntypedWorkflow[R any](
	ctx context.Context,
	nexusOptions nexus.StartOperationOptions,
	startWorkflowOptions client.StartWorkflowOptions,
	workflow any,
	args ...any,
) (WorkflowHandle[R], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// This field is expected to be populated by servers older than 1.27.0.

// Links are duplicated in startWorkflowOptions to backwards compatibility with older servers that
// don't support links in callbacks.

// This makes sure that ExecuteWorkflow will respect the WorkflowIDConflictPolicy, ie., if the
// conflict policy is to fail (default value), then ExecuteWorkflow will return an error if the
// workflow already running. For Nexus, this ensures that operation has only started successfully
// when the callback has been attached to the workflow (new or existing running workflow).

func convertNexusLinks(nexusLinks []nexus.Link, log log.Logger) ([]*common.Link, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// TODO: forward Link_NexusOperation variants once frontend validateLinks accepts them.
