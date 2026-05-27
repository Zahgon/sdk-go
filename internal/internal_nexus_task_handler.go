package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/log"
)

// errNexusTaskTimeout is returned when the Nexus task handler times out.
// It is used instead of context.DeadlineExceeded to allow the poller to differentiate between Nexus task handler
// timeout and other errors.
var errNexusTaskTimeout = errors.New("nexus task timeout")

// debugDisableTemporalFailureResponses is an internal debug switch to force the SDK
// to treat GetTemporalFailureResponses() as always returning false.
// This is only intended for testing purposes.
var debugDisableTemporalFailureResponses atomic.Bool

// SetDebugDisableTemporalFailureResponses sets the internal debug flag to disable
// temporal failure response support. This forces the SDK to use legacy error handling.
// This is only intended for testing purposes.
func SetDebugDisableTemporalFailureResponses(disable bool) { _ = "STUB: not implemented"; return }

// getEffectiveTemporalFailureResponses returns the effective value of the
// TemporalFailureResponses capability, taking into account any debug overrides.
func getEffectiveTemporalFailureResponses(serverValue bool) bool {
	_ = "STUB: not implemented"
	return false
}

type nexusTaskHandler struct {
	nexusHandler     nexus.Handler
	identity         string
	namespace        string
	taskQueueName    string
	client           Client
	dataConverter    converter.DataConverter
	failureConverter converter.FailureConverter
	logger           log.Logger
	metricsHandler   metrics.Handler
	registry         *registry
}

func newNexusTaskHandler(
	nexusHandler nexus.Handler,
	identity string,
	namespace string,
	taskQueueName string,
	client Client,
	dataConverter converter.DataConverter,
	failureConverter converter.FailureConverter,
	logger log.Logger,
	metricsHandler metrics.Handler,
	registry *registry,
) *nexusTaskHandler {
	_ = "STUB: not implemented"
	return nil
}

func (h *nexusTaskHandler) Execute(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, *workflowservice.RespondNexusTaskFailedRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (h *nexusTaskHandler) ExecuteContext(nctx *NexusOperationContext, task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, *workflowservice.RespondNexusTaskFailedRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (h *nexusTaskHandler) execute(nctx *NexusOperationContext, task *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexus.HandlerError, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (h *nexusTaskHandler) handleStartOperation(
	ctx context.Context,
	nctx *NexusOperationContext,
	req *nexuspb.StartOperationRequest,
	header nexus.Header,
) (*nexuspb.Response, *nexus.HandlerError, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

// Create a fake lazy value, Temporal server already converts Nexus content into payloads.

// Ensure we don't pass nil values to handlers.

// Convert OperationError to a Temporal failure

// Default to internal error.

// *nexus.HandlerStartOperationResultSync is generic, we can't type switch unfortunately.

func (h *nexusTaskHandler) handleCancelOperation(ctx context.Context, nctx *NexusOperationContext, req *nexuspb.CancelOperationRequest, header nexus.Header) (*nexuspb.Response, *nexus.HandlerError, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

// Support servers older than 1.27.0.
//lint:ignore SA1019 ignore deprecated

// Default to internal error.

func (h *nexusTaskHandler) goContextForTask(nctx *NexusOperationContext, header nexus.Header) (context.Context, context.CancelFunc, *nexus.HandlerError) {
	_ = "STUB: not implemented"
	// Associate the NexusOperationContext with the context.Context used to invoke operations.
	return *new(context.Context), *new(context.CancelFunc), nil
}

func (h *nexusTaskHandler) newNexusOperationContext(response *workflowservice.PollNexusTaskQueueResponse) (*NexusOperationContext, *nexus.HandlerError) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (h *nexusTaskHandler) fillInCompletion(taskToken []byte, res *nexuspb.Response, failureReasonSupport bool) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
	_ = "STUB: not implemented"
	// Handle conversion of Failure to OperationError for backwards compatibility with old servers.
	return nil, nil
}

// Convert to operation error for backwards compatibility.

func (h *nexusTaskHandler) fillInFailure(taskToken []byte, handlerError *nexus.HandlerError, failureReasonSupport bool) (*workflowservice.RespondNexusTaskFailedRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

//lint:ignore SA1019 ignore deprecated

var nexusFailureTypeString = string((&failurepb.Failure{}).ProtoReflect().Descriptor().FullName())
var nexusFailureMetadata = map[string]string{"type": nexusFailureTypeString}

func (h *nexusTaskHandler) errorToFailure(err error) (*nexuspb.Failure, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (h *nexusTaskHandler) temporalFailureToNexusFailure(failure *failurepb.Failure) (*nexuspb.Failure, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (h *nexusTaskHandler) nexusHandlerErrorToProto(handlerErr *nexus.HandlerError) (*nexuspb.HandlerError, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// payloadSerializer is a fake nexus Serializer that uses a data converter to read from an embedded payload instead of
// using the given nexus.Context. Supports only Deserialize.
type payloadSerializer struct {
	converter converter.DataConverter
	payload   *common.Payload
}

func (p *payloadSerializer) Deserialize(_ *nexus.Content, v any) error {
	_ = "STUB: not implemented"
	return nil
}

func (p *payloadSerializer) Serialize(v any) (*nexus.Content, error) {
	_ = "STUB: not implemented"
	return nil,
		// not used - operation outputs are directly serialized to payload.
		nil
}

var emptyReaderNopCloser = io.NopCloser(bytes.NewReader([]byte{}))

// convertKnownErrors converts known errors to corresponding Nexus HandlerError.
func convertKnownErrors(err error) error {
	_ = "STUB: not implemented"
	// Not using errors.As to be consistent ApplicationError checking with the rest of the SDK.
	return nil
}

// convertServiceError converts a serviceerror into a Nexus HandlerError if possible.
// If exposeDetails is true, the error message from the given error is exposed in the converted HandlerError, otherwise,
// a default message with minimal information is attached to the returned error.
// Roughly taken from https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
// and
// https://github.com/grpc-ecosystem/grpc-gateway/blob/a7cf811e6ffabeaddcfb4ff65602c12671ff326e/runtime/errors.go#L56.
func convertServiceError(err error) error { _ = "STUB: not implemented"; return nil }

// Temporal serviceerrors have a Status() method.

// Not a serviceerror, passthrough.

// Note that codes.Unauthenticated, codes.PermissionDenied have Nexus error types but we convert to internal
// because this is not a client auth error and happens when the handler fails to auth with Temporal and should
// be considered retryable.
