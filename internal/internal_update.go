package internal

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/sdk/converter"
)

type updateState string

// WorkflowUpdateStage indicates the stage of an update request.
type WorkflowUpdateStage int

const (
	// WorkflowUpdateStageUnspecified indicates the wait stage was not specified
	WorkflowUpdateStageUnspecified WorkflowUpdateStage = iota
	// WorkflowUpdateStageAdmitted indicates the update is admitted
	WorkflowUpdateStageAdmitted
	// WorkflowUpdateStageAccepted indicates the update is accepted
	WorkflowUpdateStageAccepted
	// WorkflowUpdateStageCompleted indicates the update is completed
	WorkflowUpdateStageCompleted
)

const (
	updateStateNew              updateState = "New"
	updateStateRequestInitiated updateState = "RequestScheduled"
	updateStateAccepted         updateState = "Accepted"
	updateStateCompleted        updateState = "Completed"

	updateProtocolV1 = "temporal.api.update.v1"
)

type (
	// UpdateCallbacks supplies callbacks for the different stages of processing
	// a workflow update.
	UpdateCallbacks interface {
		// Accept is called for an update after it has passed validation an
		// before execution has started.
		Accept()

		// Reject is called for an update if validation fails.
		Reject(err error)

		// Complete is called for an update with the result of executing the
		// update function. If the provided error is non-nil then the overall
		// outcome is understood to be a failure.
		Complete(success interface{}, err error)
	}

	// UpdateScheduler allows an update state machine to spawn coroutines and
	// yield itself as necessary.
	UpdateScheduler interface {
		// Spawn starts a new named coroutine, executing the given function f.
		Spawn(ctx Context, name string, highPriority bool, f func(ctx Context)) Context

		// Yield returns control to the scheduler.
		Yield(ctx Context, status string)
	}

	// updateEnv encapsulates the utility functions needed by update protocol
	// instance in order to implement the UpdateCallbacks interface. This
	// interface is conveniently implemented by
	// *workflowExecutionEventHandlerImpl.
	updateEnv interface {
		GetFailureConverter() converter.FailureConverter
		GetDataConverter() converter.DataConverter
		Send(*protocolpb.Message, ...msgSendOpt)
	}

	// updateProtocol wraps an updateEnv and some protocol metadata to
	// implement the UpdateCallbacks abstraction. It handles callbacks by
	// sending protocol messages.
	updateProtocol struct {
		protoInstanceID  string
		clientIdentity   string
		initialRequest   *updatepb.Request
		requestMsgID     string
		requestSeqID     int64
		scheduleUpdate   func(name string, id string, args *commonpb.Payloads, header *commonpb.Header, callbacks UpdateCallbacks)
		env              updateEnv
		state            updateState
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter
	}

	// updateHandler is the underlying type that is registered into a workflow
	// environment when the user-code in a workflow registers an update callback
	// for a given name. It offers the ability to invoke the associated
	// execution and validation functions.
	updateHandler struct {
		fn               interface{}
		validateFn       interface{}
		name             string
		unfinishedPolicy HandlerUnfinishedPolicy
		description      string
		dataConverter    converter.DataConverter
		failureConverter converter.FailureConverter
	}
)

// newUpdateResponder constructs an updateProtocolResponder instance to handle
// update callbacks.
func newUpdateProtocol(
	protoInstanceID string,
	scheduleUpdate func(name string, id string, args *commonpb.Payloads, header *commonpb.Header, callbacks UpdateCallbacks),
	env updateEnv,
) *updateProtocol {
	_ = "STUB: not implemented"
	return nil
}

func (up *updateProtocol) requireState(action string, valid ...updateState) {
	_ = "STUB: not implemented"
	return
}

func (up *updateProtocol) HandleMessage(msg *protocolpb.Message) error {
	_ = "STUB: not implemented"
	return nil
}

// Accept is called for an update after it has passed validation and
// before execution has started.
func (up *updateProtocol) Accept() { _ = "STUB: not implemented"; return }

// Stop holding a reference to the initial request to allow it to be GCed

// Reject is called for an update if validation fails.
func (up *updateProtocol) Reject(err error) { _ = "STUB: not implemented"; return }

// Complete is called for an update with the result of executing the
// update function.
func (up *updateProtocol) Complete(success interface{}, outcomeErr error) {
	_ = "STUB: not implemented"
	return
}

func (up *updateProtocol) checkCompletedEvent(e *historypb.HistoryEvent) bool {
	_ = "STUB: not implemented"
	return false
}

func (up *updateProtocol) checkAcceptedEvent(e *historypb.HistoryEvent) bool {
	_ = "STUB: not implemented"
	return false
}

// defaultHandler receives the initial invocation of an update during WFT
// processing. The implementation will verify that an updateHandler exists for
// the supplied name (rejecting the update otherwise) and use the provided spawn
// function to create a new coroutine that will execute in the workflow context.
// The spawned coroutine is what will actually invoke the user-supplied callback
// functions for validation and execution. Update progress is emitted via calls
// into the UpdateCallbacks parameter.
func defaultUpdateHandler(
	rootCtx Context,
	name string,
	id string,
	serializedArgs *commonpb.Payloads,
	header *commonpb.Header,
	callbacks UpdateCallbacks,
	scheduler UpdateScheduler,
) {
	_ = "STUB: not implemented"
	return
}

// we don't execute update validation during replay so that
// validation routines can change across versions

// If we suspect that handler registration has not occurred (e.g.
// because this update is part of the first workflow task and is being
// delivered before the workflow function itself has run and had a
// chance to register update handlers) then we queue updates
// to allow handler registration to occur. When a handler is registered the
// updates will be scheduled and ran.

// newUpdateHandler instantiates a new updateHandler if the supplied handler and
// opts.Validator functions pass validation of their respective interfaces and
// that the two interfaces are themselves equivalent (allowing for them to
// differ by the presence/absence of a leading Context parameter).
func newUpdateHandler(
	updateName string,
	handler interface{},
	opts UpdateHandlerOptions,
) (*updateHandler, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// validate invokes the update's validation function.
func (h *updateHandler) validate(ctx Context, input []interface{}) (err error) {
	_ = "STUB: not implemented"
	return nil
}

// Don't handle the panic since this error means the workflow state is
// likely corrupted and should be discarded.

// execute executes the update itself.
func (h *updateHandler) execute(ctx Context, input []interface{}) (result interface{}, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// HasCompleted allows the completion status of the update protocol to be
// observed externally.
func (up *updateProtocol) HasCompleted() bool { _ = "STUB: not implemented"; return false }

// validateValidatorFn validates that the supplied interface
//
// 1. is a function
// 2. has exactly one return parameter
// 3. the one return parameter is of type `error`
func validateValidatorFn(fn interface{}) error { _ = "STUB: not implemented"; return nil }

// validateUpdateHandlerFn validates that the supplied interface
//
// 1. is a function
// 2. has at least one parameter, the first of which is of type `workflow.Context`
// 3. has one or two return parameters, the last of which is of type `error`
// 4. if there are two return parameters, the first is a serializable type
func validateUpdateHandlerFn(fn interface{}) error { _ = "STUB: not implemented"; return nil }

func updateLifeCycleStageToProto(l WorkflowUpdateStage) enumspb.UpdateWorkflowExecutionLifecycleStage {
	_ = "STUB: not implemented"
	return *new(enumspb.UpdateWorkflowExecutionLifecycleStage)
}
