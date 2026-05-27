package internal

import (
	"container/list"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/sdk/v1"

	"go.temporal.io/sdk/converter"
)

type (
	commandState int32
	commandType  int32

	commandID struct {
		commandType commandType
		id          string
	}

	commandStateMachine interface {
		getState() commandState
		getID() commandID
		isDone() bool
		getCommand() *commandpb.Command // return nil if there is no command in current state
		cancel()

		handleStartedEvent()
		handleCancelInitiatedEvent()
		handleCanceledEvent()
		handleCancelFailedEvent()
		handleCompletionEvent()
		handleInitiationFailedEvent()
		handleInitiatedEvent()

		handleCommandSent()
		setData(data interface{})
		getData() interface{}
	}

	commandStateMachineBase struct {
		id      commandID
		state   commandState
		history []string
		data    interface{}
		helper  *commandsHelper
	}

	activityCommandStateMachine struct {
		*commandStateMachineBase
		scheduleID    int64
		attributes    *commandpb.ScheduleActivityTaskCommandAttributes
		startMetadata *sdk.UserMetadata
	}

	cancelActivityStateMachine struct {
		*commandStateMachineBase
		attributes *commandpb.RequestCancelActivityTaskCommandAttributes
	}

	timerCommandStateMachine struct {
		*commandStateMachineBase
		attributes    *commandpb.StartTimerCommandAttributes
		startMetadata *sdk.UserMetadata
	}

	cancelTimerCommandStateMachine struct {
		*commandStateMachineBase
		attributes *commandpb.CancelTimerCommandAttributes
	}

	childWorkflowCommandStateMachine struct {
		*commandStateMachineBase
		attributes    *commandpb.StartChildWorkflowExecutionCommandAttributes
		startMetadata *sdk.UserMetadata
	}

	naiveCommandStateMachine struct {
		*commandStateMachineBase
		command *commandpb.Command
	}

	// only possible state transition is: CREATED->SENT->INITIATED->COMPLETED
	cancelExternalWorkflowCommandStateMachine struct {
		*naiveCommandStateMachine
	}

	signalExternalWorkflowCommandStateMachine struct {
		*naiveCommandStateMachine
	}

	// only possible state transition is: CREATED->SENT->COMPLETED
	markerCommandStateMachine struct {
		*naiveCommandStateMachine
	}

	// completeOnSendStateMachine is a generic state machine that transition
	// into a comleted state immediately upon a command being sent (i.e. upon
	// handleCommandSent() being called).
	completeOnSendStateMachine struct {
		*naiveCommandStateMachine
	}

	modifyPropertiesCommandStateMachine struct {
		*naiveCommandStateMachine
	}

	// nexusOperationStateMachine is the state machine for the NexusOperation lifecycle.
	// It may never transition to the started state if the operation completes synchronously.
	// Valid transitions:
	// commandStateCreated -> commandStateCommandSent
	// commandStateCommandSent - (NexusOperationScheduled) -> commandStateInitiated
	// commandStateInitiated - (NexusOperationStarted) -> commandStateStarted
	// commandStateInitiated - (NexusOperation(Completed|Failed|Canceled|TimedOut)) -> commandStateCompleted
	// commandStateStarted - (NexusOperation(Completed|Failed|Canceled|TimedOut)) -> commandStateCompleted
	nexusOperationStateMachine struct {
		*commandStateMachineBase
		// Unique sequence number for identifying this machine SDK side.
		seq int64
		// Event ID of the NexusOperationScheduled event for correlating progress events with this machine.
		scheduledEventID int64
		attributes       *commandpb.ScheduleNexusOperationCommandAttributes
		// Instead of tracking cancelation as a state, we track it as a separate dimension with the request-cancel state
		// machine.
		cancelation   *requestCancelNexusOperationStateMachine
		startMetadata *sdk.UserMetadata
	}

	// requestCancelNexusOperationStateMachine is the state machine for the RequestCancelNexusOperation command.
	// Valid transitions:
	// commandStateCreated -> commandStateCommandSent
	// commandStateCommandSent - (NexusOperationCancelRequested) -> commandStateInitiated
	// commandStateInitiated - (NexusOperationCancelRequest(Completed|Failed)) -> commandStateCompleted
	requestCancelNexusOperationStateMachine struct {
		*commandStateMachineBase
		attributes *commandpb.RequestCancelNexusOperationCommandAttributes
	}

	versionMarker struct {
		changeID          string
		searchAttrUpdated bool
	}

	commandsHelper struct {
		nextCommandEventID int64
		orderedCommands    *list.List
		commands           map[commandID]*list.Element

		scheduledEventIDToActivityID     map[int64]string
		scheduledEventIDToCancellationID map[int64]string
		scheduledEventIDToSignalID       map[int64]string
		versionMarkerLookup              map[int64]versionMarker

		// A mapping of scheduled event ID to a sequence.
		scheduledEventIDToNexusSeq map[int64]int64
		// A list containing all nexus operation machines that have not yet been assigned a scheduled event ID.
		// Every new operation state machine is added to this list on creation and deleted once the scheduled event is
		// seen or the operation was deleted before sending the command.
		// This mechanism is based on Core SDK
		// (https://github.com/temporalio/sdk-core/blob/16c7a33dc1aec8fafb33c9ad6f77569a3dacc8ea/core/src/worker/workflow/machines/workflow_machines.rs#L837).
		nexusOperationsWithoutScheduledID *list.List
	}

	// panic when command or message state machine is in illegal state
	stateMachineIllegalStatePanic struct {
		message string
	}

	// Error returned when a child workflow with the same id already exists and hasn't completed
	// and been removed from internal state.
	childWorkflowExistsWithId struct {
		id string
	}
)

const (
	commandStateCreated                               commandState = 0
	commandStateCommandSent                           commandState = 1
	commandStateCanceledBeforeInitiated               commandState = 2
	commandStateInitiated                             commandState = 3
	commandStateStarted                               commandState = 4
	commandStateCanceledAfterInitiated                commandState = 5
	commandStateCanceledAfterStarted                  commandState = 6
	commandStateCancellationCommandSent               commandState = 7
	commandStateCompletedAfterCancellationCommandSent commandState = 8
	commandStateCompleted                             commandState = 9
	commandStateCanceledBeforeSent                    commandState = 10
	commandStateCancellationCommandAccepted           commandState = 11
)

const (
	commandTypeActivity                    commandType = 0
	commandTypeChildWorkflow               commandType = 1
	commandTypeCancellation                commandType = 2
	commandTypeMarker                      commandType = 3
	commandTypeTimer                       commandType = 4
	commandTypeSignal                      commandType = 5
	commandTypeUpsertSearchAttributes      commandType = 6
	commandTypeCancelTimer                 commandType = 7
	commandTypeRequestCancelActivityTask   commandType = 8
	commandTypeAcceptWorkflowUpdate        commandType = 9
	commandTypeCompleteWorkflowUpdate      commandType = 10
	commandTypeModifyProperties            commandType = 11
	commandTypeRejectWorkflowUpdate        commandType = 12
	commandTypeProtocolMessage             commandType = 13
	commandTypeNexusOperation              commandType = 14
	commandTypeRequestCancelNexusOperation commandType = 15
)

const (
	eventCancel                                   = "cancel"
	eventCommandSent                              = "handleCommandSent"
	eventInitiated                                = "handleInitiatedEvent"
	eventInitiationFailed                         = "handleInitiationFailedEvent"
	eventStarted                                  = "handleStartedEvent"
	eventCompletion                               = "handleCompletionEvent"
	eventCancelInitiated                          = "handleCancelInitiatedEvent"
	eventCancelFailed                             = "handleCancelFailedEvent"
	eventCanceled                                 = "handleCanceledEvent"
	eventExternalWorkflowExecutionCancelRequested = "handleExternalWorkflowExecutionCancelRequested"
)

const (
	sideEffectMarkerName        = "SideEffect"
	versionMarkerName           = "Version"
	localActivityMarkerName     = "LocalActivity"
	mutableSideEffectMarkerName = "MutableSideEffect"

	sideEffectMarkerIDName            = "side-effect-id"
	sideEffectMarkerDataName          = "data"
	versionMarkerChangeIDName         = "change-id"
	versionMarkerDataName             = "version"
	versionSearchAttributeUpdatedName = "version-search-attribute-updated"
	localActivityMarkerDataName       = "data"
	localActivityResultName           = "result"
	mutableSideEffectCallCounterName  = "mutable-side-effect-call-counter"
)

func (d commandState) String() string { _ = "STUB: not implemented"; return "" }

func (d commandType) String() string { _ = "STUB: not implemented"; return "" }

func (d commandID) String() string { _ = "STUB: not implemented"; return "" }

func makeCommandID(commandType commandType, id string) commandID {
	_ = "STUB: not implemented"
	return *new(commandID)
}

func (h *commandsHelper) newCommandStateMachineBase(commandType commandType, id string) *commandStateMachineBase {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newActivityCommandStateMachine(
	scheduleID int64,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
	startMetadata *sdk.UserMetadata,
) *activityCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newCancelActivityStateMachine(attributes *commandpb.RequestCancelActivityTaskCommandAttributes) *cancelActivityStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newNexusOperationStateMachine(
	seq int64,
	attributes *commandpb.ScheduleNexusOperationCommandAttributes,
	startMetadata *sdk.UserMetadata,
) *nexusOperationStateMachine {
	_ = "STUB: not implemented"
	return nil
}

// scheduledEventID will be assigned by the server when the corresponding event comes in.

func (h *commandsHelper) newRequestCancelNexusOperationStateMachine(attributes *commandpb.RequestCancelNexusOperationCommandAttributes) *requestCancelNexusOperationStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newTimerCommandStateMachine(
	attributes *commandpb.StartTimerCommandAttributes,
	startMetadata *sdk.UserMetadata,
) *timerCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newCancelTimerCommandStateMachine(attributes *commandpb.CancelTimerCommandAttributes) *cancelTimerCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newChildWorkflowCommandStateMachine(
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
	startMetadata *sdk.UserMetadata,
) *childWorkflowCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newNaiveCommandStateMachine(commandType commandType, id string, command *commandpb.Command) *naiveCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newMarkerCommandStateMachine(id string, attributes *commandpb.RecordMarkerCommandAttributes, userMetadata *sdk.UserMetadata) *markerCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newCancelExternalWorkflowStateMachine(attributes *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes, cancellationID string) *cancelExternalWorkflowCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newSignalExternalWorkflowStateMachine(attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes, signalID string) *signalExternalWorkflowCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newUpsertSearchAttributesStateMachine(attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes, upsertID string) *completeOnSendStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) newModifyPropertiesStateMachine(
	attributes *commandpb.ModifyWorkflowPropertiesCommandAttributes,
	changeID string,
) *modifyPropertiesCommandStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (d *commandStateMachineBase) getState() commandState {
	_ = "STUB: not implemented"
	return *new(commandState)
}

func (d *commandStateMachineBase) getID() commandID {
	_ = "STUB: not implemented"
	return *new(commandID)
}

func (d *commandStateMachineBase) isDone() bool { _ = "STUB: not implemented"; return false }

func (d *commandStateMachineBase) setData(data interface{}) { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) getData() interface{} { _ = "STUB: not implemented"; return nil }

func (d *commandStateMachineBase) moveState(newState commandState, event string) {
	_ = "STUB: not implemented"
	return
}

func (d stateMachineIllegalStatePanic) String() string { _ = "STUB: not implemented"; return "" }

func panicIllegalState(message string) { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) failStateTransition(event string) {
	_ = "STUB: not implemented"
	// this is when we detect illegal state transition, likely due to ill history sequence or nondeterministic workflow code
	return
}

func (d *commandStateMachineBase) handleCommandSent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) cancel() { _ = "STUB: not implemented"; return }

// No op. This is legit. People could cancel context after timer/activity is done.

// No op. Already canceled.

func (d *commandStateMachineBase) handleInitiatedEvent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) handleInitiationFailedEvent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) handleStartedEvent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) handleCompletionEvent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) handleCancelInitiatedEvent() { _ = "STUB: not implemented"; return }

// No state change

func (d *commandStateMachineBase) handleCancelFailedEvent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) handleCanceledEvent() { _ = "STUB: not implemented"; return }

func (d *commandStateMachineBase) String() string { _ = "STUB: not implemented"; return "" }

func (d *activityCommandStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (d *activityCommandStateMachine) handleCommandSent() { _ = "STUB: not implemented"; return }

func (d *activityCommandStateMachine) handleCancelFailedEvent() {
	_ = "STUB: not implemented"
	// Request to cancel activity now results in either activity completion, failed, timedout, or canceled
	// Request to cancel itself can never fail and invalid RequestCancelActivity commands results in the
	// entire command being failed.
	return
}

func (d *activityCommandStateMachine) cancel() { _ = "STUB: not implemented"; return }

// We also mark the schedule command as not eager if we haven't sent it yet.
// Server behavior differs on eager vs non-eager when scheduling and
// cancelling during the same task completion. If it has not been sent this
// means we are cancelling at the same time as scheduling which is not
// properly supported for eager activities.

func (d *timerCommandStateMachine) cancel() { _ = "STUB: not implemented"; return }

func (d *timerCommandStateMachine) isDone() bool { _ = "STUB: not implemented"; return false }

func (d *timerCommandStateMachine) handleCommandSent() { _ = "STUB: not implemented"; return }

func (d *cancelActivityStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (d *timerCommandStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (d *cancelTimerCommandStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (d *childWorkflowCommandStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

//lint:ignore SA1019 deprecated namespace field

func (d *childWorkflowCommandStateMachine) handleCommandSent() { _ = "STUB: not implemented"; return }

func (d *childWorkflowCommandStateMachine) handleStartedEvent() { _ = "STUB: not implemented"; return }

func (d *childWorkflowCommandStateMachine) handleInitiatedEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *childWorkflowCommandStateMachine) handleCancelFailedEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *childWorkflowCommandStateMachine) cancel() { _ = "STUB: not implemented"; return }

// A child workflow may be canceled _after_ something like an activity start
// happens inside a simulated goroutine. However, since the state of the
// entire child workflow is recorded based on when it started not when it
// was canceled, we have to move it to the end once canceled to keep the
// expected commands in order of when they actually occurred.

func (d *childWorkflowCommandStateMachine) handleCanceledEvent() { _ = "STUB: not implemented"; return }

// We've sent the command but haven't seen the server accept the cancellation. We must ensure this command hangs
// around, because it is possible for the child workflow to be canceled before we've seen the event.

func (d *childWorkflowCommandStateMachine) handleCompletionEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *childWorkflowCommandStateMachine) handleExternalWorkflowExecutionCancelRequested() {
	_ = "STUB: not implemented"
	return
}

// Now we're really done.

// We should be in the cancellation command sent stage - new state to indicate we have seen the cancel accepted

func (d *naiveCommandStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (d *naiveCommandStateMachine) cancel() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleCompletionEvent() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleInitiatedEvent() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleInitiationFailedEvent() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleStartedEvent() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleCanceledEvent() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleCancelFailedEvent() { _ = "STUB: not implemented"; return }

func (d *naiveCommandStateMachine) handleCancelInitiatedEvent() { _ = "STUB: not implemented"; return }

func (d *cancelExternalWorkflowCommandStateMachine) handleInitiatedEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *cancelExternalWorkflowCommandStateMachine) handleCompletionEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *signalExternalWorkflowCommandStateMachine) handleInitiatedEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *signalExternalWorkflowCommandStateMachine) handleCompletionEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *markerCommandStateMachine) handleCommandSent() {
	_ = "STUB: not implemented"
	// Marker command state machine is considered as completed once command is sent.
	// For SideEffect/Version markers, when the history event is applied, there is no marker command state machine yet
	// because we preload those marker events.
	// For local activity, when we apply the history event, we use it to create the marker state machine, there is no
	// other event to drive it to completed state.
	return
}

func (d *completeOnSendStateMachine) handleCommandSent() {
	_ = "STUB: not implemented"
	// This command is considered as completed once command is sent.
	return
}

func (d *modifyPropertiesCommandStateMachine) handleCommandSent() {
	_ = "STUB: not implemented"
	// This command is considered as completed once command is sent.
	return
}

func (sm *nexusOperationStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

// Only create the command in this state unlike other machines that also create it if canceled before sent.

func (sm *nexusOperationStateMachine) handleStartedEvent() { _ = "STUB: not implemented"; return }

func (sm *nexusOperationStateMachine) handleCompletionEvent() { _ = "STUB: not implemented"; return }

func (sm *nexusOperationStateMachine) cancel() {
	_ = "STUB: not implemented"
	// Already canceled or already completed.
	return
}

// No need to actually send the cancelation, mark the state machine as completed.

func (d *requestCancelNexusOperationStateMachine) getCommand() *commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

func (d *requestCancelNexusOperationStateMachine) handleInitiatedEvent() {
	_ = "STUB: not implemented"
	return
}

func (d *requestCancelNexusOperationStateMachine) handleCompletionEvent() {
	_ = "STUB: not implemented"
	return
}

func newCommandsHelper() *commandsHelper { _ = "STUB: not implemented"; return nil }

func (h *commandsHelper) incrementNextCommandEventID() { _ = "STUB: not implemented"; return }

func (h *commandsHelper) setCurrentWorkflowTaskStartedEventID(workflowTaskStartedEventID int64) {
	_ = "STUB: not implemented"
	// Server always processes the commands in the same order it is generated by client and each command results in
	// corresponding history event after processing. So we can use workflow task started event id + 2 as the offset as
	// workflow task completed event is always the first event in the workflow task followed by events generated from
	// commands. This allows client sdk to deterministically predict history event ids generated by processing of the
	// command. It is possible, notably during workflow cancellation, that commands are generated before the workflow
	// task started event is processed. In this case we need to adjust the nextCommandEventID to account for these unsent
	// commands.git
	return
}

func (h *commandsHelper) getNextID() int64 {
	_ = "STUB: not implemented"
	// First check if we have a GetVersion marker in the lookup map
	return 0
}

func (h *commandsHelper) incrementNextCommandEventIDIfVersionMarker() {
	_ = "STUB: not implemented"
	return
}

// Remove the marker from the lookup map and increment nextCommandEventID by 2 because call to GetVersion
// results in 1 or 2 events in the history.  One is GetVersion marker event for changeID and change version, other
// is UpsertSearchableAttributes to keep track of executions using particular version of code.

// UpsertSearchableAttributes may not have been written if the search attribute was too large.

func (h *commandsHelper) getCommand(id commandID) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) addCommand(command commandStateMachine) { _ = "STUB: not implemented"; return }

// Every time new command is added increment the counter used for generating ID

// This really should not exist, but is unavoidable without totally redesigning the Go SDK to avoid
// doing event number counting. EX: Because a workflow execution cancel requested event calls a callback
// on timers that immediately cancels them, we will queue up a cancel timer command even though that timer firing
// might be in the same workflow task. In practice this only seems to happen during unhandled command events.
func (h *commandsHelper) removeCancelOfResolvedCommand(commandID commandID) {
	_ = "STUB: not implemented"
	// Ensure this isn't misused for non-cancel commands
	return
}

func (h *commandsHelper) moveCommandToBack(command commandStateMachine) {
	_ = "STUB: not implemented"
	return
}

func (h *commandsHelper) scheduleActivityTask(
	scheduleID int64,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
	metadata *sdk.UserMetadata,
) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) requestCancelActivityTask(activityID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleActivityTaskClosed(activityID string, scheduledEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

// If, for whatever reason, we were going to send an activity cancel request, don't do that anymore
// since we already know the activity is resolved.

func (h *commandsHelper) handleActivityTaskScheduled(activityID string, scheduledEventID int64) {
	_ = "STUB: not implemented"
	return
}

func (h *commandsHelper) handleActivityTaskCancelRequested(scheduledEventID int64) {
	_ = "STUB: not implemented"
	return
}

func (h *commandsHelper) handleActivityTaskCanceled(activityID string, scheduledEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) getActivityAndScheduledEventIDs(event *historypb.HistoryEvent) (string, int64) {
	_ = "STUB: not implemented"
	return "", 0
}

func (h *commandsHelper) scheduleNexusOperation(
	seq int64,
	attributes *commandpb.ScheduleNexusOperationCommandAttributes,
	startMetadata *sdk.UserMetadata,
) *nexusOperationStateMachine {
	_ = "STUB: not implemented"
	return nil
}

func (h *commandsHelper) handleNexusOperationScheduled(event *historypb.HistoryEvent) {
	_ = "STUB: not implemented"
	return
}

func (h *commandsHelper) handleNexusOperationStarted(scheduledEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleNexusOperationCompleted(scheduledEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

// We don't need this anymore, the state will not transition after completion.

func (h *commandsHelper) handleNexusOperationCancelRequested(scheduledEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleNexusOperationCancelRequestDelivered(scheduledEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) requestCancelNexusOperation(seq int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

// If we haven't sent the command yet, ensure that it doesn't get mapped to the wrong scheduledEventID.

func (h *commandsHelper) recordVersionMarker(changeID string, version Version, dc converter.DataConverter, searchAttributeWasUpdated bool) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleVersionMarker(eventID int64, changeID string, searchAttrUpdated bool) {
	_ = "STUB: not implemented"
	return
}

// During processing of a workflow task we reorder all GetVersion markers and process them first.
// Keep track of all GetVersion marker events during the processing of workflow task so we can
// generate correct eventIDs for other events during replay.

func (h *commandsHelper) recordSideEffectMarker(sideEffectID int64, data *commonpb.Payloads, dc converter.DataConverter, userMetadata *sdk.UserMetadata) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) recordLocalActivityMarker(activityID string, details map[string]*commonpb.Payloads, failure *failurepb.Failure, metadata *sdk.UserMetadata) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

// LocalActivity marker is added only when it completes and schedule logic never relies on GenerateSequence to
// create a unique activity id like in the case of ExecuteActivity.  This causes the problem as we only perform
// the check to increment counter to account for GetVersion special handling as part of it.  This will result
// in wrong IDs to be generated if there is GetVersion call before local activities.  Explicitly calling getNextID
// to correctly incrementing counter before adding the command.

func (h *commandsHelper) recordMutableSideEffectMarker(mutableSideEffectID string, callCountHint int, data *commonpb.Payloads, dc converter.DataConverter, userMetadata *sdk.UserMetadata) commandStateMachine {
	_ = "STUB: not implemented"
	// In order to avoid duplicate marker IDs, we must append the counter to the
	// user-provided ID
	return *new(commandStateMachine)
}

// startChildWorkflowExecution can return an error in the event that there is already a child wf
// with the same ID which exists as a command in memory. Other SDKs actually will send this command
// to server, and have it reject it - but here the command ID is exactly equal to the child's wf ID,
// and changing that without potentially blowing up backwards compatability is difficult. So we
// return the error eagerly locally, which is at least an improvement on panicking.
func (h *commandsHelper) startChildWorkflowExecution(
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
	startMetadata *sdk.UserMetadata,
) (commandStateMachine, error) {
	_ = "STUB: not implemented"
	return *new(commandStateMachine), nil
}

func (h *commandsHelper) handleStartChildWorkflowExecutionInitiated(workflowID string) {
	_ = "STUB: not implemented"
	return
}

func (h *commandsHelper) handleStartChildWorkflowExecutionFailed(workflowID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) requestCancelExternalWorkflowExecution(namespace, workflowID, runID string, cancellationID string, childWorkflowOnly bool) commandStateMachine {
	_ = "STUB: not implemented"
	return *

	// For cancellation of child workflow only, we do not use cancellation ID
	// since the child workflow cancellation go through the existing child workflow
	// state machine, and we use workflow ID as identifier
	// we also do not use run ID, since child workflow can do continue-as-new
	// which will have different run ID
	// there will be server side validation that target workflow is child workflow
	new(commandStateMachine)
}

// sanity check that cancellation ID is not set

// sanity check that run ID is not set

// targeting child workflow

// For cancellation of external workflow, we have to use cancellation ID
// to identify different cancellation request (command) / response (history event)
// client can also use this code path to cancel its own child workflow, however, there will
// be no server side validation that target workflow is the child

// sanity check that cancellation ID is set

func (h *commandsHelper) handleRequestCancelExternalWorkflowExecutionInitiated(initiatedeventID int64, workflowID, cancellationID string) {
	_ = "STUB: not implemented"
	return
}

// this is cancellation for child workflow only

// this is cancellation for external workflow

func (h *commandsHelper) handleExternalWorkflowExecutionCancelRequested(initiatedeventID int64, workflowID string) (bool, commandStateMachine) {
	_ = "STUB: not implemented"
	return false, *new(commandStateMachine)
}

// this is cancellation for external workflow

func (h *commandsHelper) handleRequestCancelExternalWorkflowExecutionFailed(initiatedeventID int64, workflowID string) (bool, commandStateMachine) {
	_ = "STUB: not implemented"
	return false, *new(commandStateMachine)
}

// this is cancellation for child workflow only

// this is cancellation for external workflow

func (h *commandsHelper) signalExternalWorkflowExecution(
	namespace string,
	workflowID string,
	runID string,
	signalName string,
	input *commonpb.Payloads,
	header *commonpb.Header,
	signalID string,
	childWorkflowOnly bool,
) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) addProtocolMessage(msgID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) upsertSearchAttributes(upsertID string, searchAttr *commonpb.SearchAttributes) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) modifyProperties(changeID string, memo *commonpb.Memo) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleSignalExternalWorkflowExecutionInitiated(initiatedEventID int64, signalID string) {
	_ = "STUB: not implemented"
	return
}

func (h *commandsHelper) handleSignalExternalWorkflowExecutionCompleted(initiatedEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleSignalExternalWorkflowExecutionFailed(initiatedEventID int64) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) getSignalID(initiatedEventID int64) string {
	_ = "STUB: not implemented"
	return ""
}

func (h *commandsHelper) startTimer(
	attributes *commandpb.StartTimerCommandAttributes,
	options TimerOptions,
	dc converter.DataConverter,
) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) cancelTimer(timerID TimerID) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleTimerClosed(timerID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

// If, for whatever reason, we were going to send a timer cancel command, don't do that anymore
// since we already know the timer is resolved.

func (h *commandsHelper) handleTimerStarted(timerID string) { _ = "STUB: not implemented"; return }

func (h *commandsHelper) handleTimerCanceled(timerID string) { _ = "STUB: not implemented"; return }

func (h *commandsHelper) handleChildWorkflowExecutionStarted(workflowID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleChildWorkflowExecutionClosed(workflowID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) handleChildWorkflowExecutionCanceled(workflowID string) commandStateMachine {
	_ = "STUB: not implemented"
	return *new(commandStateMachine)
}

func (h *commandsHelper) getCommands(markAsSent bool) []*commandpb.Command {
	_ = "STUB: not implemented"
	return nil
}

// get next item here as we might need to remove curr in the loop

// remove completed command state machines

func (h *commandsHelper) isCancelExternalWorkflowEventForChildWorkflow(cancellationID string) bool {
	_ = "STUB: not implemented"
	// the cancellationID, i.e. Control in RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
	// will be empty if the event is for child workflow.
	// for cancellation external workflow, Control in RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
	// will have a client generated sequence ID
	return false
}

func (e *childWorkflowExistsWithId) Error() string { _ = "STUB: not implemented"; return "" }
