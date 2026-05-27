package replaytests

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"

	"go.temporal.io/sdk/workflow"
)

// Workflow1 test workflow
func Workflow1(ctx workflow.Context, name string) error { _ = "STUB: not implemented"; return nil }

// Workflow2 test workflow
func Workflow2(ctx workflow.Context, name string) error { _ = "STUB: not implemented"; return nil }

func helloworldActivity(ctx context.Context, name string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// TimerWf starts a timer and always starts another timer at workflow end, even if cancelled
func TimerWf(ctx workflow.Context) error {
	_ = "STUB: not implemented"

	// Produce another timer after being cancelled
	return nil
}

func LocalActivityWorkflow(ctx workflow.Context, name string) error {
	_ = "STUB: not implemented"
	return nil
}

func ContinueAsNewWorkflow(ctx workflow.Context, continueAsNew bool) error {
	_ = "STUB: not implemented"
	return nil
}

func UpsertMemoWorkflow(ctx workflow.Context, memo string) error {
	_ = "STUB: not implemented"
	return nil
}

func UpsertSearchAttributesWorkflow(ctx workflow.Context, field string) error {
	_ = "STUB: not implemented"
	return nil
}

func SideEffectWorkflow(ctx workflow.Context, field string) error {
	_ = "STUB: not implemented"
	return nil
}

func EmptyWorkflow(ctx workflow.Context, _ string) error { _ = "STUB: not implemented"; return nil }

func DeadlockedWorkflow(ctx workflow.Context, _ string) error {
	_ = "STUB: not implemented"
	// Sleep for just over 1 second to trigger deadlock detection
	return nil
}

func MutableSideEffectWorkflow(ctx workflow.Context) ([]int, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func VersionLoopWorkflow(ctx workflow.Context, changeID string, iterations int) error {
	_ = "STUB: not implemented"
	return nil
}

func VersionLoopWorkflowMultipleTasks(ctx workflow.Context, changeID string, iterations int) error {
	_ = "STUB: not implemented"
	return nil
}

func ChildWorkflowWaitOnSignal(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }

func DuplicateChildWorkflow(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }

func UpdateWorkflow(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }

func UpdateAndExit(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }

// passing a non-zero duration here controls whether the update is
// accepted+completed in the same WFT or accepted in one WFT and
// completed in a subsquent task.

// by waiting on a channel that is closed by a call to update we ensure that
// the update completion and workflow completion commands occur on the same
// WFT completion.

func NonDeterministicUpdate(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }

// The workflow.Sleep below was not commented out when the json
// history was generated. By commenting it out we make the update
// code non-deterministic.
//
//_ = workflow.Sleep(ctx, 1*time.Second)

func VersionAndMutableSideEffectWorkflow(ctx workflow.Context, name string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func generateUUID(ctx workflow.Context, sideEffectID string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func CancelOrderSelectWorkflow(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }

// do something different on cancel error

func ChildWorkflowCancelWithUpdate(ctx workflow.Context) error {
	_ = "STUB: not implemented"
	return nil
}

func MultipleUpdateWorkflow(ctx workflow.Context) (int, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

// Register multiple update handles in the first workflow task to make sure we process an
// update only when its handle is registered, not when any handle is registered

func CounterWorkflow(ctx workflow.Context) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func nonNegative(ctx workflow.Context, i int) error { _ = "STUB: not implemented"; return nil }

func ListAndDescribeWorkflow(ctx workflow.Context) (int, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func SelectorBlockingDefaultWorkflow(ctx workflow.Context) error {
	_ = "STUB: not implemented"
	return nil
}

func SelectorBlockingDefaultActivity(ctx context.Context, value string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func TripWorkflow(ctx workflow.Context, tripCounter int) error {
	_ = "STUB: not implemented"
	return nil
}

// TripCh to wait on trip completed event signals

// TestWorkflowWithChild is a test workflow that executes a child workflow and returns the result from it.
func ResetWorkflowWithChild(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func NexusCancelHandlerWorkflow(ctx workflow.Context, action string) (nexus.NoValue, error) {
	_ = "STUB: not implemented"
	return *new(nexus.NoValue), nil
}

var CancelOp = temporalnexus.NewWorkflowRunOperation(
	"wait-on-signal-op",
	NexusCancelHandlerWorkflow,
	func(ctx context.Context, action string, soo nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		if action == "delay-start" {
			time.Sleep(1 * time.Second)
		}
		return client.StartWorkflowOptions{
			ID: "nexus-handler-wait-for-cancel",
		}, nil
	},
)

func CancelNexusOperationBeforeSentWorkflow(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func CancelNexusOperationBeforeStartWorkflow(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// Wait for scheduled event to be recorded

func CancelNexusOperationAfterStartWorkflow(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func CancelNexusOperationAfterCompleteWorkflow(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// AwaitWithTimeoutNoTimerCancelWorkflow is used to test replay of old workflow histories
// that were created before SDKFlagCancelAwaitTimerOnCondition was introduced.
// In the old behavior, the timer is NOT cancelled when the condition becomes true.
func AwaitWithTimeoutNoTimerCancelWorkflow(ctx workflow.Context) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

func MemoChildWorkflowGob(ctx workflow.Context, input string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// Use strict gob converter - will fail if memo is JSON-encoded

func MemoEncodingWorkflowGob(ctx workflow.Context, memoValue string) (string, error) {
	_ = "STUB: not implemented"
	// Execute a child workflow with memo
	return "", nil
}

// Also upsert memo in the parent workflow

// Use strict gob converter - will fail if memo is JSON-encoded

func MemoChildWorkflowJSON(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// will fail if memo is gob-encoded

func MemoEncodingWorkflowJSON(ctx workflow.Context, memoValue string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// Also upsert memo in the parent workflow

// will fail if memo is gob-encoded

// ScheduleMemoWorkflowJSON is a workflow that validates memo passed from a schedule
// can be decoded with JSON. This is used to test backward compatibility for
// workflows started by schedules before the SDKFlagMemoUserDCEncode flag.
func ScheduleMemoWorkflowJSON(ctx workflow.Context) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// will fail if memo is gob-encoded

func ChannelWorkerWorkflow(ctx workflow.Context) error { _ = "STUB: not implemented"; return nil }
