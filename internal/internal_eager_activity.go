package internal

import (
	"sync"

	"go.temporal.io/api/workflowservice/v1"
)

// eagerActivityExecutor is a worker-scoped executor for eager activities that
// are returned from workflow task completion responses.
type eagerActivityExecutor struct {
	eagerActivityExecutorOptions

	activityWorker eagerWorker
	heldSlotCount  int
	countLock      sync.Mutex
}

type eagerActivityExecutorOptions struct {
	disabled  bool
	taskQueue string
	// If 0, there is no maximum
	maxConcurrent int
}

// newEagerActivityExecutor creates a new worker-scoped executor without an
// activityWorker set. The activityWorker must be set on the responding executor
// before it will be able to execute activities.
func newEagerActivityExecutor(options eagerActivityExecutorOptions) *eagerActivityExecutor {
	_ = "STUB: not implemented"
	return nil
}

func (e *eagerActivityExecutor) applyToRequest(
	req *workflowservice.RespondWorkflowTaskCompletedRequest,
) []*SlotPermit {
	_ = "STUB: not implemented"
	// Don't allow more than this hardcoded amount per workflow task for now
	return nil
}

// Go over every command checking for activities that can be eagerly executed

// If not present, disabled, not requested, no activity worker, on a
// different task queue, or reached max for task, we must mark as
// explicitly disabled

// If it has been requested, attempt to reserve one pending

func (e *eagerActivityExecutor) reserveOnePendingSlot() *SlotPermit {
	_ = "STUB: not implemented"
	// Confirm that, if we have a max, issued count isn't already there
	return nil
}

// Confirm that, if we have a max, held count isn't already there

// No more room

// Reserve a spot for our request via a non-blocking attempt

// Ensure that on release we decrement the held count

func (e *eagerActivityExecutor) handleResponse(
	resp *workflowservice.RespondWorkflowTaskCompletedResponse,
	reservedPermits []*SlotPermit,
) {
	_ = "STUB: not implemented"
	// Ignore disabled or none present
	return
}

// Give back unfulfilled slots and record for later use

// Release unneeded permits

// Start each activity asynchronously

// Asynchronously execute
