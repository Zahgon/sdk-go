package internal

import (
	"sync"
	"sync/atomic"

	"go.temporal.io/api/workflowservice/v1"
)

// eagerWorkflowDispatcher is responsible for finding an available worker for an eager workflow task.
type eagerWorkflowDispatcher struct {
	lock               sync.RWMutex
	workersByTaskQueue map[string]map[eagerWorker]struct{}
}

// registerWorker registers a worker that can be used for eager workflow dispatch
func (e *eagerWorkflowDispatcher) registerWorker(worker *workflowWorker) {
	_ = "STUB: not implemented"
	return
}

// deregisterWorker deregister a worker so that it will not be used for eager workflow dispatch
func (e *eagerWorkflowDispatcher) deregisterWorker(worker *workflowWorker) {
	_ = "STUB: not implemented"
	return
}

// applyToRequest updates request if eager workflow dispatch is possible and returns the eagerWorkflowExecutor to use
func (e *eagerWorkflowDispatcher) applyToRequest(request *workflowservice.StartWorkflowExecutionRequest) *eagerWorkflowExecutor {
	_ = "STUB: not implemented"
	// Try every worker that is assigned to the desired task queue.
	return nil
}

// Copy the workers so we can release the lock.

// Attach deployment options if worker has deployment versioning enabled

// eagerWorkflowExecutor is a worker-scoped executor for an eager workflow task.
type eagerWorkflowExecutor struct {
	handledResponse atomic.Bool
	worker          eagerWorker
	permit          *SlotPermit
}

// handleResponse of an eager workflow task from a StartWorkflowExecution request.
func (e *eagerWorkflowExecutor) handleResponse(response *workflowservice.PollWorkflowTaskQueueResponse) {
	_ = "STUB: not implemented"
	return
}

// Asynchronously execute the task

// releaseUnused should be called if the executor cannot be used because no eager task was received.
// It will error if handleResponse was already called, as this would indicate misuse.
func (e *eagerWorkflowExecutor) releaseUnused() { _ = "STUB: not implemented"; return }
