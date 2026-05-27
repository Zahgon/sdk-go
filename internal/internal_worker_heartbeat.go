package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/sdk/log"
)

// heartbeatManager manages heartbeat workers across namespaces for a client.
type heartbeatManager struct {
	client   *WorkflowClient
	interval time.Duration
	logger   log.Logger

	workersMutex sync.Mutex
	workers      map[string]*sharedNamespaceWorker // namespace -> worker
}

// newHeartbeatManager creates a new heartbeatManager.
func newHeartbeatManager(client *WorkflowClient, interval time.Duration, logger log.Logger) *heartbeatManager {
	_ = "STUB: not implemented"
	return nil
}

// registerWorker registers a worker's heartbeat callback with the shared heartbeat worker for the namespace.
func (m *heartbeatManager) registerWorker(
	worker *AggregatedWorker,
) error {
	_ = "STUB: not implemented"
	return nil
}

// If this is the first worker on the namespace, start a new shared namespace worker.

// unregisterWorker removes a worker's heartbeat callback. If no callbacks remain for the namespace,
// the shared heartbeat worker is stopped.
func (m *heartbeatManager) unregisterWorker(worker *AggregatedWorker) {
	_ = "STUB: not implemented"
	return
}

// sharedNamespaceWorker handles heartbeating for all workers in a specific namespace for a specific client.
type sharedNamespaceWorker struct {
	client    *WorkflowClient
	namespace string
	interval  time.Duration
	logger    log.Logger

	heartbeatCtx    context.Context
	heartbeatCancel context.CancelFunc

	// callbacksMutex should only be unlocked under
	callbacksMutex sync.RWMutex
	callbacks      map[string]func() *workerpb.WorkerHeartbeat // workerInstanceKey -> callback

	stopC    chan struct{}
	stoppedC chan struct{}
	started  atomic.Bool
}

func (hw *sharedNamespaceWorker) run() { _ = "STUB: not implemented"; return }

func (hw *sharedNamespaceWorker) sendHeartbeats() error { _ = "STUB: not implemented"; return nil }

// Server doesn't support heartbeats; return error to stop the worker.

// For other errors, log and continue heartbeating

func (hw *sharedNamespaceWorker) stop() { _ = "STUB: not implemented"; return }

// pollTimeTracker tracks the last successful poll time for each poller type.
type pollTimeTracker struct {
	times sync.Map // pollerType (string) -> time.Time (stored as int64 nanos)
}

func (p *pollTimeTracker) recordPollSuccess(pollerType string) { _ = "STUB: not implemented"; return }

func (p *pollTimeTracker) getLastPollTime(pollerType string) time.Time {
	_ = "STUB: not implemented"
	return *new(time.Time)
}
