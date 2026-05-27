package internal

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/workflowservice/v1"
)

type nexusWorkerOptions struct {
	executionParameters workerExecutionParameters
	client              Client
	workflowService     workflowservice.WorkflowServiceClient
	handler             nexus.Handler
	registry            *registry
}

type nexusWorker struct {
	executionParameters workerExecutionParameters
	workflowService     workflowservice.WorkflowServiceClient
	worker              *baseWorker
	stopC               chan struct{}
}

func newNexusWorker(opts nexusWorkerOptions) (*nexusWorker, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Start the worker.
func (w *nexusWorker) Start() error { _ = "STUB: not implemented"; return nil }

// Stop the worker.
func (w *nexusWorker) Stop() { _ = "STUB: not implemented"; return }
