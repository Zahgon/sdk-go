package internal

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/log"
)

type nexusTaskPoller struct {
	basePoller
	namespace       string
	taskQueueName   string
	identity        string
	service         workflowservice.WorkflowServiceClient
	taskHandler     *nexusTaskHandler
	logger          log.Logger
	numPollerMetric *numPollerMetric
}

type nexusTask struct {
	task *workflowservice.PollNexusTaskQueueResponse
}

var _ taskPoller = &nexusTaskPoller{}

func newNexusTaskPoller(
	taskHandler *nexusTaskHandler,
	service workflowservice.WorkflowServiceClient,
	params workerExecutionParameters,
) *nexusTaskPoller {
	_ = "STUB: not implemented"
	return nil
}

// Poll the nexus task queue and update the num_poller metric
func (ntp *nexusTaskPoller) pollNexusTaskQueue(ctx context.Context, request *workflowservice.PollNexusTaskQueueRequest) (*workflowservice.PollNexusTaskQueueResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (ntp *nexusTaskPoller) poll(ctx context.Context) (taskForWorker, error) {
	_ = "STUB: not implemented"
	return *new(taskForWorker), nil
}

// No operation info is available on empty poll. Emit using base scope.

// PollTask polls a new task
func (ntp *nexusTaskPoller) PollTask() (taskForWorker, error) {
	_ = "STUB: not implemented"
	return *new(taskForWorker), nil
}

// ProcessTask processes a new task
func (ntp *nexusTaskPoller) ProcessTask(task interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// We didn't get a request, poll must have timed out.

// Schedule-to-start (from the time the request hit the frontend).
// Note that this metric does not include the service and operation name as they are not relevant when polling from
// the Nexus task queue.

// context wasn't propagated to us, use a background context.

// Process the nexus task.

// Execution latency (in-SDK processing time).

// Increment failure in all forms of errors:
// Internal error processing the task.
// Failure from user handler.
// Special case for the start response with operation error.

//lint:ignore SA1019 handle legacy operation error format for backward compatibility.

// Failure must contain a NexusHandlerFailureInfo

//lint:ignore SA1019 handle legacy operation error format for backward compatibility.

// Let the poller machinery drop the task, nothing to report back.
// This is only expected due to context deadline errors.

// E2E latency, from frontend until we finished reporting completion.

func (ntp *nexusTaskPoller) reportCompletion(
	completion *workflowservice.RespondNexusTaskCompletedRequest,
	failure *workflowservice.RespondNexusTaskFailedRequest,
) error {
	_ = "STUB: not implemented"
	return nil

	// No workflow or activity tags to report.
	// Task queue expected to be empty for Respond*Task... requests.
}
