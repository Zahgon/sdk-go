package internal

import (
	"go.temporal.io/sdk/log"
)

// ** This is for internal stress testing framework **

// PressurePoints
const (
	pressurePointTypeWorkflowTaskStartTimeout    = "workflow-task-start-timeout"
	pressurePointTypeWorkflowTaskCompleted       = "workflow-task-complete"
	pressurePointTypeActivityTaskScheduleTimeout = "activity-task-schedule-timeout"
	pressurePointTypeActivityTaskStartTimeout    = "activity-task-start-timeout"
	pressurePointConfigProbability               = "probability"
	pressurePointConfigSleep                     = "sleep"
	workerOptionsConfig                          = "worker-options"
	workerOptionsConfigConcurrentPollRoutineSize = "ConcurrentPollRoutineSize"
)

type (
	pressurePointMgr interface {
		Execute(pressurePointName string) error
	}

	pressurePointMgrImpl struct {
		config map[string]map[string]string
		logger log.Logger
	}
)

// newWorkflowWorkerWithPressurePoints returns an instance of a workflow worker.
func newWorkflowWorkerWithPressurePoints(client *WorkflowClient, params workerExecutionParameters, pressurePoints map[string]map[string]string, registry *registry) (worker *workflowWorker) {
	_ = "STUB: not implemented"
	return nil
}

func (p *pressurePointMgrImpl) Execute(pressurePointName string) error {
	_ = "STUB: not implemented"
	return nil
}

// If probability is configured.

// Drop the task.
