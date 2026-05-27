package metrics

import (
	"google.golang.org/grpc/codes"
)

// RootTags returns a set of base tags for all metrics.
func RootTags(namespace string) map[string]string { _ = "STUB: not implemented"; return nil }

// RPCTags returns a set of tags for RPC calls.
func RPCTags(workflowType, activityType, taskQueueName string) map[string]string {
	_ = "STUB: not implemented"
	return nil
}

// WorkflowTags returns a set of tags for workflows.
func WorkflowTags(workflowType string) map[string]string { _ = "STUB: not implemented"; return nil }

// ActivityTags returns a set of tags for activities.
func ActivityTags(workflowType, activityType, taskQueueName string) map[string]string {
	_ = "STUB: not implemented"
	return nil
}

// LocalActivityTags returns a set of tags for local activities.
func LocalActivityTags(workflowType, activityType string) map[string]string {
	_ = "STUB: not implemented"
	return nil
}

// NexusTags returns a set of tags for Nexus Operations.
func NexusTags(service, operation, taskQueueName string) map[string]string {
	_ = "STUB: not implemented"
	return nil
}

// NexusTaskFailureTags returns a set of tags for Nexus Operation failures.
func NexusTaskFailureTags(reason string) map[string]string { _ = "STUB: not implemented"; return nil }

// TaskQueueTags returns a set of tags for a task queue.
func TaskQueueTags(taskQueue string) map[string]string { _ = "STUB: not implemented"; return nil }

// WorkerTags returns a set of tags for workers.
func WorkerTags(workerType string) map[string]string { _ = "STUB: not implemented"; return nil }

// PollerTags returns a set of tags for pollers.
func PollerTags(pollerType string) map[string]string { _ = "STUB: not implemented"; return nil }

// WorkflowTaskFailedTags returns a set of tags for a workflow task failure.
func WorkflowTaskFailedTags(reason string) map[string]string { _ = "STUB: not implemented"; return nil }

// RequestFailureCodeTags returns a set of tags for a request failure.
func RequestFailureCodeTags(statusCode codes.Code) map[string]string {
	_ = "STUB: not implemented"
	return nil
}

// Annoyingly gRPC defines this, but does not expose it publicly.
func canonicalString(c codes.Code) string { _ = "STUB: not implemented"; return "" }
