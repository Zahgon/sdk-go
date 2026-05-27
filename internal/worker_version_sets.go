package internal

import (
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// A stand-in for a Build Id for unversioned Workers.
//
// Exposed as: [go.temporal.io/sdk/client.UnversionedBuildID]
const UnversionedBuildID = ""

// VersioningIntent indicates whether the user intends certain commands to be run on
// a compatible worker build ID version or not.
//
// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
//
// Exposed as: [go.temporal.io/sdk/temporal.VersioningIntent]
type VersioningIntent int

const (
	// VersioningIntentUnspecified indicates that the SDK should choose the most sensible default
	// behavior for the type of command, accounting for whether the command will be run on the same
	// task queue as the current worker.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.VersioningIntentUnspecified]
	VersioningIntentUnspecified VersioningIntent = iota
	// VersioningIntentCompatible indicates that the command should run on a worker with compatible
	// version if possible. It may not be possible if the target task queue does not also have
	// knowledge of the current worker's build ID.
	//
	// Deprecated: This has the same effect as [VersioningIntentInheritBuildID], use that instead.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.VersioningIntentCompatible]
	VersioningIntentCompatible
	// VersioningIntentDefault indicates that the command should run on the target task queue's
	// current overall-default build ID.
	//
	// Deprecated: This has the same effect as [VersioningIntentUseAssignmentRules], use that instead.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.VersioningIntentDefault]
	VersioningIntentDefault
	// VersioningIntentInheritBuildID indicates the command should inherit the current Build ID of the
	// Workflow triggering it, and not use Assignment Rules. (Redirect Rules are still applicable)
	// This is the default behavior for commands running on the same Task Queue as the current worker.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.VersioningIntentInheritBuildID]
	VersioningIntentInheritBuildID
	// VersioningIntentUseAssignmentRules indicates the command should use the latest Assignment Rules
	// to select a Build ID independently of the workflow triggering it.
	// This is the default behavior for commands not running on the same Task Queue as the current worker.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// Exposed as: [go.temporal.io/sdk/temporal.VersioningIntentUseAssignmentRules]
	VersioningIntentUseAssignmentRules
)

// TaskReachability specifies which category of tasks may reach a worker on a versioned task queue.
// Used both in a reachability query and its response.
//
// Exposed as: [go.temporal.io/sdk/client.TaskReachability]
type TaskReachability int

const (
	// TaskReachabilityUnspecified indicates the reachability was not specified
	//
	// Exposed as: [go.temporal.io/sdk/client.TaskReachabilityUnspecified]
	TaskReachabilityUnspecified = iota
	// TaskReachabilityNewWorkflows indicates the Build Id might be used by new workflows
	//
	// Exposed as: [go.temporal.io/sdk/client.TaskReachabilityNewWorkflows]
	TaskReachabilityNewWorkflows
	// TaskReachabilityExistingWorkflows indicates the Build Id might be used by open workflows
	// and/or closed workflows.
	//
	// Exposed as: [go.temporal.io/sdk/client.TaskReachabilityExistingWorkflows]
	TaskReachabilityExistingWorkflows
	// TaskReachabilityOpenWorkflows indicates the Build Id might be used by open workflows.
	//
	// Exposed as: [go.temporal.io/sdk/client.TaskReachabilityOpenWorkflows]
	TaskReachabilityOpenWorkflows
	// TaskReachabilityClosedWorkflows indicates the Build Id might be used by closed workflows
	//
	// Exposed as: [go.temporal.io/sdk/client.TaskReachabilityClosedWorkflows]
	TaskReachabilityClosedWorkflows
)

type (
	// UpdateWorkerBuildIdCompatibilityOptions is the input to
	// Client.UpdateWorkerBuildIdCompatibility.
	//
	// Exposed as: [go.temporal.io/sdk/client.UpdateWorkerBuildIdCompatibilityOptions]
	UpdateWorkerBuildIdCompatibilityOptions struct {
		// The task queue to update the version sets of.
		TaskQueue string
		Operation UpdateBuildIDOp
	}

	// UpdateBuildIDOp is an interface for the different operations that can be
	// performed when updating the worker build ID compatibility sets for a task queue.
	//
	// Possible operations are:
	//   - BuildIDOpAddNewIDInNewDefaultSet
	//   - BuildIDOpAddNewCompatibleVersion
	//   - BuildIDOpPromoteSet
	//   - BuildIDOpPromoteIDWithinSet
	UpdateBuildIDOp interface {
		targetedBuildId() string
	}
	//
	// Exposed as: [go.temporal.io/sdk/client.BuildIDOpAddNewIDInNewDefaultSet]
	BuildIDOpAddNewIDInNewDefaultSet struct {
		BuildID string
	}
	//
	// Exposed as: [go.temporal.io/sdk/client.BuildIDOpAddNewCompatibleVersion]
	BuildIDOpAddNewCompatibleVersion struct {
		BuildID                   string
		ExistingCompatibleBuildID string
		MakeSetDefault            bool
	}
	//
	// Exposed as: [go.temporal.io/sdk/client.BuildIDOpPromoteSet]
	BuildIDOpPromoteSet struct {
		BuildID string
	}
	//
	// Exposed as: [go.temporal.io/sdk/client.BuildIDOpPromoteIDWithinSet]
	BuildIDOpPromoteIDWithinSet struct {
		BuildID string
	}
)

// Validates and converts the user's options into the proto request. Namespace must be attached afterward.
func (uw *UpdateWorkerBuildIdCompatibilityOptions) validateAndConvertToProto() (*workflowservice.UpdateWorkerBuildIdCompatibilityRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Exposed as: [go.temporal.io/sdk/client.GetWorkerBuildIdCompatibilityOptions]
type GetWorkerBuildIdCompatibilityOptions struct {
	TaskQueue string
	MaxSets   int
}

// Exposed as: [go.temporal.io/sdk/client.GetWorkerTaskReachabilityOptions]
type GetWorkerTaskReachabilityOptions struct {
	// BuildIDs - The build IDs to query the reachability of. At least one build ID must be provided.
	BuildIDs []string
	// TaskQueues - The task queues with Build IDs defined on them that the request is
	// concerned with.
	//
	// Optional: defaults to all task queues
	TaskQueues []string
	// Reachability - The reachability this request is concerned with.
	//
	// Optional: defaults to all types of reachability
	Reachability TaskReachability
}

// Exposed as: [go.temporal.io/sdk/client.WorkerTaskReachability]
type WorkerTaskReachability struct {
	// BuildIDReachability - map of build IDs and their reachability information
	// May contain an entry with UnversionedBuildID for an unversioned worker
	BuildIDReachability map[string]*BuildIDReachability
}

// Exposed as: [go.temporal.io/sdk/client.BuildIDReachability]
type BuildIDReachability struct {
	// TaskQueueReachable map of task queues and their reachability information.
	TaskQueueReachable map[string]*TaskQueueReachability
	// UnretrievedTaskQueues is a list of task queues not retrieved because the server limits
	// the number that can be queried at once.
	UnretrievedTaskQueues []string
}

// Exposed as: [go.temporal.io/sdk/client.TaskQueueReachability]
type TaskQueueReachability struct {
	// TaskQueueReachability for a worker in a single task queue.
	// If TaskQueueReachability is empty, this worker is considered unreachable in this task queue.
	TaskQueueReachability []TaskReachability
}

// WorkerBuildIDVersionSets is the response for Client.GetWorkerBuildIdCompatibility and represents the sets
// of worker build id based versions.
//
// Exposed as: [go.temporal.io/sdk/client.WorkerBuildIDVersionSets]
type WorkerBuildIDVersionSets struct {
	Sets []*CompatibleVersionSet
}

// Default returns the current overall default version. IE: The one that will be used to start new workflows.
// Returns the empty string if there are no versions present.
func (s *WorkerBuildIDVersionSets) Default() string { _ = "STUB: not implemented"; return "" }

// CompatibleVersionSet represents a set of worker build ids which are compatible with each other.
type CompatibleVersionSet struct {
	BuildIDs []string
}

func workerVersionSetsFromProtoResponse(response *workflowservice.GetWorkerBuildIdCompatibilityResponse) *WorkerBuildIDVersionSets {
	_ = "STUB: not implemented"
	return nil
}

func workerVersionSetsFromProto(sets []*taskqueuepb.CompatibleVersionSet) []*CompatibleVersionSet {
	_ = "STUB: not implemented"
	return nil
}

func workerTaskReachabilityFromProtoResponse(response *workflowservice.GetWorkerTaskReachabilityResponse) *WorkerTaskReachability {
	_ = "STUB: not implemented"
	return nil
}

func buildIDReachabilityFromProto(sets []*taskqueuepb.BuildIdReachability) map[string]*BuildIDReachability {
	_ = "STUB: not implemented"
	return nil
}

func taskQueueReachabilityFromProto(sets []*taskqueuepb.TaskQueueReachability) (map[string]*TaskQueueReachability, []string) {
	_ = "STUB: not implemented"
	return nil, nil
}

func taskReachabilityToProto(r TaskReachability) enumspb.TaskReachability {
	_ = "STUB: not implemented"
	return *new(enumspb.TaskReachability)
}

func taskReachabilityFromProto(r enumspb.TaskReachability) TaskReachability {
	_ = "STUB: not implemented"
	return *new(TaskReachability)
}

func (v *BuildIDOpAddNewIDInNewDefaultSet) targetedBuildId() string {
	_ = "STUB: not implemented"
	return ""
}
func (v *BuildIDOpAddNewCompatibleVersion) targetedBuildId() string {
	_ = "STUB: not implemented"
	return ""
}
func (v *BuildIDOpPromoteSet) targetedBuildId() string { _ = "STUB: not implemented"; return "" }
func (v *BuildIDOpPromoteIDWithinSet) targetedBuildId() string {
	_ = "STUB: not implemented"
	return ""

	// Helper to determine if how the `InheritBuildId` flag for a command should be set based on
	// the user's intent and whether the target task queue matches this worker's task queue.
}

func determineInheritBuildIdFlagForCommand(intent VersioningIntent, workerTq, TargetTq string) bool {
	_ = "STUB: not implemented"
	return false
}

// If the target task queue doesn't match ours, use the default version. Empty target counts
// as matching.
