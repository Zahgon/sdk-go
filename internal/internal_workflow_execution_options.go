package internal

import (
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type (
	// UpdateWorkflowExecutionOptionsRequest is a request for [Client.UpdateWorkflowExecutionOptions].
	//
	// NOTE: Experimental
	UpdateWorkflowExecutionOptionsRequest struct {
		// ID of the workflow.
		WorkflowId string
		// Running execution for a workflow ID. If empty string then it will pick the last running execution.
		RunId string
		// WorkflowExecutionOptionsChanges specifies changes to the options of a workflow execution.
		WorkflowExecutionOptionsChanges WorkflowExecutionOptionsChanges
	}

	// WorkflowExecutionOptions describes options for a workflow execution.
	//
	// NOTE: Experimental
	WorkflowExecutionOptions struct {
		// If set, it takes precedence over the Versioning Behavior provided with code annotations.
		VersioningOverride VersioningOverride
	}

	// WorkflowExecutionOptionsChanges describes changes to the options of a workflow execution in
	// [WorkflowExecutionOptions]. An entry with a `nil` pointer means do not change.
	//
	// NOTE: Experimental
	WorkflowExecutionOptionsChanges struct {
		// If non-nil, change the versioning override.
		VersioningOverride *VersioningOverrideChange
	}

	// VersioningOverrideChange sets or removes a versioning override when used with
	// [WorkflowExecutionOptionsChanges].
	//
	// NOTE: Experimental
	VersioningOverrideChange struct {
		// Set the override entry if non-nil. If nil, remove any previously set override.
		Value VersioningOverride
	}

	// VersioningOverride changes the versioning configuration of a specific workflow execution.
	// If set, it takes precedence over the Versioning Behavior provided with workflow type
	// registration or default worker options.
	//
	// To remove the override, the [UpdateWorkflowExecutionOptionsRequest] should include a pointer
	// to an empty [VersioningOverride] value in [WorkflowExecutionOptionsChanges]. See
	// [WorkflowExecutionOptionsChanges] for details.
	//
	// NOTE: Experimental
	VersioningOverride interface {
		behavior() VersioningBehavior
	}

	// PinnedVersioningOverride means the workflow will be pinned to a specific deployment version.
	//
	// NOTE: Experimental
	PinnedVersioningOverride struct {
		Version WorkerDeploymentVersion
	}

	// AutoUpgradeVersioningOverride means the workflow will auto-upgrade to the current deployment
	// version on the next workflow task.
	//
	// NOTE: Experimental
	AutoUpgradeVersioningOverride struct {
	}

	// OnConflictOptions specifies the actions to be taken when using the workflow ID conflict policy
	// USE_EXISTING.
	//
	// NOTE: Experimental
	OnConflictOptions struct {
		AttachRequestID           bool
		AttachCompletionCallbacks bool
		AttachLinks               bool
	}
)

func (*PinnedVersioningOverride) behavior() VersioningBehavior {
	_ = "STUB: not implemented"
	return *new(VersioningBehavior)
}

func (*AutoUpgradeVersioningOverride) behavior() VersioningBehavior {
	_ = "STUB: not implemented"
	return *new(VersioningBehavior)
}

// Mapping WorkflowExecutionOptions field names to proto ones.
var workflowExecutionOptionsMap map[string]string = map[string]string{
	"VersioningOverride": "versioning_override",
}

func generateWorkflowExecutionOptionsPaths(mask []string) []string {
	_ = "STUB: not implemented"
	return nil
}

func workflowExecutionOptionsMaskToProto(mask []string) *fieldmaskpb.FieldMask {
	_ = "STUB: not implemented"
	return nil
}

func versioningOverrideToProto(versioningOverride VersioningOverride) *workflowpb.VersioningOverride {
	_ = "STUB: not implemented"
	return nil
}

func versioningOverrideFromProto(versioningOverride *workflowpb.VersioningOverride) VersioningOverride {
	_ = "STUB: not implemented"
	return *new(VersioningOverride)
}

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

func workflowExecutionOptionsToProto(options WorkflowExecutionOptions) *workflowpb.WorkflowExecutionOptions {
	_ = "STUB: not implemented"
	return nil
}

func workflowExecutionOptionsChangesToProto(changes WorkflowExecutionOptionsChanges) (*workflowpb.WorkflowExecutionOptions, *fieldmaskpb.FieldMask) {
	_ = "STUB: not implemented"
	return nil, nil
}

func workflowExecutionOptionsFromProtoUpdateResponse(response *workflowservice.UpdateWorkflowExecutionOptionsResponse) WorkflowExecutionOptions {
	_ = "STUB: not implemented"
	return *new(WorkflowExecutionOptions)
}

func (r *UpdateWorkflowExecutionOptionsRequest) validateAndConvertToProto(namespace string) (*workflowservice.UpdateWorkflowExecutionOptionsRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (o *OnConflictOptions) ToProto() *workflowpb.OnConflictOptions {
	_ = "STUB: not implemented"
	return nil
}
