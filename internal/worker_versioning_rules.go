package internal

import (
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// VersioningRamp is an interface for the different strategies of gradual workflow deployments.
	VersioningRamp interface {
		validateRamp() error
	}

	// VersioningRampByPercentage sends a proportion of the traffic to the target Build ID.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningRampByPercentage]
	VersioningRampByPercentage struct {
		// Percentage of traffic with a value in [0,100)
		Percentage float32
	}

	// VersioningAssignmentRule is a BuildID assigment rule for a task queue.
	// Assignment rules only affect new workflows.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningAssignmentRule]
	VersioningAssignmentRule struct {
		// The BuildID of new workflows affected by this rule.
		TargetBuildID string
		// A strategy for gradual workflow deployment.
		Ramp VersioningRamp
	}

	// VersioningAssignmentRuleWithTimestamp contains an assignment rule annotated
	// by the server with its creation time.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningAssignmentRuleWithTimestamp]
	VersioningAssignmentRuleWithTimestamp struct {
		Rule VersioningAssignmentRule
		// The time when the server created this rule.
		CreateTime time.Time
	}

	// VersioningAssignmentRule is a BuildID redirect rule for a task queue.
	// It changes the behavior of currently running workflows and new ones.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningRedirectRule]
	VersioningRedirectRule struct {
		SourceBuildID string
		TargetBuildID string
	}

	// VersioningRedirectRuleWithTimestamp contains a redirect rule annotated
	// by the server with its creation time.
	// WARNING: Worker versioning is currently experimental
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningRedirectRuleWithTimestamp]
	VersioningRedirectRuleWithTimestamp struct {
		Rule VersioningRedirectRule
		// The time when the server created this rule.
		CreateTime time.Time
	}

	// VersioningConflictToken is a conflict token to serialize updates.
	// An update with an old token fails with `serviceerror.FailedPrecondition`.
	// The current token can be obtained with [GetWorkerVersioningRules], or returned by a successful [UpdateWorkerVersioningRules].
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningConflictToken]
	VersioningConflictToken struct {
		token []byte
	}

	// UpdateWorkerVersioningRulesOptions is the input to [Client.UpdateWorkerVersioningRules].
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.UpdateWorkerVersioningRulesOptions]
	UpdateWorkerVersioningRulesOptions struct {
		// The task queue to update the versioning rules of.
		TaskQueue string
		// A conflict token to serialize updates.
		ConflictToken VersioningConflictToken
		Operation     VersioningOperation
	}

	// VersioningOperation is an interface for the different operations that can be
	// performed when updating the worker versioning rules for a task queue.
	//
	// Possible operations are:
	//   - [VersioningOperationInsertAssignmentRule]
	//   - [VersioningOperationReplaceAssignmentRule]
	//   - [VersioningOperationDeleteAssignmentRule]
	//   - [VersioningOperationAddRedirectRule]
	//   - [VersioningOperationReplaceRedirectRule]
	//   - [VersioningOperationDeleteRedirectRule]
	//   - [VersioningOperationCommitBuildID]
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	VersioningOperation interface {
		validateOp() error
	}

	// VersioningOperationInsertAssignmentRule is an operation for UpdateWorkerVersioningRulesOptions
	// that inserts the rule to the list of assignment rules for this Task Queue.
	// The rules are evaluated in order, starting from index 0. The first
	// applicable rule will be applied and the rest will be ignored.
	// By default, the new rule is inserted at the beginning of the list
	// (index 0). If the given index is too larger the rule will be
	// inserted at the end of the list.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationInsertAssignmentRule]
	VersioningOperationInsertAssignmentRule struct {
		RuleIndex int32
		Rule      VersioningAssignmentRule
	}

	// VersioningOperationReplaceAssignmentRule is an operation for UpdateWorkerVersioningRulesOptions
	// that replaces the assignment rule at a given index. By default presence of one
	// unconditional rule, i.e., no hint filter or ramp, is enforced, otherwise
	// the delete operation will be rejected. Set `force` to true to
	// bypass this validation.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationReplaceAssignmentRule]
	VersioningOperationReplaceAssignmentRule struct {
		RuleIndex int32
		Rule      VersioningAssignmentRule
		Force     bool
	}

	// VersioningOperationDeleteAssignmentRule is an operation for UpdateWorkerVersioningRulesOptions
	// that deletes the assignment rule at a given index. By default presence of one
	// unconditional rule, i.e., no hint filter or ramp, is enforced, otherwise
	// the delete operation will be rejected. Set `force` to true to
	// bypass this validation.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationDeleteAssignmentRule]
	VersioningOperationDeleteAssignmentRule struct {
		RuleIndex int32
		Force     bool
	}

	// VersioningOperationAddRedirectRule is an operation for UpdateWorkerVersioningRulesOptions
	// that adds the rule to the list of redirect rules for this Task Queue. There
	// can be at most one redirect rule for each distinct Source BuildID.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationAddRedirectRule]
	VersioningOperationAddRedirectRule struct {
		Rule VersioningRedirectRule
	}

	// VersioningOperationReplaceRedirectRule is an operation for UpdateWorkerVersioningRulesOptions
	// that replaces the routing rule with the given source BuildID.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationReplaceRedirectRule]
	VersioningOperationReplaceRedirectRule struct {
		Rule VersioningRedirectRule
	}

	// VersioningOperationDeleteRedirectRule is an operation for UpdateWorkerVersioningRulesOptions
	// that deletes the routing rule with the given source Build ID.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationDeleteRedirectRule]
	VersioningOperationDeleteRedirectRule struct {
		SourceBuildID string
	}

	// VersioningOperationCommitBuildID is an operation for UpdateWorkerVersioningRulesOptions
	// that completes  the rollout of a BuildID and cleanup unnecessary rules possibly
	// created during a gradual rollout. Specifically, this command will make the following changes
	// atomically:
	//  1. Adds an assignment rule (with full ramp) for the target Build ID at
	//     the end of the list.
	//  2. Removes all previously added assignment rules to the given target
	//     Build ID (if any).
	//  3. Removes any fully-ramped assignment rule for other Build IDs.
	//
	// To prevent committing invalid Build IDs, we reject the request if no
	// pollers have been seen recently for this Build ID. Use the `force`
	// option to disable this validation.
	//
	// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
	//
	// WARNING: Worker versioning is currently experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.VersioningOperationCommitBuildID]
	VersioningOperationCommitBuildID struct {
		TargetBuildID string
		Force         bool
	}
)

// Token
// Returns an internal representation of this token, mostly for debugging purposes.
//
// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
//
// WARNING: Worker versioning is currently experimental
func (c *VersioningConflictToken) Token() []byte { _ = "STUB: not implemented"; return nil }

func (uw *UpdateWorkerVersioningRulesOptions) validateAndConvertToProto(namespace string) (*workflowservice.UpdateWorkerVersioningRulesRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// GetWorkerVersioningOptions is the input to [Client.GetWorkerVersioningRules].
//
// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
//
// WARNING: Worker versioning is currently experimental
//
// Exposed as: [go.temporal.io/sdk/client.GetWorkerVersioningOptions]
type GetWorkerVersioningOptions struct {
	// The task queue to get the versioning rules from.
	TaskQueue string
}

func (gw *GetWorkerVersioningOptions) validateAndConvertToProto(namespace string) (*workflowservice.GetWorkerVersioningRulesRequest, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// WorkerVersioningRules is the response for [Client.GetWorkerVersioningRules].
//
// Deprecated: Build-id based versioning is deprecated in favor of worker deployment based versioning and will be removed soon.
//
// WARNING: Worker versioning is currently experimental
//
// Exposed as: [go.temporal.io/sdk/client.WorkerVersioningRules]
type WorkerVersioningRules struct {
	AssignmentRules []*VersioningAssignmentRuleWithTimestamp
	RedirectRules   []*VersioningRedirectRuleWithTimestamp
	ConflictToken   VersioningConflictToken
}

func versioningAssignmentRuleToProto(rule *VersioningAssignmentRule) *taskqueuepb.BuildIdAssignmentRule {
	_ = "STUB: not implemented"
	// Assumed `rule` already validated
	return nil
}

func versioningRedirectRuleToProto(rule *VersioningRedirectRule) *taskqueuepb.CompatibleBuildIdRedirectRule {
	_ = "STUB: not implemented"
	// Assumed `rule` already validated
	return nil
}

func versioningAssignmentRuleFromProto(rule *taskqueuepb.BuildIdAssignmentRule, timestamp *timestamppb.Timestamp) *VersioningAssignmentRuleWithTimestamp {
	_ = "STUB: not implemented"
	return nil
}

func versioningRedirectRuleFromProto(rule *taskqueuepb.CompatibleBuildIdRedirectRule, timestamp *timestamppb.Timestamp) *VersioningRedirectRuleWithTimestamp {
	_ = "STUB: not implemented"
	return nil
}

func workerVersioningRulesFromResponse(assignmentRules []*taskqueuepb.TimestampedBuildIdAssignmentRule, redirectRules []*taskqueuepb.TimestampedCompatibleBuildIdRedirectRule, token []byte) *WorkerVersioningRules {
	_ = "STUB: not implemented"
	return nil
}

func workerVersioningRulesFromProtoUpdateResponse(response *workflowservice.UpdateWorkerVersioningRulesResponse) *WorkerVersioningRules {
	_ = "STUB: not implemented"
	return nil
}

func workerVersioningRulesFromProtoGetResponse(response *workflowservice.GetWorkerVersioningRulesResponse) *WorkerVersioningRules {
	_ = "STUB: not implemented"
	return nil
}

func (r *VersioningRampByPercentage) validateRamp() error { _ = "STUB: not implemented"; return nil }

func (r *VersioningAssignmentRule) validateRule() error { _ = "STUB: not implemented"; return nil }

// Ramp is optional, defaults to "nothing to validate"

func (r *VersioningRedirectRule) validateRule() error { _ = "STUB: not implemented"; return nil }

func (u *VersioningOperationInsertAssignmentRule) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}
func (u *VersioningOperationReplaceAssignmentRule) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}
func (u *VersioningOperationDeleteAssignmentRule) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}
func (u *VersioningOperationAddRedirectRule) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}
func (u *VersioningOperationReplaceRedirectRule) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}

func (u *VersioningOperationDeleteRedirectRule) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}

func (u *VersioningOperationCommitBuildID) validateOp() error {
	_ = "STUB: not implemented"
	return nil
}
