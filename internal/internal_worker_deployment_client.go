package internal

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A reserved identifier of unversioned workers.
const WorkerDeploymentUnversioned = "__unversioned__"

// A reserved separator for Worker Deployment Versions.
const WorkerDeploymentVersionSeparator = "."

var errBuildIdCantBeEmpty = fmt.Errorf("BuildID cannot be empty")

// safeAsTime ensures that a nil proto timestamp makes `IsZero()` true.
func safeAsTime(timestamp *timestamppb.Timestamp) time.Time {
	_ = "STUB: not implemented"
	return *new(time.Time)
}

type (
	// WorkerDeploymentClient is the client for managing worker deployments.
	workerDeploymentClient struct {
		workflowClient *WorkflowClient
	}

	// workerDeploymentHandleImpl is the implementation of [WorkerDeploymentHandle]
	workerDeploymentHandleImpl struct {
		Name           string
		workflowClient *WorkflowClient
	}

	// workerDeploymentListIteratorImpl is the implementation of [WorkerDeploymentListIterator].
	// Adapted from [scheduleListIteratorImpl].
	workerDeploymentListIteratorImpl struct {
		// nextWorkerDeploymentIndex - Local index to cached deployments.
		nextWorkerDeploymentIndex int

		// err - Error from getting the last page of deployments.
		err error

		// response - Last page of deployments from server.
		response *workflowservice.ListWorkerDeploymentsResponse

		// paginate - Function to get the next page of deployment from server.
		paginate func(nexttoken []byte) (*workflowservice.ListWorkerDeploymentsResponse, error)
	}
)

func (iter *workerDeploymentListIteratorImpl) HasNext() bool {
	_ = "STUB: not implemented"
	return false
}

func (iter *workerDeploymentListIteratorImpl) Next() (*WorkerDeploymentListEntry, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func workerDeploymentRoutingConfigFromProto(routingConfig *deployment.RoutingConfig) WorkerDeploymentRoutingConfig {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentRoutingConfig)
}

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

func workerDeploymentListEntryFromProto(summary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary) *WorkerDeploymentListEntry {
	_ = "STUB: not implemented"
	return nil
}

func workerDeploymentVersionSummariesFromProto(summaries []*deployment.WorkerDeploymentInfo_WorkerDeploymentVersionSummary) []WorkerDeploymentVersionSummary {
	_ = "STUB: not implemented"
	return nil
}

//lint:ignore SA1019 ignore deprecated versioning APIs

// Shouldn't receive any summary like this

func workerDeploymentInfoFromProto(info *deployment.WorkerDeploymentInfo) WorkerDeploymentInfo {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentInfo)
}

func (h *workerDeploymentHandleImpl) validate() error { _ = "STUB: not implemented"; return nil }

func (h *workerDeploymentHandleImpl) buildIdToVersionStr(buildId string) string {
	_ = "STUB: not implemented"
	return ""
}

func (h *workerDeploymentHandleImpl) Describe(ctx context.Context, options WorkerDeploymentDescribeOptions) (WorkerDeploymentDescribeResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentDescribeResponse), nil
}

func (h *workerDeploymentHandleImpl) SetCurrentVersion(ctx context.Context, options WorkerDeploymentSetCurrentVersionOptions) (WorkerDeploymentSetCurrentVersionResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentSetCurrentVersionResponse), nil
}

//lint:ignore SA1019 ignore deprecated versioning APIs

func (h *workerDeploymentHandleImpl) SetRampingVersion(ctx context.Context, options WorkerDeploymentSetRampingVersionOptions) (WorkerDeploymentSetRampingVersionResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentSetRampingVersionResponse), nil
}

//lint:ignore SA1019 ignore deprecated versioning APIs

//lint:ignore SA1019 ignore deprecated versioning APIs

func (h *workerDeploymentHandleImpl) SetManagerIdentity(ctx context.Context, options WorkerDeploymentSetManagerIdentityOptions) (WorkerDeploymentSetManagerIdentityResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentSetManagerIdentityResponse), nil
}

//lint:ignore SA1019 ignore deprecated versioning APIs

func workerDeploymentTaskQueuesInfosFromProto(tqInfos []*deployment.WorkerDeploymentVersionInfo_VersionTaskQueueInfo) []WorkerDeploymentTaskQueueInfo {
	_ = "STUB: not implemented"
	return nil
}

func workerDeploymentDrainageInfoFromProto(drainageInfo *deployment.VersionDrainageInfo) *WorkerDeploymentVersionDrainageInfo {
	_ = "STUB: not implemented"
	return nil
}

func workerDeploymentVersionInfoFromProto(info *deployment.WorkerDeploymentVersionInfo) WorkerDeploymentVersionInfo {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentVersionInfo)
}

//lint:ignore SA1019 ignore deprecated versioning APIs

// Should never happen unless server is sending junk data

func (h *workerDeploymentHandleImpl) DescribeVersion(ctx context.Context, options WorkerDeploymentDescribeVersionOptions) (WorkerDeploymentVersionDescription, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentVersionDescription), nil
}

func (h *workerDeploymentHandleImpl) DeleteVersion(ctx context.Context, options WorkerDeploymentDeleteVersionOptions) (WorkerDeploymentDeleteVersionResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentDeleteVersionResponse), nil
}

func workerDeploymentUpsertEntriesMetadataToProto(dc converter.DataConverter, update WorkerDeploymentMetadataUpdate) map[string]*common.Payload {
	_ = "STUB: not implemented"
	return nil
}

func (h *workerDeploymentHandleImpl) UpdateVersionMetadata(ctx context.Context, options WorkerDeploymentUpdateVersionMetadataOptions) (WorkerDeploymentUpdateVersionMetadataResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentUpdateVersionMetadataResponse), nil
}

func (wdc *workerDeploymentClient) List(ctx context.Context, options WorkerDeploymentListOptions) (WorkerDeploymentListIterator, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentListIterator), nil
}

func (wdc *workerDeploymentClient) Delete(ctx context.Context, options WorkerDeploymentDeleteOptions) (WorkerDeploymentDeleteResponse, error) {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentDeleteResponse), nil
}

func (wdc *workerDeploymentClient) GetHandle(name string) WorkerDeploymentHandle {
	_ = "STUB: not implemented"
	return *new(WorkerDeploymentHandle)
}
