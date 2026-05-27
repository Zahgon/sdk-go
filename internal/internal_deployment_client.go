package internal

import (
	"context"

	"go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
)

type (
	// deploymentClient is the client for managing deployments.
	deploymentClient struct {
		workflowClient *WorkflowClient
	}

	// deploymentListIteratorImpl is the implementation of [DeploymentListIterator].
	// Adapted from [scheduleListIteratorImpl].
	deploymentListIteratorImpl struct {
		// nextDeploymentIndex - Local index to cached deployments.
		nextDeploymentIndex int

		// err - Error from getting the last page of deployments.
		err error

		// response - Last page of deployments from server.
		response *workflowservice.ListDeploymentsResponse

		// paginate - Function to get the next page of deployment from server.
		paginate func(nexttoken []byte) (*workflowservice.ListDeploymentsResponse, error)
	}
)

func (iter *deploymentListIteratorImpl) HasNext() bool { _ = "STUB: not implemented"; return false }

func (iter *deploymentListIteratorImpl) Next() (*DeploymentListEntry, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func deploymentFromProto(deployment *deployment.Deployment) Deployment {
	_ = "STUB: not implemented"
	return *new(Deployment)
}

func deploymentToProto(deploymentID Deployment) *deployment.Deployment {
	_ = "STUB: not implemented"
	return nil
}

func deploymentListEntryFromProto(deployment *deployment.DeploymentListInfo) *DeploymentListEntry {
	_ = "STUB: not implemented"
	return nil
}

func deploymentTaskQueuesInfoFromProto(tqsInfo []*deployment.DeploymentInfo_TaskQueueInfo) []DeploymentTaskQueueInfo {
	_ = "STUB: not implemented"
	return nil
}

func deploymentInfoFromProto(deploymentInfo *deployment.DeploymentInfo) DeploymentInfo {
	_ = "STUB: not implemented"
	return *new(DeploymentInfo)
}

func deploymentDescriptionFromProto(deploymentInfo *deployment.DeploymentInfo) DeploymentDescription {
	_ = "STUB: not implemented"
	return *new(DeploymentDescription)
}

func deploymentReachabilityInfoFromProto(response *workflowservice.GetDeploymentReachabilityResponse) DeploymentReachabilityInfo {
	_ = "STUB: not implemented"
	return *new(DeploymentReachabilityInfo)
}

func deploymentGetCurrentResponseFromProto(deploymentInfo *deployment.DeploymentInfo) DeploymentGetCurrentResponse {
	_ = "STUB: not implemented"
	return *new(DeploymentGetCurrentResponse)
}

func deploymentMetadataUpdateToProto(dc converter.DataConverter, update DeploymentMetadataUpdate) *deployment.UpdateDeploymentMetadata {
	_ = "STUB: not implemented"
	return nil
}

func (dc *deploymentClient) List(ctx context.Context, options DeploymentListOptions) (DeploymentListIterator, error) {
	_ = "STUB: not implemented"
	return *new(DeploymentListIterator), nil
}

func validateDeployment(deployment Deployment) error { _ = "STUB: not implemented"; return nil }

func (dc *deploymentClient) Describe(ctx context.Context, options DeploymentDescribeOptions) (DeploymentDescription, error) {
	_ = "STUB: not implemented"
	return *new(DeploymentDescription), nil
}

func (dc *deploymentClient) GetReachability(ctx context.Context, options DeploymentGetReachabilityOptions) (DeploymentReachabilityInfo, error) {
	_ = "STUB: not implemented"
	return *new(DeploymentReachabilityInfo), nil
}

func (dc *deploymentClient) GetCurrent(ctx context.Context, options DeploymentGetCurrentOptions) (DeploymentGetCurrentResponse, error) {
	_ = "STUB: not implemented"
	return *new(DeploymentGetCurrentResponse), nil
}

func (dc *deploymentClient) SetCurrent(ctx context.Context, options DeploymentSetCurrentOptions) (DeploymentSetCurrentResponse, error) {
	_ = "STUB: not implemented"
	return *new(DeploymentSetCurrentResponse), nil
}
