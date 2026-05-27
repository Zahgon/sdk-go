package internal

import (
	"context"
	"iter"
	"time"

	activitypb "go.temporal.io/api/activity/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
)

const pollActivityTimeout = 60 * time.Second

type (
	// ClientStartActivityOptions contains configuration parameters for starting an activity execution.
	// ID and TaskQueue are required. At least one of ScheduleToCloseTimeout or StartToCloseTimeout is required.
	// Other parameters are optional.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.StartActivityOptions]
	ClientStartActivityOptions struct {
		// ID - The business identifier of the activity.
		//
		// Required
		ID string
		// TaskQueue - The task queue to schedule the activity on.
		//
		// Required
		TaskQueue string
		// ScheduleToCloseTimeout - Total time that a workflow is willing to wait for an Activity to complete.
		// ScheduleToCloseTimeout limits the total time of an Activity's execution including retries
		// 		(use StartToCloseTimeout to limit the time of a single attempt).
		// The zero value of this uses default value.
		// Either this option or StartToCloseTimeout is required: Defaults to unlimited.
		ScheduleToCloseTimeout time.Duration
		// ScheduleToStartTimeout - Time that the Activity Task can stay in the Task Queue before it is picked up by
		// a Worker. Do not specify this timeout unless using host specific Task Queues for Activity Tasks are being
		// used for routing. In almost all situations that don't involve routing activities to specific hosts, it is
		// better to rely on the default value.
		// ScheduleToStartTimeout is always non-retryable. Retrying after this timeout doesn't make sense, as it would
		// just put the Activity Task back into the same Task Queue.
		//
		// Optional: Defaults to unlimited.
		ScheduleToStartTimeout time.Duration
		// StartToCloseTimeout - Maximum time of a single Activity execution attempt.
		// Note that the Temporal Server doesn't detect Worker process failures directly. It relies on this timeout
		// to detect that an Activity that didn't complete on time. So this timeout should be as short as the longest
		// possible execution of the Activity body. Potentially long running Activities must specify HeartbeatTimeout
		// and call Activity.RecordHeartbeat(ctx, "my-heartbeat") periodically for timely failure detection.
		// Either this option or ScheduleToCloseTimeout is required: Defaults to the ScheduleToCloseTimeout value.
		StartToCloseTimeout time.Duration
		// HeartbeatTimeout - Heartbeat interval. Activity must call Activity.RecordHeartbeat(ctx, "my-heartbeat")
		// before this interval passes after the last heartbeat or the Activity starts.
		HeartbeatTimeout time.Duration
		// ActivityIDConflictPolicy - Defines what to do when trying to start an activity with the same ID as a
		// running activity. Note that it is never valid to have two running instances of the same activity ID.
		// See ActivityIDReusePolicy for handling activity ID duplication with a *closed* activity.
		ActivityIDConflictPolicy enumspb.ActivityIdConflictPolicy
		// ActivityIDReusePolicy - Defines whether to allow re-using an activity ID from a previously closed activity.
		// If the request is denied, the server returns an ActivityExecutionAlreadyStarted error.
		// See ActivityIDConflictPolicy for handling ID duplication with a *running* activity.
		ActivityIDReusePolicy enumspb.ActivityIdReusePolicy
		// RetryPolicy - Specifies how to retry an Activity if an error occurs.
		// More details are available at docs.temporal.io.
		// RetryPolicy is optional. If one is not specified, a default RetryPolicy is provided by the server.
		// The default RetryPolicy provided by the server specifies:
		//  - InitialInterval of 1 second
		//  - BackoffCoefficient of 2.0
		//  - MaximumInterval of 100 x InitialInterval
		//  - MaximumAttempts of 0 (unlimited)
		// To disable retries, set MaximumAttempts to 1.
		// The default RetryPolicy provided by the server can be overridden by the dynamic config.
		RetryPolicy *RetryPolicy
		// TypedSearchAttributes - Specifies Search Attributes that will be attached to the Workflow. Search Attributes are
		// additional indexed information attributed to workflow and used for search and visibility. The search attributes
		// can be used in query of List/Scan/Count workflow APIs. The key and its value type must be registered on Temporal
		// server side. For supported operations on different server versions see [Visibility].
		//
		// Optional: default to none.
		//
		// [Visibility]: https://docs.temporal.io/visibility
		TypedSearchAttributes SearchAttributes
		// Summary is a single-line summary for this activity that will appear in UI/CLI. This can be
		// in single-line Temporal Markdown format.
		//
		// Optional: defaults to none/empty.
		//
		// NOTE: Experimental
		Summary string
		// Details - General fixed details for this workflow execution that will appear in UI/CLI. This can be in
		// Temporal markdown format and can span multiple lines. This is a fixed value on the workflow that cannot be
		// updated. For details that can be updated, use SetCurrentDetails within the workflow.
		//
		// Optional: defaults to none/empty.
		//
		// NOTE: Experimental
		Details string
		// Priority - Optional priority settings that control relative ordering of
		// task processing when tasks are backed up in a queue.
		//
		// WARNING: Task queue priority is currently experimental.
		Priority Priority
	}

	// ClientGetActivityHandleOptions contains input for GetActivityHandle call.
	// ActivityID and RunID are required.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.GetActivityHandleOptions]
	ClientGetActivityHandleOptions struct {
		ActivityID string
		RunID      string
	}

	// ClientListActivitiesOptions contains input for ListActivities call.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.ListActivitiesOptions]
	ClientListActivitiesOptions struct {
		Query string
	}

	// ClientListActivitiesResult contains the result of the ListActivities call.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.ListActivitiesResult]
	ClientListActivitiesResult struct {
		Results iter.Seq2[*ClientActivityExecutionInfo, error]
	}

	// ClientCountActivitiesOptions contains input for CountActivities call.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.CountActivitiesOptions]
	ClientCountActivitiesOptions struct {
		Query string
	}

	// ClientCountActivitiesResult contains the result of the CountActivities call.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.CountActivitiesResult]
	ClientCountActivitiesResult struct {
		Count  int64
		Groups []ClientCountActivitiesAggregationGroup
	}

	// ClientCountActivitiesAggregationGroup contains groups of activities if
	// CountActivityExecutions is grouped by a field.
	// The list might not be complete, and the counts of each group is approximate.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.CountActivitiesAggregationGroup]
	ClientCountActivitiesAggregationGroup struct {
		GroupValues []any
		Count       int64
	}

	// ClientActivityHandle represents a running or completed standalone activity execution.
	// It can be used to get the result, describe, cancel, or terminate the activity.
	//
	// Methods may be added to this interface; implementing it directly is discouraged.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.ActivityHandle]
	ClientActivityHandle interface {
		// GetID returns the ID of the activity this handle points to.
		GetID() string
		// GetRunID returns the run ID that this handle was created with.
		//
		// Handle returned by [client.Client] has it set to run ID of the started execution.
		//
		// Handle returned by client.Client.GetActivityHandle has it set to the provided run ID.
		// If empty run ID was provided, then this function returns empty string and the handle points to the most
		// recent execution with matching activity ID. The run ID of this execution can be retrieved by calling Describe.
		GetRunID() string
		// Get waits until the activity finishes and gets its result. If the activity completes successfully, the result
		// is written to valuePtr and nil is returned. If the activity failed, the failure is returned as an error.
		// If an error is encountered trying to get the activity result, that error is returned.
		Get(ctx context.Context, valuePtr any) error
		// Describe returns detailed information about current state of the activity execution.
		Describe(ctx context.Context, options ClientDescribeActivityOptions) (*ClientActivityExecutionDescription, error)
		// Cancel requests cancellation of the activity.
		Cancel(ctx context.Context, options ClientCancelActivityOptions) error
		// Terminate terminates the activity.
		Terminate(ctx context.Context, options ClientTerminateActivityOptions) error
	}

	// ClientDescribeActivityOptions contains options for ClientActivityHandle.Describe call.
	// For future compatibility, currently unused.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.DescribeActivityOptions]
	ClientDescribeActivityOptions struct{}

	// ClientCancelActivityOptions contains options for ClientActivityHandle.Cancel call.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.CancelActivityOptions]
	ClientCancelActivityOptions struct {
		// Reason is optional description of the reason for cancellation.
		Reason string
	}

	// ClientTerminateActivityOptions contains options for ClientActivityHandle.Terminate call.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.TerminateActivityOptions]
	ClientTerminateActivityOptions struct {
		// Reason is optional description of the reason for cancellation.
		Reason string
	}

	// ClientActivityExecutionInfo contains information about an activity execution.
	// This is returned by ListActivities and embedded in ClientActivityExecutionDescription.
	//
	// NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.ActivityExecutionInfo]
	ClientActivityExecutionInfo struct {
		// Raw PB message this struct was built from. This field is nil in the result of ClientActivityHandle.Describe call - use
		// ClientActivityExecutionDescription.RawExecutionInfo instead.
		RawExecutionListInfo  *activitypb.ActivityExecutionListInfo
		ActivityID            string
		ActivityRunID         string
		ActivityType          string
		ScheduleTime          time.Time
		CloseTime             time.Time
		Status                enumspb.ActivityExecutionStatus
		TypedSearchAttributes SearchAttributes
		TaskQueue             string
		ExecutionDuration     time.Duration
	}

	// ClientActivityExecutionDescription contains detailed information about an activity execution.
	// This is returned by ClientActivityHandle.Describe.
	//
	//	NOTE: Experimental
	//
	// Exposed as: [go.temporal.io/sdk/client.ActivityExecutionDescription]
	ClientActivityExecutionDescription struct {
		ClientActivityExecutionInfo
		// Raw PB message this struct was built from.
		RawExecutionInfo        *activitypb.ActivityExecutionInfo
		RunState                enumspb.PendingActivityState
		LastHeartbeatTime       time.Time
		LastStartedTime         time.Time
		Attempt                 int32
		RetryPolicy             *RetryPolicy
		ExpirationTime          time.Time
		LastWorkerIdentity      string
		CurrentRetryInterval    time.Duration
		LastAttemptCompleteTime time.Time
		NextAttemptScheduleTime time.Time
		LastDeploymentVersion   *WorkerDeploymentVersion
		Priority                Priority
		CanceledReason          string
		dataConverter           converter.DataConverter
		failureConverter        converter.FailureConverter
		inboundPayloadVisitor   PayloadVisitor
		summary                 string
		details                 string
	}

	// clientActivityHandleImpl is the default implementation of ClientActivityHandle.
	clientActivityHandleImpl struct {
		client *WorkflowClient
		id     string
		runID  string
		result *ClientPollActivityResultOutput
	}
)

// HasHeartbeatDetails returns whether heartbeat details are present. Use GetHeartbeatDetails to retrieve them.
func (d *ClientActivityExecutionDescription) HasHeartbeatDetails() bool {
	_ = "STUB: not implemented"
	return false
}

// GetHeartbeatDetails retrieves heartbeat details. Returns ErrNoData if heartbeat details are not present.
// The details are deserialized into provided pointers using the data converter of the client used to make the Describe call.
// Returns error if data conversion fails.
func (d *ClientActivityExecutionDescription) GetHeartbeatDetails(valuePtrs ...any) error {
	_ = "STUB: not implemented"
	return nil
}

// GetLastFailure returns the last failure of the activity execution, using the failure converter of the client used to
// make the Describe call. Returns nil if there was no failure.
func (d *ClientActivityExecutionDescription) GetLastFailure() error {
	_ = "STUB: not implemented"
	return nil
}

// GetSummary returns summary of the activity. See ClientStartActivityOptions.Summary. Returns empty string if there is no summary.
// Uses the data converter of the client used to make the Describe call. Returns error if data conversion fails.
func (d *ClientActivityExecutionDescription) GetSummary() (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// GetDetails returns details of the activity. See ClientStartActivityOptions.Details. Returns empty string if there are no details.
// Uses the data converter of the client used to make the Describe call. Returns error if data conversion fails.
func (d *ClientActivityExecutionDescription) GetDetails() (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func (h *clientActivityHandleImpl) GetID() string { _ = "STUB: not implemented"; return "" }

func (h *clientActivityHandleImpl) GetRunID() string { _ = "STUB: not implemented"; return "" }

func (h *clientActivityHandleImpl) Get(ctx context.Context, valuePtr any) error {
	_ = "STUB: not implemented"
	return nil
}

// repeatedly poll, the loop repeats until there's an outcome

func (h *clientActivityHandleImpl) Describe(ctx context.Context, options ClientDescribeActivityOptions) (*ClientActivityExecutionDescription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (h *clientActivityHandleImpl) Cancel(ctx context.Context, options ClientCancelActivityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (h *clientActivityHandleImpl) Terminate(ctx context.Context, options ClientTerminateActivityOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (wc *WorkflowClient) ExecuteActivity(ctx context.Context, options ClientStartActivityOptions, activity any, args ...any) (ClientActivityHandle, error) {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle), nil
}

// Set header before interceptor run so interceptors can access it

func (wc *WorkflowClient) GetActivityHandle(options ClientGetActivityHandleOptions) ClientActivityHandle {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle)
}

func (wc *WorkflowClient) ListActivities(ctx context.Context, options ClientListActivitiesOptions) (ClientListActivitiesResult, error) {
	_ = "STUB: not implemented"
	return *new(ClientListActivitiesResult), nil
}

func (wc *WorkflowClient) getListActivitiesPage(ctx context.Context, request *workflowservice.ListActivityExecutionsRequest) (*workflowservice.ListActivityExecutionsResponse, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *WorkflowClient) CountActivities(ctx context.Context, options ClientCountActivitiesOptions) (*ClientCountActivitiesResult, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// should never fail, and if it does, leaving nil behind

func (w *workflowClientInterceptor) ExecuteActivity(
	ctx context.Context,
	in *ClientExecuteActivityInput,
) (ClientActivityHandle, error) {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle), nil
}

func (options *ClientStartActivityOptions) validateAndSetInRequest(request *workflowservice.StartActivityExecutionRequest, dataConverter converter.DataConverter) error {
	_ = "STUB: not implemented"
	return nil
}

func (w *workflowClientInterceptor) GetActivityHandle(
	in *ClientGetActivityHandleInput,
) ClientActivityHandle {
	_ = "STUB: not implemented"
	return *new(ClientActivityHandle)
}

func (w *workflowClientInterceptor) PollActivityResult(
	ctx context.Context,
	in *ClientPollActivityResultInput,
) (*ClientPollActivityResultOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (w *workflowClientInterceptor) DescribeActivity(
	ctx context.Context,
	in *ClientDescribeActivityInput,
) (*ClientDescribeActivityOutput, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (w *workflowClientInterceptor) CancelActivity(
	ctx context.Context,
	in *ClientCancelActivityInput,
) error {
	_ = "STUB: not implemented"
	return nil
}

func (w *workflowClientInterceptor) TerminateActivity(
	ctx context.Context,
	in *ClientTerminateActivityInput,
) error {
	_ = "STUB: not implemented"
	return nil
}
