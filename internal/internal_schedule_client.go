package internal

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
)

type (

	// ScheduleClient is the client for starting a workflow execution.
	scheduleClient struct {
		workflowClient         *WorkflowClient
		outboundPayloadVisitor PayloadVisitor
	}

	// scheduleHandleImpl is the implementation of ScheduleHandle.
	scheduleHandleImpl struct {
		ID                     string
		client                 *WorkflowClient
		outboundPayloadVisitor PayloadVisitor
	}

	// scheduleListIteratorImpl is the implementation of ScheduleListIterator
	scheduleListIteratorImpl struct {
		// nextScheduleIndex - Local cached schedules events and corresponding consuming index
		nextScheduleIndex int

		// err - From getting the latest page of schedules
		err error

		// response - From getting the latest page of schedules
		response *workflowservice.ListSchedulesResponse

		// paginate - Function which use a next token to get next page of schedules events
		paginate func(nexttoken []byte) (*workflowservice.ListSchedulesResponse, error)
	}
)

func (w *workflowClientInterceptor) CreateSchedule(ctx context.Context, in *ScheduleClientCreateInput) (ScheduleHandle, error) {
	_ = "STUB: not implemented"
	// This is always set before interceptor is invoked
	return *new(ScheduleHandle), nil
}

// Only send an initial patch if we need to.

// Convert to nil so the server uses the default
// catchup window,otherwise it will use the minimum (10s).

// run propagators to extract information about tracing and other stuff, store in headers field

func (sc *scheduleClient) Create(ctx context.Context, options ScheduleOptions) (ScheduleHandle, error) {
	_ = "STUB: not implemented"
	return *new(ScheduleHandle), nil
}

// Set header before interceptor run

// Run via interceptor

func (sc *scheduleClient) GetHandle(ctx context.Context, scheduleID string) ScheduleHandle {
	_ = "STUB: not implemented"
	return *new(ScheduleHandle)
}

func (sc *scheduleClient) List(ctx context.Context, options ScheduleListOptions) (ScheduleListIterator, error) {
	_ = "STUB: not implemented"
	return *new(ScheduleListIterator), nil
}

func (iter *scheduleListIteratorImpl) HasNext() bool { _ = "STUB: not implemented"; return false }

func (iter *scheduleListIteratorImpl) Next() (*ScheduleListEntry, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (scheduleHandle *scheduleHandleImpl) GetID() string { _ = "STUB: not implemented"; return "" }

func (scheduleHandle *scheduleHandleImpl) Delete(ctx context.Context) error {
	_ = "STUB: not implemented"
	return nil
}

func (scheduleHandle *scheduleHandleImpl) Backfill(ctx context.Context, options ScheduleBackfillOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (scheduleHandle *scheduleHandleImpl) Update(ctx context.Context, options ScheduleUpdateOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (scheduleHandle *scheduleHandleImpl) Describe(ctx context.Context) (*ScheduleDescription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (scheduleHandle *scheduleHandleImpl) Trigger(ctx context.Context, options ScheduleTriggerOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (scheduleHandle *scheduleHandleImpl) Pause(ctx context.Context, options SchedulePauseOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (scheduleHandle *scheduleHandleImpl) Unpause(ctx context.Context, options ScheduleUnpauseOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func convertToPBScheduleSpec(scheduleSpec *ScheduleSpec) *schedulepb.ScheduleSpec {
	_ = "STUB: not implemented"
	return nil
}

// TODO support custom time zone data

func convertFromPBScheduleSpec(scheduleSpec *schedulepb.ScheduleSpec) *ScheduleSpec {
	_ = "STUB: not implemented"
	return nil
}

func scheduleDescriptionFromPB(
	logger log.Logger,
	namespace string,
	dc converter.DataConverter,
	describeResponse *workflowservice.DescribeScheduleResponse,
) (*ScheduleDescription, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertToPBSchedule(ctx context.Context, client *WorkflowClient, schedule *Schedule) (*schedulepb.Schedule, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Only convert non-zero CatchWindow so server treats 0 as nil and uses the default catchup window.

func convertFromPBScheduleListEntry(schedule *schedulepb.ScheduleListEntry) *ScheduleListEntry {
	_ = "STUB: not implemented"
	return nil
}

func convertToPBScheduleAction(
	ctx context.Context,
	client *WorkflowClient,
	scheduleAction ScheduleAction,
) (*schedulepb.ScheduleAction, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Set header before interceptor run

// Default workflow ID

// Validate function and get name

// Encode workflow inputs that may already be encoded

// Encode workflow memos that may already be encoded

// Add any untyped search attributes that aren't already there

// get workflow headers from the context

// TODO maybe just panic instead?

func convertFromPBScheduleAction(
	logger log.Logger,
	namespace string,
	dc converter.DataConverter,
	action *schedulepb.ScheduleAction,
) (ScheduleAction, error) {
	_ = "STUB: not implemented"
	return *new(ScheduleAction), nil
}

// Create untyped list for any attribute not in the existing list

// TODO maybe just panic instead?

func convertToPBBackfillList(backfillRequests []ScheduleBackfill) []*schedulepb.BackfillRequest {
	_ = "STUB: not implemented"
	return nil
}

func convertToPBRangeList(scheduleRange []ScheduleRange) []*schedulepb.Range {
	_ = "STUB: not implemented"
	return nil
}

func convertFromPBRangeList(scheduleRangePB []*schedulepb.Range) []ScheduleRange {
	_ = "STUB: not implemented"
	return nil
}

func convertFromPBScheduleCalendarSpecList(calendarSpecPB []*schedulepb.StructuredCalendarSpec) []ScheduleCalendarSpec {
	_ = "STUB: not implemented"
	return nil
}

func applyScheduleCalendarSpecDefault(calendarSpec *ScheduleCalendarSpec) {
	_ = "STUB: not implemented"
	return
}

func convertToPBScheduleCalendarSpecList(calendarSpec []ScheduleCalendarSpec) []*schedulepb.StructuredCalendarSpec {
	_ = "STUB: not implemented"
	return nil
}

func convertFromPBScheduleActionResultList(aa []*schedulepb.ScheduleActionResult) []ScheduleActionResult {
	_ = "STUB: not implemented"
	return nil
}

func encodeScheduleWorklowArgs(dc converter.DataConverter, args []interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// arg is already encoded

func encodeScheduleWorkflowMemo(dc converter.DataConverter, input map[string]interface{}) (*commonpb.Memo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
