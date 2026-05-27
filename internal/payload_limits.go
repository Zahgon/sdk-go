package internal

import (
	"context"
	"sync/atomic"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"
)

// PayloadLimitOptions for when payload sizes exceed limits.
//
// NOTE: Experimental
//
// Exposed as: [go.temporal.io/sdk/client.PayloadLimitOptions]
type PayloadLimitOptions struct {
	// The limit (in bytes) at which a payload size warning is logged.
	// If unspecified or zero, defaults to 512 KiB.
	PayloadSizeWarning int
	// The limit (in bytes) at which an aggregate memo size warning is logged.
	// If unspecified or zero, defaults to 512 KiB.
	MemoSizeWarning int
}

type payloadLimitCheckKey struct{}
type memoLimitCheckKey struct{}

func withPayloadLimitChecks(ctx context.Context, checks limitCheck) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

func withMemoLimitChecks(ctx context.Context, checks limitCheck) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

func getPayloadLimitChecks(ctx context.Context) limitCheck {
	_ = "STUB: not implemented"
	return *new(limitCheck)
}

func getMemoLimitChecks(ctx context.Context) limitCheck {
	_ = "STUB: not implemented"
	return *new(limitCheck)
}

type limitCheck uint8

const (
	limitCheckNone    limitCheck = 0
	limitCheckError   limitCheck = 1 << 0
	limitCheckWarning limitCheck = 1 << 1
	limitCheckAll                = limitCheckError | limitCheckWarning
)

type payloadSizeError struct {
	message string
	size    int64
	limit   int64
}

func (e payloadSizeError) Error() string { _ = "STUB: not implemented"; return "" }

type payloadLimits struct {
	payloadSize int64
	memoSize    int64
}

func payloadLimitOptionsToLimits(options PayloadLimitOptions) (payloadLimits, error) {
	_ = "STUB: not implemented"
	return *new(payloadLimits), nil
}

type payloadLimitsVisitorImpl struct {
	errorLimits   atomic.Pointer[payloadLimits]
	warningLimits payloadLimits
	logger        log.Logger
}

var _ PayloadVisitor = (*payloadLimitsVisitorImpl)(nil)
var _ PayloadVisitorWithContextHook = (*payloadLimitsVisitorImpl)(nil)

func (v *payloadLimitsVisitorImpl) Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Rewrap into Payloads to get the measured size that the server would also observe.

// ContextHook is used here to specialize the limit check logic based on how server has one-off decisions
// for each proto and field that contains payloads.
func (v *payloadLimitsVisitorImpl) ContextHook(ctx context.Context, msg proto.Message) (context.Context, error) {
	_ = "STUB: not implemented"
	return *

	// RecordMarkerCommandAttributes.Details is a map[string]Payloads
	// Server has specialized size checking for map[string]Payloads for this field.
	new(context.Context), nil
}

// UpsertWorkflowSearchAttributesCommandAttributes.SearchAttributes is a map[string]Payload
// Server has specialized size checking for map[string]Payload (that are not Memo fields).

// ModifyWorkflowPropertiesCommandAttributes.Properties is a map[string]Payload
// Server has specialized size checking for map[string]Payload (that are not Memo fields).

// Server translates too large results into failed query results

// Server translates too large results into failed query results

// CreateScheduleRequest has a custom size checking algorithm checked against the payload size limit.

// Server only supports StartWorkflow action; skip if not StartWorkflow and let the server handle it.

// UpdateScheduleRequest.Memo is not validated by the server against memo size limits.

// Failures are passed through to server which will append failure details instead of failing the workflow.
// Skip error checking these to allow the server to receive the failures.

// These are the additional protos that server checks that the SDK does not currently check:
// - UpsertWorkflowSearchAttributesCommandAttributes has another SearchAttributes size check that combines execution info,
//   which is not available in the SDK. Violating the limit will terminate the workflow.
// - ModifyWorkflowPropertiesCommandAttributes has another SearchAttributes size check that combines execution info,
//   which is not available in the SDK. Violating the limit will terminate the workflow.
// - UpdateScheduleRequest has a complicated payload sizing algorithm that combines multiple fields into another proto,
//   proto-preferred encodes them, and checks against the payload size limit. Violating the limit returns an error to the client.
// - PatchScheduleRequest has a Patch field that is proto-preferred encoded and checked against the payload size limit.
//   Violating the limit returns an error to the client.
// - ListScheduleMatchingTimesRequest is proto-preferred encoded and checked against the payload size limit.
//   Violating the limit returns an error to the client.

func (v *payloadLimitsVisitorImpl) checkPayloadSize(size int64, checks limitCheck) error {
	_ = "STUB: not implemented"
	return nil
}

func (v *payloadLimitsVisitorImpl) checkMemoSize(size int64, checks limitCheck) error {
	_ = "STUB: not implemented"
	return nil
}

func (v *payloadLimitsVisitorImpl) getPayloadMapSize(fields map[string]*commonpb.Payload) int64 {
	_ = "STUB: not implemented"
	return 0
}

// Intentionally measure data size, not payload size, to match server behavior.

func (v *payloadLimitsVisitorImpl) getPayloadsMapSize(data map[string]*commonpb.Payloads) int64 {
	_ = "STUB: not implemented"
	return 0
}

func (v *payloadLimitsVisitorImpl) setErrorLimits(errorLimits *payloadLimits) {
	_ = "STUB: not implemented"
	return
}

func newPayloadLimitsVisitor(warningLimits payloadLimits, logger log.Logger) (PayloadVisitor, func(*payloadLimits)) {
	_ = "STUB: not implemented"
	return *new(PayloadVisitor), nil
}
