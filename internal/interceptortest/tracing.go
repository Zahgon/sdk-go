package interceptortest

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

var testWorkflowStartTime = time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC)

type testUpdateCallbacks struct {
	AcceptImpl   func()
	RejectImpl   func(err error)
	CompleteImpl func(success interface{}, err error)
}

// Accept implements internal.UpdateCallbacks.
func (t *testUpdateCallbacks) Accept() {
	_ = "STUB: not implemented"

	// Complete implements internal.UpdateCallbacks.
	return
}

func (t *testUpdateCallbacks) Complete(success interface{}, err error) {
	_ = "STUB: not implemented"

	// Reject implements internal.UpdateCallbacks.
	return
}

func (t *testUpdateCallbacks) Reject(err error) {
	_ = "STUB: not implemented"

	// TestTracer is an interceptor.Tracer that returns finished spans.
	return
}

type TestTracer interface {
	interceptor.Tracer
	FinishedSpans() []*SpanInfo
	SpanName(options *interceptor.TracerStartSpanOptions) string
}

// SpanInfo is information about a span.
type SpanInfo struct {
	Name     string
	Children []*SpanInfo
}

// Span creates a SpanInfo.
func Span(name string, children ...*SpanInfo) *SpanInfo { _ = "STUB: not implemented"; return nil }

// RunTestWorkflow executes a test workflow with a tracing interceptor.
func RunTestWorkflow(t *testing.T, tracer interceptor.Tracer) { _ = "STUB: not implemented"; return }

// Set tracer interceptor

// Send an update

// Exec

// Confirm result

// Query workflow

func RunTestWorkflowWithError(t *testing.T, tracer interceptor.Tracer) {
	_ = "STUB: not implemented"
	return
}

// Set tracer interceptor

// Exec

// Confirm result

func AssertSpanPropagation(t *testing.T, tracer TestTracer) { _ = "STUB: not implemented"; return }

// This is the workflow that gets started from the nexus workflow run operation.

func testWorkflowWithError(_ workflow.Context) error { _ = "STUB: not implemented"; return nil }

func testWorkflow(ctx workflow.Context) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Run code

// Run child

// Signal and get result

func testWaitForCancelWorkflow(ctx workflow.Context, input nexus.NoValue) (nexus.NoValue, error) {
	_ = "STUB: not implemented"
	return *new(nexus.NoValue), nil
}

func testWorkflowChild(ctx workflow.Context) (ret []string, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func workflowInternal(ctx workflow.Context, waitSignal bool) (ret []string, err error) {
	_ = "STUB: not implemented"
	// Add signal and query handling
	return nil, nil
}

// Exec normal activity

// Exec local activity

func testActivity(ctx context.Context) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func testActivityLocal(ctx context.Context) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
