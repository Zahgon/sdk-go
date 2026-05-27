package internal

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

type headerKey struct{}

// Header provides Temporal header information from the context for reading or
// writing during specific interceptor calls. See documentation in the
// interceptor package for more details.
//
// Exposed as: [go.temporal.io/sdk/interceptor.Header]
func Header(ctx context.Context) map[string]*commonpb.Payload {
	_ = "STUB: not implemented"
	return nil
}

func contextWithNewHeader(ctx context.Context) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

func contextWithoutHeader(ctx context.Context) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

func contextWithHeaderPropagated(
	ctx context.Context,
	header *commonpb.Header,
	ctxProps []ContextPropagator,
) (context.Context, error) {
	_ = "STUB: not implemented"
	return *new(context.Context), nil
}

func headerPropagated(ctx context.Context, ctxProps []ContextPropagator) (*commonpb.Header, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// WorkflowHeader provides Temporal header information from the workflow context
// for reading or writing during specific interceptor calls. See documentation
// in the interceptor package for more details.
//
// Exposed as: [go.temporal.io/sdk/interceptor.WorkflowHeader]
func WorkflowHeader(ctx Context) map[string]*commonpb.Payload {
	_ = "STUB: not implemented"
	return nil
}

func workflowContextWithNewHeader(ctx Context) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func workflowContextWithoutHeader(ctx Context) Context {
	_ = "STUB: not implemented"
	return *new(Context)
}

func workflowContextWithHeaderPropagated(
	ctx Context,
	header *commonpb.Header,
	ctxProps []ContextPropagator,
) (Context, error) {
	_ = "STUB: not implemented"
	return *new(Context), nil
}

func workflowHeaderPropagated(ctx Context, ctxProps []ContextPropagator) (*commonpb.Header, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
