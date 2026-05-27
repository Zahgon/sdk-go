package internal

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"google.golang.org/protobuf/proto"
)

type PayloadVisitor interface {
	Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error)
}

type PayloadVisitorWithContextHook interface {
	PayloadVisitor
	ContextHook(ctx context.Context, msg proto.Message) (context.Context, error)
}

type compositePayloadVisitor struct {
	visitors []PayloadVisitor
}

var _ PayloadVisitor = (*compositePayloadVisitor)(nil)
var _ PayloadVisitorWithContextHook = (*compositePayloadVisitor)(nil)

func (v *compositePayloadVisitor) Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (v *compositePayloadVisitor) ContextHook(ctx context.Context, msg proto.Message) (context.Context, error) {
	_ = "STUB: not implemented"
	return *new(context.Context), nil
}

func newCompositePayloadVisitor(visitors ...PayloadVisitor) PayloadVisitor {
	_ = "STUB: not implemented"
	return *new(PayloadVisitor)
}

// visitProtoPayloads runs visitor over all payloads in msg, skipping search
// attributes. If visitor is nil, msg is unchanged.
func visitProtoPayloads(ctx context.Context, visitor PayloadVisitor, msg proto.Message, concurrencyLimit int) error {
	_ = "STUB: not implemented"
	return nil
}

// visitPayload runs visitor over a single payload. If visitor is nil
// the original payload is returned unchanged.
func visitPayload(ctx context.Context, visitor PayloadVisitor, p *commonpb.Payload) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
