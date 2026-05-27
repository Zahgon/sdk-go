package nexusclient

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// An OperationHandle is used to cancel operations and get their result and status.
type OperationHandle[T any] struct {
	// Name of the Operation this handle represents.
	Operation string
	// Handler generated token for this handle's operation.
	Token string

	client *HTTPClient
}

// Cancel requests to cancel an asynchronous operation.
//
// Cancelation is asynchronous and may be not be respected by the operation's implementation.
func (h *OperationHandle[T]) Cancel(ctx context.Context, options nexus.CancelOperationOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// Do this once here and make sure it doesn't leak.
