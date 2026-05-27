package protocol

import (
	"sync"

	protocolpb "go.temporal.io/api/protocol/v1"
)

type (
	// Instance is the required interface for protocol objects.
	Instance interface {
		HandleMessage(*protocolpb.Message) error
		HasCompleted() bool
	}

	// Registry stores running protocols.
	Registry struct {
		mut       sync.Mutex
		instances map[string]Instance
	}
)

func NewRegistry() *Registry { _ = "STUB: not implemented"; return nil }

// FindOrAdd looks up an existing protocol by instance ID or constructs a new
// one and registers it under the instance ID indicated.
func (r *Registry) FindOrAdd(instID string, ctor func() Instance) Instance {
	_ = "STUB: not implemented"
	return *new(Instance)
}

// ClearCompleted walks the registered protocols and removes those that have
// completed.
func (r *Registry) ClearCompleted() { _ = "STUB: not implemented"; return }
