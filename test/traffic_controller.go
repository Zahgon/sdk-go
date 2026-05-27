package test

import (
	"context"
	"sync"

	"go.temporal.io/sdk/log"
)

const FailAllAttempts = -1

type SimpleTrafficController struct {
	totalCalls   map[string]int
	allowedCalls map[string]int
	failAttempts map[string]map[int]error // maps operation to individual attempts and corresponding errors
	logger       log.Logger
	lock         sync.RWMutex
}

func NewSimpleTrafficController() *SimpleTrafficController { _ = "STUB: not implemented"; return nil }

func (tc *SimpleTrafficController) CheckCallAllowed(_ context.Context, method string, _, _ interface{}) error {
	_ = "STUB: not implemented"
	// Name of the API being called
	return nil
}

func (tc *SimpleTrafficController) AddError(operation string, err error, failAttempts ...int) {
	_ = "STUB: not implemented"
	return
}
