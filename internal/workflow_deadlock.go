package internal

import (
	"context"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type deadlockDetector struct {
	lock    sync.RWMutex // Applies to all fields below
	tickers map[*deadlockTicker]struct{}
	paused  bool
}

type deadlockTicker struct {
	d *deadlockDetector

	lock                sync.Mutex // Applies to all fields below
	t                   *time.Ticker
	paused              bool
	expectedExpiration  time.Time
	pausedWithRemaining time.Duration
}

// PauseDeadlockDetector pauses the deadlock detector for all coroutines.
func PauseDeadlockDetector(ctx Context) { _ = "STUB: not implemented"; return }

// ResumeDeadlockDetector resumes the deadlock detector for all coroutines.
func ResumeDeadlockDetector(ctx Context) { _ = "STUB: not implemented"; return }

// DataConverterWithoutDeadlockDetection returns a data converter that disables
// workflow deadlock detection for each call on the data converter. This should
// be used for advanced data converters that may perform remote calls or
// otherwise intentionally execute longer than the default deadlock detection
// timeout.
//
// Exposed as: [go.temporal.io/sdk/workflow.DataConverterWithoutDeadlockDetection]
func DataConverterWithoutDeadlockDetection(c converter.DataConverter) converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

// getDeadlockDetector returns the deadlock detector if the context represents
// a running workflow or nil if not.
func getDeadlockDetector(ctx Context) *deadlockDetector { _ = "STUB: not implemented"; return nil }

func newDeadlockDetector() *deadlockDetector { _ = "STUB: not implemented"; return nil }

// begin starts a new deadlock detection ticker which may start as paused
// depending on the state of the detector. Callers must call end to clean up the
// ticker.
func (d *deadlockDetector) begin(timeout time.Duration) *deadlockTicker {
	_ = "STUB: not implemented"
	return nil
}

// Set different values based on whether paused or not

func (d *deadlockDetector) pause() { _ = "STUB: not implemented"; return }

func (d *deadlockDetector) resume() { _ = "STUB: not implemented"; return }

func (d *deadlockTicker) reached() <-chan time.Time { _ = "STUB: not implemented"; return nil }

func (d *deadlockTicker) pause() { _ = "STUB: not implemented"; return }

// To prevent later panic, we make this at least 1

func (d *deadlockTicker) resume() { _ = "STUB: not implemented"; return }

// We intentionally put this after reset and accept that this is later than
// the reset time to be safe

func (d *deadlockTicker) end() { _ = "STUB: not implemented"; return }

type dataConverterWithoutDeadlock struct {
	context    Context
	underlying converter.DataConverter
}

// Exposed as: [go.temporal.io/sdk/workflow.ContextAware]
var _ ContextAware = &dataConverterWithoutDeadlock{}

func (d *dataConverterWithoutDeadlock) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (d *dataConverterWithoutDeadlock) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (d *dataConverterWithoutDeadlock) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (d *dataConverterWithoutDeadlock) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func (d *dataConverterWithoutDeadlock) ToString(input *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

func (d *dataConverterWithoutDeadlock) ToStrings(input *commonpb.Payloads) []string {
	_ = "STUB: not implemented"
	return nil
}

func (d *dataConverterWithoutDeadlock) WithWorkflowContext(ctx Context) converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

func (d *dataConverterWithoutDeadlock) WithContext(ctx context.Context) converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}
