package log

import (
	"go.temporal.io/sdk/log"
)

// TestReporter is a log adapter for gomock.
type TestReporter struct {
	logger log.Logger
}

// NewTestReporter creates new instance of TestReporter.
func NewTestReporter(logger log.Logger) *TestReporter { _ = "STUB: not implemented"; return nil }

// Errorf writes error to the log.
func (t *TestReporter) Errorf(format string, args ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// Fatalf writes error to the log and exits.
func (t *TestReporter) Fatalf(format string, args ...interface{}) {
	_ = "STUB: not implemented"
	return
}
