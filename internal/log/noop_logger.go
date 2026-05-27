package log

import (
	"go.temporal.io/sdk/log"
)

// NoopLogger is Logger implementation that doesn't produce any logs.
type NoopLogger struct {
}

// NewNopLogger creates new instance of NoopLogger.
func NewNopLogger() *NoopLogger { _ = "STUB: not implemented"; return nil }

// Debug does nothing.
func (l *NoopLogger) Debug(string, ...interface{}) {
	_ = "STUB: not implemented"

	// Info does nothing.
	return
}

func (l *NoopLogger) Info(string, ...interface{}) {
	_ = "STUB: not implemented"

	// Warn does nothing.
	return
}

func (l *NoopLogger) Warn(string, ...interface{}) {
	_ = "STUB: not implemented"

	// Error does nothing.
	return
}

func (l *NoopLogger) Error(string, ...interface{}) {
	_ = "STUB: not implemented"

	// With returns new NoopLogger.
	return
}

func (l *NoopLogger) With(...interface{}) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}
