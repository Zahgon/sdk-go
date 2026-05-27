package log

import (
	golog "log"

	"go.temporal.io/sdk/log"
)

// DefaultLogger is Logger implementation on top of standard log.Logger. It is used if logger is not specified.
type DefaultLogger struct {
	logger        *golog.Logger
	globalKeyvals string
}

// NewDefaultLogger creates new instance of DefaultLogger.
func NewDefaultLogger() *DefaultLogger { _ = "STUB: not implemented"; return nil }

func (l *DefaultLogger) println(level, msg string, keyvals []interface{}) {
	_ = "STUB: not implemented"
	// To avoid extra space when globalKeyvals is not specified.
	return
}

// Debug writes message to the log.
func (l *DefaultLogger) Debug(msg string, keyvals ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// Info writes message to the log.
func (l *DefaultLogger) Info(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Warn writes message to the log.
func (l *DefaultLogger) Warn(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Error writes message to the log.
func (l *DefaultLogger) Error(msg string, keyvals ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// With returns new logger the prepend every log entry with keyvals.
func (l *DefaultLogger) With(keyvals ...interface{}) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}
