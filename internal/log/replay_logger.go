package log

import (
	"go.temporal.io/sdk/log"
)

var _ log.Logger = (*ReplayLogger)(nil)
var _ log.WithLogger = (*ReplayLogger)(nil)
var _ log.WithSkipCallers = (*ReplayLogger)(nil)

// ReplayLogger is Logger implementation that is aware of replay.
type ReplayLogger struct {
	logger                log.Logger
	isReplay              *bool // pointer to bool that indicate if it is in replay mode
	enableLoggingInReplay *bool // pointer to bool that indicate if logging is enabled in replay mode
}

// NewReplayLogger crates new instance of ReplayLogger.
func NewReplayLogger(logger log.Logger, isReplay *bool, enableLoggingInReplay *bool) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (l *ReplayLogger) check() bool { _ = "STUB: not implemented"; return false }

// Debug writes message to the log if it is not a replay.
func (l *ReplayLogger) Debug(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Info writes message to the log if it is not a replay.
func (l *ReplayLogger) Info(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Warn writes message to the log if it is not a replay.
func (l *ReplayLogger) Warn(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Error writes message to the log if it is not a replay.
func (l *ReplayLogger) Error(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// With returns new logger that prepend every log entry with keyvals.
func (l *ReplayLogger) With(keyvals ...interface{}) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}

func (l *ReplayLogger) WithCallerSkip(depth int) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}
