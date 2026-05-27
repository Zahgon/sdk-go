package log

import (
	"sync"

	"go.temporal.io/sdk/log"
)

// MemoryLoggerWithoutWith is a Logger implementation that stores logs in memory (useful for testing). Use Lines() to get log lines.
type MemoryLoggerWithoutWith struct {
	lock          *sync.RWMutex
	lines         *[]string
	globalKeyvals string
}

// NewMemoryLoggerWithoutWith creates new instance of MemoryLoggerWithoutWith.
func NewMemoryLoggerWithoutWith() *MemoryLoggerWithoutWith { _ = "STUB: not implemented"; return nil }

func (l *MemoryLoggerWithoutWith) println(level, msg string, keyvals []interface{}) {
	_ = "STUB: not implemented"
	return
}

// To avoid extra space when globalKeyvals is not specified.

// Debug appends message to the log.
func (l *MemoryLoggerWithoutWith) Debug(msg string, keyvals ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// Info appends message to the log.
func (l *MemoryLoggerWithoutWith) Info(msg string, keyvals ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// Warn appends message to the log.
func (l *MemoryLoggerWithoutWith) Warn(msg string, keyvals ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// Error appends message to the log.
func (l *MemoryLoggerWithoutWith) Error(msg string, keyvals ...interface{}) {
	_ = "STUB: not implemented"
	return
}

// Lines returns written log lines.
func (l *MemoryLoggerWithoutWith) Lines() []string { _ = "STUB: not implemented"; return nil }

type MemoryLogger struct {
	*MemoryLoggerWithoutWith
}

// NewMemoryLogger creates new instance of MemoryLogger.
func NewMemoryLogger() *MemoryLogger { _ = "STUB: not implemented"; return nil }

// With returns new logger that prepend every log entry with keyvals.
func (l *MemoryLogger) With(keyvals ...interface{}) log.Logger {
	_ = "STUB: not implemented"
	return *new(log.Logger)
}
