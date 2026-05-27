//go:build go1.21

package log

import (
	"context"
	"log/slog"
)

type slogLogger struct {
	logger *slog.Logger
	depth  int
}

// NewStructuredLogger creates an adapter around the given logger to be passed to Temporal.
func NewStructuredLogger(logger *slog.Logger) Logger {
	_ = "STUB: not implemented"
	return *new(Logger)
}

func (s *slogLogger) Debug(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

func (s *slogLogger) Info(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

func (s *slogLogger) Warn(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

func (s *slogLogger) Error(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

func (s *slogLogger) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	_ = "STUB: not implemented"
	return
}

func (s *slogLogger) With(keyvals ...interface{}) Logger {
	_ = "STUB: not implemented"
	return *new(Logger)
}

func (s *slogLogger) WithCallerSkip(depth int) Logger {
	_ = "STUB: not implemented"
	return *new(Logger)
}
