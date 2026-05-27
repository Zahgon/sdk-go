package log

// With creates a child Logger that includes the supplied key-value pairs in each log entry. It does this by
// using the supplied logger if it implements WithLogger; otherwise, it does so by intercepting every log call.
func With(logger Logger, keyvals ...interface{}) Logger {
	_ = "STUB: not implemented"
	return *new(Logger)
}

// Skip creates a child Logger that increase increases its' caller skip depth if it
// implements [WithSkipCallers]. Otherwise returns the original logger.
func Skip(logger Logger, depth int) Logger { _ = "STUB: not implemented"; return *new(Logger) }

var _ Logger = (*withLogger)(nil)
var _ WithSkipCallers = (*withLogger)(nil)

type withLogger struct {
	logger  Logger
	keyvals []interface{}
}

func newWithLogger(logger Logger, keyvals ...interface{}) *withLogger {
	_ = "STUB: not implemented"
	return nil
}

func (l *withLogger) prependKeyvals(keyvals []interface{}) []interface{} {
	_ = "STUB: not implemented"
	return nil
}

// Debug writes message to the log.
func (l *withLogger) Debug(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Info writes message to the log.
func (l *withLogger) Info(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Warn writes message to the log.
func (l *withLogger) Warn(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

// Error writes message to the log.
func (l *withLogger) Error(msg string, keyvals ...interface{}) { _ = "STUB: not implemented"; return }

func (l *withLogger) WithCallerSkip(depth int) Logger {
	_ = "STUB: not implemented"
	return *new(Logger)
}
