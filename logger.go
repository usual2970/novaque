package novaque

import "go.uber.org/zap"

// Logger is the structured logging contract used by novaque. It mirrors zap's
// field-based API so a configured *zap.Logger plugs in via Zap; hosts on other
// libraries can satisfy it with a thin shim. Message bodies are never logged.
type Logger interface {
	// Debug logs hot-path events: successful publishes and non-empty claims.
	// Expect high volume when enabled in production.
	Debug(msg string, fields ...zap.Field)
	// Info logs lifecycle transitions: Client and Consumer Start and
	// Shutdown, once per actual transition.
	Info(msg string, fields ...zap.Field)
	// Warn mirrors zap's Warn for shim symmetry; novaque itself currently
	// emits no Warn entries.
	Warn(msg string, fields ...zap.Field)
	// Error logs swallowed failures only: claim backoff, reap, purge, stats
	// flush/prune, and ack/requeue failures that survive retries. Errors
	// returned to the caller are not duplicate-logged.
	Error(msg string, fields ...zap.Field)

	// With returns a child Logger that adds fields to every entry.
	With(fields ...zap.Field) Logger
}

// Zap adapts a *zap.Logger to Logger for injection via Options.Logger.
// A nil logger falls back to the silent Nop default.
func Zap(base *zap.Logger) Logger {
	if base == nil {
		return defaultLogger()
	}
	return zapLogger{base: base}
}

// zapLogger forwards every call to the wrapped *zap.Logger.
type zapLogger struct {
	base *zap.Logger
}

func (l zapLogger) Debug(msg string, fields ...zap.Field) { l.base.Debug(msg, fields...) }
func (l zapLogger) Info(msg string, fields ...zap.Field)  { l.base.Info(msg, fields...) }
func (l zapLogger) Warn(msg string, fields ...zap.Field)  { l.base.Warn(msg, fields...) }
func (l zapLogger) Error(msg string, fields ...zap.Field) { l.base.Error(msg, fields...) }

func (l zapLogger) With(fields ...zap.Field) Logger {
	return zapLogger{base: l.base.With(fields...)}
}

// defaultLogger returns the silent fallback used when Options.Logger is nil.
func defaultLogger() Logger { return defaultLog }

var defaultLog Logger = zapLogger{base: zap.NewNop()}
