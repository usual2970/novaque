package novaque

import "go.uber.org/zap"

// Logger is the structured logging contract used by novaque. It mirrors zap's
// field-based API so a configured *zap.Logger plugs in via Zap; hosts on other
// libraries can satisfy it with a thin shim. Message bodies are never logged.
type Logger interface {
	Debug(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
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
