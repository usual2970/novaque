package novaque

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/usual2970/novaque/store"
)

// nopStore satisfies store.Store for logger wiring tests.
type nopStore struct{}

func (nopStore) Migrate(context.Context) error { return nil }
func (nopStore) EnsureTopic(context.Context, string) (int64, error) {
	return 1, nil
}
func (nopStore) EnsureChannel(context.Context, string, string) (int64, error) {
	return 1, nil
}
func (nopStore) Publish(context.Context, int64, []byte, store.PublishOpts) (int64, error) {
	return 1, nil
}
func (nopStore) Claim(context.Context, int64, string, time.Duration, int) ([]store.Delivery, error) {
	return nil, nil
}
func (nopStore) Ack(context.Context, int64, string) error                { return nil }
func (nopStore) Requeue(context.Context, int64, string, time.Time) error { return nil }
func (nopStore) ReapExpiredLeases(context.Context, int) (int64, error)   { return 0, nil }
func (nopStore) PurgeExpired(context.Context, int) (int64, error)        { return 0, nil }

// recEntry is one recorded call on a recordingLogger.
type recEntry struct {
	msg    string
	fields []zap.Field
}

// recordingLogger is an injected Logger double sharing one entry log with its children.
type recordingLogger struct {
	entries *[]recEntry
}

func newRecordingLogger() *recordingLogger {
	entries := make([]recEntry, 0)
	return &recordingLogger{entries: &entries}
}

func (r *recordingLogger) record(msg string, fields []zap.Field) {
	*r.entries = append(*r.entries, recEntry{msg: msg, fields: fields})
}

func (r *recordingLogger) Debug(msg string, fields ...zap.Field) { r.record(msg, fields) }
func (r *recordingLogger) Info(msg string, fields ...zap.Field)  { r.record(msg, fields) }
func (r *recordingLogger) Warn(msg string, fields ...zap.Field)  { r.record(msg, fields) }
func (r *recordingLogger) Error(msg string, fields ...zap.Field) { r.record(msg, fields) }

func (r *recordingLogger) With(fields ...zap.Field) Logger {
	return &recordingLogger{entries: r.entries}
}

func TestOpenDefaultsLoggerToSilentNop(t *testing.T) {
	c, err := Open(nopStore{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	lg := c.logger()
	if lg == nil {
		t.Fatal("expected non-nil default logger")
	}
	if lg != defaultLogger() {
		t.Fatal("expected the silent Nop default")
	}
	adapted, ok := lg.(zapLogger)
	if !ok {
		t.Fatalf("expected zapLogger adapter, got %T", lg)
	}
	if !reflect.DeepEqual(adapted.base.Core(), zap.NewNop().Core()) {
		t.Fatal("expected default to be backed by zap Nop")
	}
	// Silent: all levels are safe to call and emit nothing.
	lg.Debug("d")
	lg.Info("i")
	lg.Warn("w")
	lg.Error("e")
}

func TestOptionsWithDefaultsBindsLogger(t *testing.T) {
	if got := (Options{}).withDefaults().Logger; got == nil {
		t.Fatal("expected withDefaults to bind a default Logger")
	}
}

func TestOpenKeepsInjectedLogger(t *testing.T) {
	rec := newRecordingLogger()
	c, err := Open(nopStore{}, Options{Logger: rec})
	if err != nil {
		t.Fatal(err)
	}
	if c.logger() != Logger(rec) {
		t.Fatal("expected client to use the injected logger instance")
	}
	child := c.logger().With(zap.String("scope", "child"))
	child.Info("hello", zap.String("k", "v"))
	if len(*rec.entries) != 1 {
		t.Fatalf("expected 1 recorded entry, got %d", len(*rec.entries))
	}
	if got := (*rec.entries)[0]; got.msg != "hello" || len(got.fields) != 1 || got.fields[0].Key != "k" {
		t.Fatalf("unexpected recorded entry: %+v", got)
	}
}

func TestZapAdapterForwardsLevelsAndFields(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	lg := Zap(zap.New(core))

	lg.Debug("dbg", zap.String("k", "v"))
	lg.Info("inf", zap.Int("n", 1))
	lg.Warn("wrn")
	lg.Error("err")

	entries := logs.All()
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
	want := []struct {
		level zapcore.Level
		msg   string
	}{
		{zapcore.DebugLevel, "dbg"},
		{zapcore.InfoLevel, "inf"},
		{zapcore.WarnLevel, "wrn"},
		{zapcore.ErrorLevel, "err"},
	}
	for i, w := range want {
		if entries[i].Level != w.level || entries[i].Message != w.msg {
			t.Fatalf("entry %d: got %s/%s, want %s/%s", i, entries[i].Level, entries[i].Message, w.level, w.msg)
		}
	}
	if f := entries[0].Context[0]; f.Key != "k" || f.String != "v" {
		t.Fatalf("expected k=v field, got %+v", f)
	}
	if f := entries[1].Context[0]; f.Key != "n" || f.Integer != 1 {
		t.Fatalf("expected n=1 field, got %+v", f)
	}
}

func TestZapAdapterWithChildCarriesFields(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	lg := Zap(zap.New(core))

	child := lg.With(zap.String("who", "child"))
	if child == nil {
		t.Fatal("expected non-nil child logger")
	}
	child.Info("from child")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Message != "from child" {
		t.Fatalf("unexpected message: %q", entries[0].Message)
	}
	if f := entries[0].Context[0]; f.Key != "who" || f.String != "child" {
		t.Fatalf("expected who=child context field, got %+v", f)
	}
}

func TestZapNilFallsBackToSilentNop(t *testing.T) {
	lg := Zap(nil)
	if lg == nil {
		t.Fatal("Zap(nil) must not return nil")
	}
	if lg != defaultLogger() {
		t.Fatal("expected Zap(nil) to return the silent Nop default")
	}
	lg.Debug("d")
	lg.Info("i")
	lg.Warn("w")
	lg.Error("e")
}

func TestClientLoggerNeverNil(t *testing.T) {
	c := &Client{} // zero-value defensive path
	if c.logger() == nil {
		t.Fatal("expected defensive Nop fallback for zero-value Client")
	}
}
