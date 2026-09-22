package novaque_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/usual2970/novaque"

	"github.com/usual2970/novaque/store"
)

// fakeStore is an in-memory Store for domain unit tests (no MySQL import).
type fakeStore struct {
	mu       sync.Mutex
	topics   map[string]int64
	channels map[string]map[string]int64 // topic -> channel -> id
	messages []fakeMsg
	nextID   int64

	failPublish bool
	lastOpts    store.PublishOpts
	publishN    int

	// Publish self-heal capture (U3 admin plan): forced per-call errors
	// (one-shot queue, drained before failPublish), a call counter that
	// includes failed attempts, the topic id each attempt saw, and an
	// EnsureTopic/EnsureChannel call count. Under mu.
	publishErrs     []error
	publishCalls    int
	publishTopicIDs []int64
	ensureTopicN    int
	ensureChannelN  int

	// Failure toggles and a claimable-delivery queue, hit from consumer and
	// maintenance goroutines; always read/written under mu.
	failClaim   bool
	claimBlock  bool // Claim blocks until ctx is done, then returns ctx.Err()
	failAck     bool
	failRequeue bool
	failReap    bool
	failPurge   bool
	claimQueue  []store.Delivery
	claimN      int

	// Stats capture (U3): reads record resolved ids and return recognizable
	// values derived from them; prune/flush record every call. Under mu.
	failStatsRead  bool
	failPruneStats bool
	failFlushStats bool

	lastChannelCountersID int64
	lastTopicCountersID   int64
	lastBacklogID         int64
	pruneRetentions       []int
	flushN                int

	// Admin-surface capture (U3 admin plan): reads return these preset rows
	// verbatim and record the id/window args each call saw; dead ops and
	// deletes record their targets. Under mu.
	backlogs     []store.BacklogRow
	topicDaily   []store.DailyCounters
	channelDaily []store.DailyCounters
	deadRows     []store.DeadDelivery

	lastDailyTopicID    int64
	lastDailyTopicDays  int
	lastBacklogsTopicID int64
	lastDailyChannelID  int64
	lastDailyChanDays   int
	lastDeadChannelID   int64
	lastDeadBefore      int64
	lastDeadLimit       int
	lastRequeueDeadID   int64
	lastRequeueDeadChan int64
	lastRequeueDeadTTL  time.Duration
	lastDeleteDeadID    int64
	lastDeleteDeadChan  int64
	lastDeleteTopicID   int64
	lastDeleteChannelID int64
}

// setFlag flips a failure/block toggle under mu (toggles are read by client loops).
func (f *fakeStore) setFlag(flag *bool, v bool) {
	f.mu.Lock()
	*flag = v
	f.mu.Unlock()
}

func (f *fakeStore) enqueueClaim(ds ...store.Delivery) {
	f.mu.Lock()
	f.claimQueue = append(f.claimQueue, ds...)
	f.mu.Unlock()
}

func (f *fakeStore) claims() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimN
}

type fakeMsg struct {
	id       int64
	topic    string
	body     []byte
	channels []string // channel names present at publish
}

func newFake() *fakeStore {
	return &fakeStore{
		topics:   map[string]int64{},
		channels: map[string]map[string]int64{},
		nextID:   1,
	}
}

func (f *fakeStore) Migrate(context.Context) error { return nil }

func (f *fakeStore) EnsureTopic(_ context.Context, name string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureTopicN++
	if id, ok := f.topics[name]; ok {
		return id, nil
	}
	id := f.nextID
	f.nextID++
	f.topics[name] = id
	f.channels[name] = map[string]int64{}
	return id, nil
}

func (f *fakeStore) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	if _, err := f.EnsureTopic(ctx, topic); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureChannelN++
	if id, ok := f.channels[topic][channel]; ok {
		return id, nil
	}
	id := f.nextID
	f.nextID++
	f.channels[topic][channel] = id
	return id, nil
}

func (f *fakeStore) Publish(_ context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishCalls++
	f.publishTopicIDs = append(f.publishTopicIDs, topicID)
	var forced error
	if len(f.publishErrs) > 0 {
		forced = f.publishErrs[0]
		f.publishErrs = f.publishErrs[1:]
	}
	if forced == nil && f.failPublish {
		forced = errors.New("boom")
	}
	if forced != nil {
		return 0, forced
	}
	var topic string
	for name, id := range f.topics {
		if id == topicID {
			topic = name
			break
		}
	}
	if topic == "" {
		// Mirror the driver contract: publishing to an id with no topic row
		// (the topic was deleted behind this process's memo) is
		// store.ErrTopicGone.
		return 0, store.ErrTopicGone
	}
	f.lastOpts = opts
	f.publishN++
	var chans []string
	for name := range f.channels[topic] {
		chans = append(chans, name)
	}
	id := f.nextID
	f.nextID++
	f.messages = append(f.messages, fakeMsg{id: id, topic: topic, body: append([]byte(nil), body...), channels: chans})
	return id, nil
}

func (f *fakeStore) Claim(ctx context.Context, _ int64, _ string, _ time.Duration, _ int) ([]store.Delivery, error) {
	f.mu.Lock()
	f.claimN++
	block := f.claimBlock
	fail := f.failClaim
	var q []store.Delivery
	if !block && !fail {
		// Only drain the queue on the success path so a failing claim
		// concurrent with an enqueue does not drop the delivery.
		q = f.claimQueue
		f.claimQueue = nil
	}
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if fail {
		return nil, errors.New("claim boom")
	}
	return q, nil
}
func (f *fakeStore) Ack(context.Context, int64, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAck {
		return errors.New("ack boom")
	}
	return nil
}
func (f *fakeStore) Requeue(context.Context, int64, string, time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRequeue {
		return errors.New("requeue boom")
	}
	return nil
}
func (f *fakeStore) ReapExpiredLeases(context.Context, int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReap {
		return 0, errors.New("reap boom")
	}
	return 0, nil
}
func (f *fakeStore) PurgeExpired(context.Context, int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPurge {
		return 0, errors.New("purge boom")
	}
	return 0, nil
}

// Stats reads record the resolved id and return a recognizable value derived
// from it so forwarding tests can assert the id actually reached the store.
func (f *fakeStore) ChannelCounters(_ context.Context, channelID int64) (store.ChannelCounters, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failStatsRead {
		return store.ChannelCounters{}, errors.New("stats boom")
	}
	f.lastChannelCountersID = channelID
	return store.ChannelCounters{Publish: 1000 + channelID}, nil
}
func (f *fakeStore) TopicCounters(_ context.Context, topicID int64) (store.ChannelCounters, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failStatsRead {
		return store.ChannelCounters{}, errors.New("stats boom")
	}
	f.lastTopicCountersID = topicID
	return store.ChannelCounters{Publish: 2000 + topicID}, nil
}
func (f *fakeStore) ChannelBacklog(_ context.Context, channelID int64) (store.ChannelBacklog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failStatsRead {
		return store.ChannelBacklog{}, errors.New("stats boom")
	}
	f.lastBacklogID = channelID
	return store.ChannelBacklog{Pending: 3000 + channelID}, nil
}
func (f *fakeStore) PruneStats(_ context.Context, retentionDays int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPruneStats {
		return 0, errors.New("prune boom")
	}
	f.pruneRetentions = append(f.pruneRetentions, retentionDays)
	return 42, nil
}
func (f *fakeStore) FlushStats(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFlushStats {
		return errors.New("flush boom")
	}
	f.flushN++
	return nil
}

// Admin surface (mountable admin UI plan U1): listings and deletes are
// minimal in-memory implementations over the maps; backlog, daily counters,
// and dead ops return preset rows verbatim while recording the args each call
// saw (U3 capture). Reads never create rows.
func (f *fakeStore) ListTopics(_ context.Context) ([]store.TopicInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.TopicInfo
	for name, id := range f.topics {
		out = append(out, store.TopicInfo{ID: id, Name: name})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *fakeStore) ListChannels(_ context.Context) ([]store.ChannelInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.ChannelInfo
	for tName, chans := range f.channels {
		topicID := f.topics[tName]
		for cName, id := range chans {
			out = append(out, store.ChannelInfo{ID: id, TopicID: topicID, Name: cName})
		}
	}
	// Topic ids ascend with creation, so (TopicID, Name) is a stable stand-in
	// for the contract's topic-name-then-channel-name order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].TopicID != out[j].TopicID {
			return out[i].TopicID < out[j].TopicID
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (f *fakeStore) Backlogs(_ context.Context) ([]store.BacklogRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.backlogs, nil
}

// BacklogsForTopic records the scoped topic id and returns the preset rows
// verbatim (the fake is not row-accurate to one topic — the forwarding test
// only pins that the id reached the store untouched).
func (f *fakeStore) BacklogsForTopic(_ context.Context, topicID int64) ([]store.BacklogRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastBacklogsTopicID = topicID
	return f.backlogs, nil
}

func (f *fakeStore) TopicDailyCounters(_ context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDailyTopicID = topicID
	f.lastDailyTopicDays = days
	return f.topicDaily, nil
}

func (f *fakeStore) ChannelDailyCounters(_ context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDailyChannelID = channelID
	f.lastDailyChanDays = days
	return f.channelDaily, nil
}

func (f *fakeStore) ListDead(_ context.Context, channelID int64, before int64, limit int) ([]store.DeadDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeadChannelID = channelID
	f.lastDeadBefore = before
	f.lastDeadLimit = limit
	return f.deadRows, nil
}

func (f *fakeStore) RequeueDead(_ context.Context, deliveryID, channelID int64, freshTTL time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastRequeueDeadID = deliveryID
	f.lastRequeueDeadChan = channelID
	f.lastRequeueDeadTTL = freshTTL
	return nil
}

func (f *fakeStore) DeleteDead(_ context.Context, deliveryID, channelID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeleteDeadID = deliveryID
	f.lastDeleteDeadChan = channelID
	return nil
}

func (f *fakeStore) DeleteTopic(_ context.Context, topicID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeleteTopicID = topicID
	for name, id := range f.topics {
		if id == topicID {
			delete(f.topics, name)
			delete(f.channels, name)
		}
	}
	return nil
}

func (f *fakeStore) DeleteChannel(_ context.Context, channelID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeleteChannelID = channelID
	for _, chans := range f.channels {
		for cName, id := range chans {
			if id == channelID {
				delete(chans, cName)
			}
		}
	}
	return nil
}

func TestOpenRejectsNilStore(t *testing.T) {
	if _, err := novaque.Open(nil, novaque.Options{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishUsesStoreWithoutMySQL(t *testing.T) {
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.Subscribe("t", "c", func(context.Context, *novaque.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	id, err := c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected id")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.messages) != 1 || len(f.messages[0].channels) != 1 {
		t.Fatalf("unexpected fanout %#v", f.messages)
	}
}

func TestPublishErrorSurfaced(t *testing.T) {
	f := newFake()
	f.failPublish = true
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(context.Background(), "t", []byte("x"), novaque.PublishOpts{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishDelayValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("forwards delay", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: time.Hour,
			TTL:   2 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.publishN != 1 || f.lastOpts.Delay != time.Hour || f.lastOpts.TTL != 2*time.Hour {
			t.Fatalf("opts %#v n=%d", f.lastOpts, f.publishN)
		}
	})

	t.Run("zero delay", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{}); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.lastOpts.Delay != 0 {
			t.Fatalf("delay %v", f.lastOpts.Delay)
		}
	})

	t.Run("over max", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: novaque.MaxDelay + time.Second,
			TTL:   91 * 24 * time.Hour,
		})
		if !errors.Is(err, novaque.ErrDelayTooLong) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})

	t.Run("negative", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{Delay: -time.Second, TTL: time.Hour})
		if !errors.Is(err, novaque.ErrDelayNegative) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})

	t.Run("default ttl vs long delay", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{}) // DefaultTTL 7d
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{Delay: 8 * 24 * time.Hour})
		if !errors.Is(err, novaque.ErrDelayExceedsTTL) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})

	t.Run("max delay accepted", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: novaque.MaxDelay,
			TTL:   91 * 24 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("same second collapse", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: 2 * time.Second,
			TTL:   2500 * time.Millisecond,
		})
		if !errors.Is(err, novaque.ErrDelayExceedsTTL) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})
}

var _ store.Store = (*fakeStore)(nil)

// --- recording Logger double (external-package version; logger_test.go's is internal) ---

// spyEntry is one recorded log call, with any With-scope fields merged in.
type spyEntry struct {
	level  string // "debug" | "info" | "warn" | "error"
	msg    string
	fields []zap.Field
}

// spyLog is the shared entry sink behind a spyLogger and its With children.
type spyLog struct {
	mu      sync.Mutex
	entries []spyEntry
}

func (s *spyLog) add(level, msg string, scope, fields []zap.Field) {
	all := make([]zap.Field, 0, len(scope)+len(fields))
	all = append(all, scope...)
	all = append(all, fields...)
	s.mu.Lock()
	s.entries = append(s.entries, spyEntry{level: level, msg: msg, fields: all})
	s.mu.Unlock()
}

func (s *spyLog) all() []spyEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]spyEntry(nil), s.entries...)
}

// count returns entries at level with exactly msg.
func (s *spyLog) count(level, msg string) int {
	n := 0
	for _, e := range s.all() {
		if e.level == level && e.msg == msg {
			n++
		}
	}
	return n
}

// countLevel returns all entries at level regardless of message.
func (s *spyLog) countLevel(level string) int {
	n := 0
	for _, e := range s.all() {
		if e.level == level {
			n++
		}
	}
	return n
}

// countOp returns entries at level carrying a string field op=<op>.
func (s *spyLog) countOp(level, op string) int {
	n := 0
	for _, e := range s.all() {
		if e.level == level && fieldString(e, "op") == op {
			n++
		}
	}
	return n
}

func findEntry(s *spyLog, level, msg string) (spyEntry, bool) {
	for _, e := range s.all() {
		if e.level == level && e.msg == msg {
			return e, true
		}
	}
	return spyEntry{}, false
}

func findEntryOp(s *spyLog, level, op string) (spyEntry, bool) {
	for _, e := range s.all() {
		if e.level == level && fieldString(e, "op") == op {
			return e, true
		}
	}
	return spyEntry{}, false
}

func fieldString(e spyEntry, key string) string {
	for _, f := range e.fields {
		if f.Key == key && f.Type == zapcore.StringType {
			return f.String
		}
	}
	return ""
}

func fieldInt64(e spyEntry, key string) (int64, bool) {
	for _, f := range e.fields {
		if f.Key == key && f.Type == zapcore.Int64Type {
			return f.Integer, true
		}
	}
	return 0, false
}

func hasField(e spyEntry, key string) bool {
	for _, f := range e.fields {
		if f.Key == key {
			return true
		}
	}
	return false
}

// spyLogger implements novaque.Logger by recording entries into a shared sink.
type spyLogger struct {
	sink  *spyLog
	scope []zap.Field
}

var _ novaque.Logger = (*spyLogger)(nil)

func newSpyLogger() *spyLogger { return &spyLogger{sink: &spyLog{}} }

func (l *spyLogger) Debug(msg string, fields ...zap.Field) { l.sink.add("debug", msg, l.scope, fields) }
func (l *spyLogger) Info(msg string, fields ...zap.Field)  { l.sink.add("info", msg, l.scope, fields) }
func (l *spyLogger) Warn(msg string, fields ...zap.Field)  { l.sink.add("warn", msg, l.scope, fields) }
func (l *spyLogger) Error(msg string, fields ...zap.Field) { l.sink.add("error", msg, l.scope, fields) }

func (l *spyLogger) With(fields ...zap.Field) novaque.Logger {
	child := &spyLogger{sink: l.sink}
	child.scope = append(append([]zap.Field(nil), l.scope...), fields...)
	return child
}

// waitFor polls cond until it holds or timeout elapses, then fails the test.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// ackHandler is a handler that always succeeds.
func ackHandler(context.Context, *novaque.Message) error { return nil }

func delivery(id int64, topic, channel string) store.Delivery {
	return store.Delivery{
		ID:          id,
		MessageID:   id,
		Topic:       topic,
		Channel:     channel,
		Body:        []byte("x"),
		LeaseToken:  "tok",
		Attempts:    1,
		MaxAttempts: 5,
	}
}

// TestLifecycleLogs covers AE1: Client and Consumer Start/Shutdown emit Info
// exactly once per actual transition; duplicate Start and Shutdown-when-not-
// started stay silent.
func TestLifecycleLogs(t *testing.T) {
	ctx := context.Background()

	f := newFake()
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "client started"); got != 1 {
		t.Fatalf("expected 1 client started Info, got %d", got)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "client started"); got != 1 {
		t.Fatalf("duplicate Start must stay silent, got %d Info", got)
	}

	co, err := c.Subscribe("orders", "email", ackHandler)
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}
	e, ok := findEntry(spy.sink, "info", "consumer started")
	if !ok {
		t.Fatal("expected consumer started Info")
	}
	if got := fieldString(e, "topic"); got != "orders" {
		t.Fatalf("consumer started topic = %q, want orders", got)
	}
	if got := fieldString(e, "channel"); got != "email" {
		t.Fatalf("consumer started channel = %q, want email", got)
	}
	if got := spy.sink.count("info", "consumer started"); got != 1 {
		t.Fatalf("expected 1 consumer started Info, got %d", got)
	}
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "consumer started"); got != 1 {
		t.Fatalf("duplicate Consumer Start must stay silent, got %d Info", got)
	}

	if err := co.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "consumer shutdown"); got != 1 {
		t.Fatalf("expected 1 consumer shutdown Info, got %d", got)
	}
	if err := co.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "consumer shutdown"); got != 1 {
		t.Fatalf("Shutdown when not started must stay silent, got %d Info", got)
	}

	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "client shutdown"); got != 1 {
		t.Fatalf("expected 1 client shutdown Info, got %d", got)
	}
	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := spy.sink.count("info", "client shutdown"); got != 1 {
		t.Fatalf("second Client Shutdown must stay silent, got %d Info", got)
	}

	// Nothing-started Shutdown: fully silent.
	f2 := newFake()
	spy2 := newSpyLogger()
	c2, err := novaque.Open(f2, novaque.Options{Logger: spy2})
	if err != nil {
		t.Fatal(err)
	}
	co2, err := c2.Subscribe("t", "c", ackHandler)
	if err != nil {
		t.Fatal(err)
	}
	if err := co2.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c2.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(spy2.sink.all()); n != 0 {
		t.Fatalf("expected zero log entries when nothing started, got %d", n)
	}
}

// TestPublishSuccessDebugLogsWithoutBody covers AE2: one Debug with topic and
// message_id, and the body bytes appear in no message or field anywhere.
func TestPublishSuccessDebugLogsWithoutBody(t *testing.T) {
	f := newFake()
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy})
	if err != nil {
		t.Fatal(err)
	}

	body := []byte("top-secret-payload-7f3a91")
	id, err := c.Publish(context.Background(), "billing", body, novaque.PublishOpts{})
	if err != nil {
		t.Fatal(err)
	}

	if got := spy.sink.count("debug", "published"); got != 1 {
		t.Fatalf("expected exactly 1 published Debug, got %d", got)
	}
	e, ok := findEntry(spy.sink, "debug", "published")
	if !ok {
		t.Fatal("missing published Debug entry")
	}
	if got := fieldString(e, "topic"); got != "billing" {
		t.Fatalf("published topic = %q, want billing", got)
	}
	if got, ok := fieldInt64(e, "message_id"); !ok || got != id {
		t.Fatalf("published message_id = %d (present=%v), want %d", got, ok, id)
	}

	secret := string(body)
	for _, e := range spy.sink.all() {
		if strings.Contains(e.msg, secret) {
			t.Fatalf("body bytes leaked into log message %q", e.msg)
		}
		for _, fl := range e.fields {
			if fl.Key == "body" {
				t.Fatalf("body key present in fields: %s", fl.Key)
			}
			if fl.Type == zapcore.StringType && strings.Contains(fl.String, secret) {
				t.Fatalf("body bytes leaked into field %s=%q", fl.Key, fl.String)
			}
		}
	}
}

// TestClaimLogsDebugOnceAndSilentWhenEmpty covers AE3: a non-empty claim emits
// one Debug with topic/channel/count; later empty polls stay silent.
func TestClaimLogsDebugOnceAndSilentWhenEmpty(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	co, err := c.Subscribe("jobs", "worker", ackHandler)
	if err != nil {
		t.Fatal(err)
	}

	f.enqueueClaim(delivery(42, "jobs", "worker"))
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = co.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "claim Debug", func() bool {
		return spy.sink.count("debug", "claimed") >= 1
	})

	// Two more (empty) poll cycles must not add claim Debugs.
	base := f.claims()
	waitFor(t, 5*time.Second, "two empty polls", func() bool {
		return f.claims() >= base+2
	})
	if got := spy.sink.count("debug", "claimed"); got != 1 {
		t.Fatalf("expected exactly 1 claim Debug after empty polls, got %d", got)
	}
	e, ok := findEntry(spy.sink, "debug", "claimed")
	if !ok {
		t.Fatal("missing claimed Debug entry")
	}
	if fieldString(e, "topic") != "jobs" || fieldString(e, "channel") != "worker" {
		t.Fatalf("claimed entry missing topic/channel, fields: %+v", e.fields)
	}
	if n, ok := fieldInt64(e, "count"); !ok || n != 1 {
		t.Fatalf("claimed count = %d (present=%v), want 1", n, ok)
	}
}

// TestClaimErrorLogsErrorThenRecovers covers AE4: a claim failure emits an
// Error with op=claim and polling continues after the fake is healthy again.
func TestClaimErrorLogsErrorThenRecovers(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.failClaim, true)
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	co, err := c.Subscribe("jobs", "worker", ackHandler)
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = co.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "claim Error", func() bool {
		return spy.sink.countOp("error", "claim") >= 1
	})

	// Flip back to healthy and queue work: a later claim must succeed.
	f.setFlag(&f.failClaim, false)
	f.enqueueClaim(delivery(50, "jobs", "worker"))
	waitFor(t, 5*time.Second, "claim Debug after recovery", func() bool {
		return spy.sink.count("debug", "claimed") >= 1
	})

	if got := spy.sink.countOp("error", "claim"); got < 1 {
		t.Fatalf("expected claim Errors to stay recorded, got %d", got)
	}
}

// TestAckFailureAfterRetriesLogsError covers AE5 (ack half): ack failing after
// retries emits an Error with delivery_id and op=ack.
func TestAckFailureAfterRetriesLogsError(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.failAck, true)
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	co, err := c.Subscribe("jobs", "worker", ackHandler)
	if err != nil {
		t.Fatal(err)
	}

	f.enqueueClaim(delivery(77, "jobs", "worker"))
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = co.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "ack Error", func() bool {
		return spy.sink.countOp("error", "ack") >= 1
	})

	e, ok := findEntryOp(spy.sink, "error", "ack")
	if !ok {
		t.Fatal("missing ack Error entry")
	}
	if got, ok := fieldInt64(e, "delivery_id"); !ok || got != 77 {
		t.Fatalf("ack delivery_id = %d (present=%v), want 77", got, ok)
	}
	if !hasField(e, "err") {
		t.Fatalf("ack Error missing err field, fields: %+v", e.fields)
	}
	if got := spy.sink.countOp("error", "requeue"); got != 0 {
		t.Fatalf("ack path must not emit requeue Errors, got %d", got)
	}
}

// TestRequeueFailureAfterRetriesLogsError covers the requeue half of AE5:
// requeue failing after retries emits an Error with delivery_id and op=requeue;
// the handler's own error is never logged.
func TestRequeueFailureAfterRetriesLogsError(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.failRequeue, true)
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	handlerErr := errors.New("handler boom")
	co, err := c.Subscribe("jobs", "worker", func(context.Context, *novaque.Message) error { return handlerErr })
	if err != nil {
		t.Fatal(err)
	}

	f.enqueueClaim(delivery(88, "jobs", "worker"))
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = co.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "requeue Error", func() bool {
		return spy.sink.countOp("error", "requeue") >= 1
	})

	e, ok := findEntryOp(spy.sink, "error", "requeue")
	if !ok {
		t.Fatal("missing requeue Error entry")
	}
	if got, ok := fieldInt64(e, "delivery_id"); !ok || got != 88 {
		t.Fatalf("requeue delivery_id = %d (present=%v), want 88", got, ok)
	}
	if !hasField(e, "err") {
		t.Fatalf("requeue Error missing err field, fields: %+v", e.fields)
	}
	if got := spy.sink.countOp("error", "ack"); got != 0 {
		t.Fatalf("requeue path must not emit ack Errors, got %d", got)
	}
	for _, e := range spy.sink.all() {
		for _, fl := range e.fields {
			if fl.Key == "err" && strings.Contains(fmt.Sprint(fl.Interface), "handler boom") {
				t.Fatalf("handler error must never be logged, found in %q: %v", e.msg, fl.Interface)
			}
		}
	}
}

// TestPublishErrorReturnedNotErrorLogged covers AE5 (publish half): a Publish
// failure is returned to the caller and produces zero library log entries.
func TestPublishErrorReturnedNotErrorLogged(t *testing.T) {
	f := newFake()
	f.failPublish = true
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(context.Background(), "t", []byte("x"), novaque.PublishOpts{}); err == nil {
		t.Fatal("expected publish error")
	}
	if got := spy.sink.countLevel("error"); got != 0 {
		t.Fatalf("library must not Error-log failures returned to the caller, got %d", got)
	}
	if n := len(spy.sink.all()); n != 0 {
		t.Fatalf("expected fully silent publish failure, got %d entries", n)
	}
}

// TestReapAndPurgeErrorsLogged: swallowed reap/purge tick failures emit Errors
// with the matching op.
func TestReapAndPurgeErrorsLogged(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.failReap, true)
	f.setFlag(&f.failPurge, true)
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{
		Logger:        spy,
		ReapInterval:  2 * time.Millisecond,
		PurgeInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "reap Error", func() bool {
		return spy.sink.countOp("error", "reap") >= 1
	})
	waitFor(t, 5*time.Second, "purge Error", func() bool {
		return spy.sink.countOp("error", "purge") >= 1
	})
}

// TestCanceledContextClaimErrorNotLogged covers Q2: a claim failure caused by
// the run context being canceled during Shutdown is not Error-logged.
func TestCanceledContextClaimErrorNotLogged(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.claimBlock, true) // Claim blocks until ctx is done, then returns ctx.Err()
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{Logger: spy, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	co, err := c.Subscribe("jobs", "worker", ackHandler)
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Wait until the poller is parked inside Claim on the run context.
	waitFor(t, 5*time.Second, "poller inside Claim", func() bool {
		return f.claims() >= 1
	})

	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := co.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}

	if got := spy.sink.countOp("error", "claim"); got != 0 {
		t.Fatalf("canceled-ctx claim failures must stay silent, got %d Error entries", got)
	}
	if got := spy.sink.countLevel("error"); got != 0 {
		t.Fatalf("expected zero Error entries around shutdown, got %d", got)
	}
}

// --- U3 (admin plan): publish self-heal + admin surface ---

// TestPublishSelfHealAfterErrTopicGone covers the KTD9 client retry: a
// publish against a memoized-but-deleted topic id evicts the memo, re-Ensures
// the name, and retries exactly once against the re-created row.
func TestPublishSelfHealAfterErrTopicGone(t *testing.T) {
	ctx := context.Background()

	t.Run("retry publishes against re-created topic", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Publish(ctx, "orders", []byte("one"), novaque.PublishOpts{}); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		oldID := f.topics["orders"]
		f.mu.Unlock()

		// Foreign-process delete: hit the fake directly so the client's
		// CachingStore keeps its stale memo — exactly the cross-process case.
		if err := f.DeleteTopic(ctx, oldID); err != nil {
			t.Fatal(err)
		}

		if _, err := c.Publish(ctx, "orders", []byte("two"), novaque.PublishOpts{}); err != nil {
			t.Fatalf("publish after foreign delete must self-heal, got %v", err)
		}

		f.mu.Lock()
		defer f.mu.Unlock()
		newID := f.topics["orders"]
		if newID == oldID {
			t.Fatalf("topic was not re-created, id still %d", newID)
		}
		if got := f.publishTopicIDs; len(got) != 3 || got[0] != oldID || got[1] != oldID || got[2] != newID {
			t.Fatalf("publish attempts saw topic ids %v, want [%d %d %d]", got, oldID, oldID, newID)
		}
		// The memo must have been evicted between attempts: EnsureTopic hit
		// the fake exactly twice (initial resolve + re-Ensure), never a third.
		if f.ensureTopicN != 2 {
			t.Fatalf("EnsureTopic called %d times, want exactly 2 (memo not evicted)", f.ensureTopicN)
		}
	})

	t.Run("second ErrTopicGone is returned", func(t *testing.T) {
		f := newFake()
		f.mu.Lock()
		f.publishErrs = []error{store.ErrTopicGone, store.ErrTopicGone}
		f.mu.Unlock()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Publish(ctx, "orders", []byte("x"), novaque.PublishOpts{}); !errors.Is(err, store.ErrTopicGone) {
			t.Fatalf("got %v, want wrapped ErrTopicGone", err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.publishCalls != 2 {
			t.Fatalf("publish attempted %d times, want exactly 2 (one retry)", f.publishCalls)
		}
	})

	t.Run("non-sentinel error is not retried", func(t *testing.T) {
		f := newFake()
		boom := errors.New("boom")
		f.mu.Lock()
		f.publishErrs = []error{boom}
		f.mu.Unlock()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Publish(ctx, "orders", []byte("x"), novaque.PublishOpts{}); !errors.Is(err, boom) {
			t.Fatalf("got %v, want the original error surfaced", err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.publishCalls != 1 {
			t.Fatalf("publish attempted %d times, want 1 (no retry)", f.publishCalls)
		}
		if f.ensureTopicN != 1 {
			t.Fatalf("EnsureTopic called %d times, want 1 (no re-Ensure)", f.ensureTopicN)
		}
	})
}

// TestCreateTopicChannelIdempotent covers R5 client wiring: creates are
// Ensure-backed, so a duplicate create resolves the same id, not an error.
func TestCreateTopicChannelIdempotent(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	id1, err := c.CreateTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.CreateTopic(ctx, "orders")
	if err != nil {
		t.Fatalf("duplicate create must be idempotent: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("duplicate create resolved %d then %d", id1, id2)
	}
	cid1, err := c.CreateChannel(ctx, "orders", "email")
	if err != nil {
		t.Fatal(err)
	}
	cid2, err := c.CreateChannel(ctx, "orders", "email")
	if err != nil {
		t.Fatalf("duplicate channel create must be idempotent: %v", err)
	}
	if cid1 != cid2 {
		t.Fatalf("duplicate channel create resolved %d then %d", cid1, cid2)
	}
}

// TestDeleteForwardsIDWithoutEnsure covers KTD6/R6 client wiring: deletes are
// ID-addressed straight through to the store — no name Ensure anywhere on the
// path — and the id is forwarded unchanged.
func TestDeleteForwardsIDWithoutEnsure(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := c.CreateTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	channelID, err := c.CreateChannel(ctx, "orders", "email")
	if err != nil {
		t.Fatal(err)
	}

	f.mu.Lock()
	ensureTopics, ensureChans := f.ensureTopicN, f.ensureChannelN
	f.mu.Unlock()

	if err := c.DeleteTopic(ctx, topicID); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteChannel(ctx, channelID); err != nil {
		t.Fatal(err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastDeleteTopicID != topicID {
		t.Fatalf("DeleteTopic forwarded id %d, want %d", f.lastDeleteTopicID, topicID)
	}
	if f.lastDeleteChannelID != channelID {
		t.Fatalf("DeleteChannel forwarded id %d, want %d", f.lastDeleteChannelID, channelID)
	}
	if f.ensureTopicN != ensureTopics || f.ensureChannelN != ensureChans {
		t.Fatalf("delete paths must not Ensure: topics %d->%d, channels %d->%d",
			ensureTopics, f.ensureTopicN, ensureChans, f.ensureChannelN)
	}
}

// TestDeadOpsForwardThroughClient covers R7 client wiring: RequeueDead
// passes the channel scope (review #10) and the client's DefaultTTL
// (explicit option and zero-value default); DeleteDead forwards the id and
// channel.
func TestDeadOpsForwardThroughClient(t *testing.T) {
	ctx := context.Background()

	t.Run("requeue passes channel scope and configured DefaultTTL", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{DefaultTTL: 3 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.RequeueDead(ctx, 77, 5); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.lastRequeueDeadID != 77 || f.lastRequeueDeadChan != 5 || f.lastRequeueDeadTTL != 3*time.Hour {
			t.Fatalf("RequeueDead saw (%d, chan %d, %v), want (77, chan 5, 3h)", f.lastRequeueDeadID, f.lastRequeueDeadChan, f.lastRequeueDeadTTL)
		}
	})

	t.Run("requeue falls back to default TTL", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.RequeueDead(ctx, 1, 2); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.lastRequeueDeadTTL != 7*24*time.Hour {
			t.Fatalf("RequeueDead TTL %v, want the 7d default", f.lastRequeueDeadTTL)
		}
	})

	t.Run("delete dead forwards the id and channel", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.DeleteDead(ctx, 99, 6); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.lastDeleteDeadID != 99 || f.lastDeleteDeadChan != 6 {
			t.Fatalf("DeleteDead forwarded (%d, chan %d), want (99, chan 6)", f.lastDeleteDeadID, f.lastDeleteDeadChan)
		}
	})
}

// TestAdminListMethodsReturnStoreResultsUntransformed covers the thin
// forwarding layer: every admin read returns the store's rows verbatim with
// ids and window args forwarded unchanged.
func TestAdminListMethodsReturnStoreResultsUntransformed(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	presetBacklogs := []store.BacklogRow{{ChannelID: 5, Pending: 7, Ready: 1, InFlight: 2, Dead: 3}}
	presetTopicDaily := []store.DailyCounters{{Day: time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC), Publish: 4}}
	presetChannelDaily := []store.DailyCounters{{Day: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), Claim: 6}}
	presetDead := []store.DeadDelivery{{ID: 9, ChannelID: 5, Body: []byte("poison")}}
	f.mu.Lock()
	f.backlogs = presetBacklogs
	f.topicDaily = presetTopicDaily
	f.channelDaily = presetChannelDaily
	f.deadRows = presetDead
	f.mu.Unlock()

	if _, err := f.EnsureChannel(ctx, "t1", "c1"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.EnsureChannel(ctx, "t2", "a1"); err != nil {
		t.Fatal(err)
	}

	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}

	wantTopics, err := f.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topics, err := c.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(topics, wantTopics) {
		t.Fatalf("ListTopics %v, want store rows %v", topics, wantTopics)
	}

	wantChannels, err := f.ListChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	channels, err := c.ListChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(channels, wantChannels) {
		t.Fatalf("ListChannels %v, want store rows %v", channels, wantChannels)
	}

	backlogs, err := c.Backlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(backlogs, presetBacklogs) {
		t.Fatalf("Backlogs %v, want preset %v", backlogs, presetBacklogs)
	}

	scoped, err := c.BacklogsForTopic(ctx, 13)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scoped, presetBacklogs) {
		t.Fatalf("BacklogsForTopic %v, want preset %v", scoped, presetBacklogs)
	}

	point, err := c.ChannelBacklogByID(ctx, 14)
	if err != nil {
		t.Fatal(err)
	}
	if point.Pending != 3014 {
		t.Fatalf("ChannelBacklogByID %v, want the store's point row (Pending 3014)", point)
	}

	daily, err := c.TopicDailyCounters(ctx, 11, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(daily, presetTopicDaily) {
		t.Fatalf("TopicDailyCounters %v, want preset %v", daily, presetTopicDaily)
	}

	daily, err = c.ChannelDailyCounters(ctx, 12, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(daily, presetChannelDaily) {
		t.Fatalf("ChannelDailyCounters %v, want preset %v", daily, presetChannelDaily)
	}

	dead, err := c.ListDead(ctx, 5, 40, 20)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dead, presetDead) {
		t.Fatalf("ListDead %v, want preset %v", dead, presetDead)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastDailyTopicID != 11 || f.lastDailyTopicDays != 7 {
		t.Fatalf("TopicDailyCounters saw (%d, %d), want (11, 7)", f.lastDailyTopicID, f.lastDailyTopicDays)
	}
	if f.lastBacklogsTopicID != 13 {
		t.Fatalf("BacklogsForTopic saw %d, want 13", f.lastBacklogsTopicID)
	}
	if f.lastBacklogID != 14 {
		t.Fatalf("ChannelBacklogByID saw %d, want 14", f.lastBacklogID)
	}
	if f.lastDailyChannelID != 12 || f.lastDailyChanDays != 3 {
		t.Fatalf("ChannelDailyCounters saw (%d, %d), want (12, 3)", f.lastDailyChannelID, f.lastDailyChanDays)
	}
	if f.lastDeadChannelID != 5 || f.lastDeadBefore != 40 || f.lastDeadLimit != 20 {
		t.Fatalf("ListDead saw (%d, %d, %d), want (5, 40, 20)", f.lastDeadChannelID, f.lastDeadBefore, f.lastDeadLimit)
	}
}

// TestDailyCountersFlushStatsBeforeRead covers KTD12: the two daily-counter
// reads drain this process's buffered counter deltas best-effort first — and
// only they do (lists and dead reads never flush); a failing flush never
// fails the read.
func TestDailyCountersFlushStatsBeforeRead(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.ListTopics(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Backlogs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListDead(ctx, 5, 0, 20); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	flushed := f.flushN
	f.mu.Unlock()
	if flushed != 0 {
		t.Fatalf("non-counter reads must not flush, saw %d FlushStats calls", flushed)
	}

	if _, err := c.TopicDailyCounters(ctx, 11, 7); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	flushed = f.flushN
	f.mu.Unlock()
	if flushed != 1 {
		t.Fatalf("TopicDailyCounters must flush first, saw %d FlushStats calls", flushed)
	}

	if _, err := c.ChannelDailyCounters(ctx, 12, 3); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	flushed = f.flushN
	f.mu.Unlock()
	if flushed != 2 {
		t.Fatalf("ChannelDailyCounters must flush first, saw %d FlushStats calls", flushed)
	}

	// Best-effort: a failing flush is logged and swallowed, never fatal to
	// the read.
	f.setFlag(&f.failFlushStats, true)
	defer f.setFlag(&f.failFlushStats, false)
	if _, err := c.TopicDailyCounters(ctx, 11, 1); err != nil {
		t.Fatalf("daily-counter read must survive a failed flush, got %v", err)
	}
}
