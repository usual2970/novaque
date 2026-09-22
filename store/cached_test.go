package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usual2970/novaque/store"
)

type countingStore struct {
	topicCalls   atomic.Int64
	channelCalls atomic.Int64
	topics       sync.Map
	channels     sync.Map
	nextID       atomic.Int64

	channelCountersCalls atomic.Int64
	topicCountersCalls   atomic.Int64
	backlogCalls         atomic.Int64
	pruneCalls           atomic.Int64
	flushCalls           atomic.Int64

	// Admin surface (U1): every method counts one inner call per outer call;
	// mutations capture their id/ttl arguments so forwarding tests can assert
	// the values actually reached the inner Store.
	listTopicsCalls     atomic.Int64
	listChannelsCalls   atomic.Int64
	backlogsCalls       atomic.Int64
	backlogsTopicCalls  atomic.Int64
	topicDailyCalls     atomic.Int64
	channelDailyCalls   atomic.Int64
	listDeadCalls       atomic.Int64
	requeueDeadCalls    atomic.Int64
	deleteDeadCalls     atomic.Int64
	deleteTopicCalls    atomic.Int64
	deleteChannelCalls  atomic.Int64
	lastRequeueID       atomic.Int64
	lastRequeueChan     atomic.Int64
	lastRequeueTTL      atomic.Int64
	lastDeleteDeadID    atomic.Int64
	lastDeleteDeadChan  atomic.Int64
	lastDeleteTopicID   atomic.Int64
	lastDeleteChannelID atomic.Int64
}

func newCounting() *countingStore {
	c := &countingStore{}
	c.nextID.Store(1)
	return c
}

func (c *countingStore) Migrate(context.Context) error { return nil }

func (c *countingStore) EnsureTopic(_ context.Context, name string) (int64, error) {
	c.topicCalls.Add(1)
	if v, ok := c.topics.Load(name); ok {
		return v.(int64), nil
	}
	id := c.nextID.Add(1) - 1
	c.topics.Store(name, id)
	return id, nil
}

func (c *countingStore) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	c.channelCalls.Add(1)
	key := topic + "\x00" + channel
	if v, ok := c.channels.Load(key); ok {
		return v.(int64), nil
	}
	if _, err := c.EnsureTopic(ctx, topic); err != nil {
		return 0, err
	}
	id := c.nextID.Add(1) - 1
	c.channels.Store(key, id)
	return id, nil
}

func (c *countingStore) Publish(context.Context, int64, []byte, store.PublishOpts) (int64, error) {
	return 1, nil
}

func (c *countingStore) Claim(context.Context, int64, string, time.Duration, int) ([]store.Delivery, error) {
	return nil, nil
}
func (c *countingStore) Ack(context.Context, int64, string) error { return nil }
func (c *countingStore) Requeue(context.Context, int64, string, time.Time) error {
	return nil
}
func (c *countingStore) ReapExpiredLeases(context.Context, int) (int64, error) { return 0, nil }
func (c *countingStore) PurgeExpired(context.Context, int) (int64, error)      { return 0, nil }

func (c *countingStore) ChannelCounters(_ context.Context, channelID int64) (store.ChannelCounters, error) {
	c.channelCountersCalls.Add(1)
	return store.ChannelCounters{Publish: channelID}, nil
}
func (c *countingStore) TopicCounters(_ context.Context, topicID int64) (store.ChannelCounters, error) {
	c.topicCountersCalls.Add(1)
	return store.ChannelCounters{Publish: topicID}, nil
}
func (c *countingStore) ChannelBacklog(_ context.Context, channelID int64) (store.ChannelBacklog, error) {
	c.backlogCalls.Add(1)
	return store.ChannelBacklog{Pending: channelID}, nil
}
func (c *countingStore) PruneStats(_ context.Context, retentionDays int) (int64, error) {
	c.pruneCalls.Add(1)
	return int64(retentionDays), nil
}
func (c *countingStore) FlushStats(context.Context) error {
	c.flushCalls.Add(1)
	return nil
}

// topicID returns the id inner assigned to name, or -1 when unknown.
func (c *countingStore) topicID(name string) int64 {
	v, ok := c.topics.Load(name)
	if !ok {
		return -1
	}
	return v.(int64)
}

// Admin surface (U1): reads return recognizable rows derived from their
// arguments; mutations record their id/ttl so forwarding tests can pin both
// the call count and the exact values that reached inner.
func (c *countingStore) ListTopics(context.Context) ([]store.TopicInfo, error) {
	c.listTopicsCalls.Add(1)
	return []store.TopicInfo{{ID: 1, Name: "counted"}}, nil
}

func (c *countingStore) ListChannels(context.Context) ([]store.ChannelInfo, error) {
	c.listChannelsCalls.Add(1)
	return []store.ChannelInfo{{ID: 2, TopicID: 1, Name: "counted"}}, nil
}

func (c *countingStore) Backlogs(context.Context) ([]store.BacklogRow, error) {
	c.backlogsCalls.Add(1)
	return []store.BacklogRow{{ChannelID: 7, Pending: 11, Ready: 5, InFlight: 3, Dead: 2}}, nil
}

func (c *countingStore) BacklogsForTopic(_ context.Context, topicID int64) ([]store.BacklogRow, error) {
	c.backlogsTopicCalls.Add(1)
	return []store.BacklogRow{{ChannelID: topicID, Pending: 11, Ready: 5, InFlight: 3, Dead: 2}}, nil
}

func (c *countingStore) TopicDailyCounters(_ context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	c.topicDailyCalls.Add(1)
	return []store.DailyCounters{{Day: time.Unix(0, 0).UTC(), Publish: topicID, Claim: int64(days)}}, nil
}

func (c *countingStore) ChannelDailyCounters(_ context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	c.channelDailyCalls.Add(1)
	return []store.DailyCounters{{Day: time.Unix(0, 0).UTC(), Publish: channelID, Claim: int64(days)}}, nil
}

func (c *countingStore) ListDead(_ context.Context, channelID int64, before int64, limit int) ([]store.DeadDelivery, error) {
	c.listDeadCalls.Add(1)
	return []store.DeadDelivery{{ID: before, ChannelID: channelID, Status: store.StatusDead, Attempts: limit}}, nil
}

func (c *countingStore) RequeueDead(_ context.Context, deliveryID, channelID int64, freshTTL time.Duration) error {
	c.requeueDeadCalls.Add(1)
	c.lastRequeueID.Store(deliveryID)
	c.lastRequeueChan.Store(channelID)
	c.lastRequeueTTL.Store(int64(freshTTL))
	return nil
}

func (c *countingStore) DeleteDead(_ context.Context, deliveryID, channelID int64) error {
	c.deleteDeadCalls.Add(1)
	c.lastDeleteDeadID.Store(deliveryID)
	c.lastDeleteDeadChan.Store(channelID)
	return nil
}

func (c *countingStore) DeleteTopic(_ context.Context, topicID int64) error {
	c.deleteTopicCalls.Add(1)
	c.lastDeleteTopicID.Store(topicID)
	return nil
}

func (c *countingStore) DeleteChannel(_ context.Context, channelID int64) error {
	c.deleteChannelCalls.Add(1)
	c.lastDeleteChannelID.Store(channelID)
	return nil
}

// countingStore must keep satisfying the full Store contract, stats included
// (U4 fake completeness check).
var _ store.Store = (*countingStore)(nil)

func TestCachingStoreMemoizesEnsure(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	id1, err := s.EnsureTopic(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.EnsureTopic(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("ids differ: %d vs %d", id1, id2)
	}
	if n := inner.topicCalls.Load(); n != 1 {
		t.Fatalf("EnsureTopic inner calls = %d, want 1", n)
	}

	ch1, err := s.EnsureChannel(ctx, "events", "workers")
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := s.EnsureChannel(ctx, "events", "workers")
	if err != nil {
		t.Fatal(err)
	}
	if ch1 != ch2 {
		t.Fatalf("channel ids differ: %d vs %d", ch1, ch2)
	}
	if n := inner.channelCalls.Load(); n != 1 {
		t.Fatalf("EnsureChannel inner calls = %d, want 1", n)
	}
}

// TestCachingStorePassesThroughStatsAndBacklog covers R7: stats reads, backlog
// COUNTs, prune, and flush are never memoized — every call must reach the
// inner Store.
func TestCachingStorePassesThroughStatsAndBacklog(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := s.ChannelCounters(ctx, 7); err != nil {
			t.Fatal(err)
		}
		if _, err := s.TopicCounters(ctx, 5); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ChannelBacklog(ctx, 7); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PruneStats(ctx, 30); err != nil {
			t.Fatal(err)
		}
		if err := s.FlushStats(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if n := inner.channelCountersCalls.Load(); n != 2 {
		t.Fatalf("ChannelCounters inner calls = %d, want 2", n)
	}
	if n := inner.topicCountersCalls.Load(); n != 2 {
		t.Fatalf("TopicCounters inner calls = %d, want 2", n)
	}
	if n := inner.backlogCalls.Load(); n != 2 {
		t.Fatalf("ChannelBacklog inner calls = %d, want 2", n)
	}
	if n := inner.pruneCalls.Load(); n != 2 {
		t.Fatalf("PruneStats inner calls = %d, want 2", n)
	}
	if n := inner.flushCalls.Load(); n != 2 {
		t.Fatalf("FlushStats inner calls = %d, want 2", n)
	}
}

func TestWithCacheIdempotent(t *testing.T) {
	inner := newCounting()
	s1 := store.WithCache(inner)
	s2 := store.WithCache(s1)
	if s1 != s2 {
		t.Fatal("WithCache should not double-wrap")
	}
}

// TestCachingStoreStatsConcurrentSmoke covers U4: a short concurrent hammer
// through store.WithCache on every stats method — with countingStore already
// atomic, each of the attempts must reach inner exactly once (R7 passthrough
// holds under concurrency) and nothing may panic.
func TestCachingStoreStatsConcurrentSmoke(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	const goroutines = 8
	const rounds = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if _, err := s.ChannelCounters(ctx, 7); err != nil {
					t.Errorf("channel counters: %v", err)
					return
				}
				if _, err := s.TopicCounters(ctx, 5); err != nil {
					t.Errorf("topic counters: %v", err)
					return
				}
				if _, err := s.ChannelBacklog(ctx, 7); err != nil {
					t.Errorf("backlog: %v", err)
					return
				}
				if _, err := s.PruneStats(ctx, 30); err != nil {
					t.Errorf("prune: %v", err)
					return
				}
				if err := s.FlushStats(ctx); err != nil {
					t.Errorf("flush: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * rounds)
	if n := inner.channelCountersCalls.Load(); n != want {
		t.Errorf("ChannelCounters inner calls = %d, want %d", n, want)
	}
	if n := inner.topicCountersCalls.Load(); n != want {
		t.Errorf("TopicCounters inner calls = %d, want %d", n, want)
	}
	if n := inner.backlogCalls.Load(); n != want {
		t.Errorf("ChannelBacklog inner calls = %d, want %d", n, want)
	}
	if n := inner.pruneCalls.Load(); n != want {
		t.Errorf("PruneStats inner calls = %d, want %d", n, want)
	}
	if n := inner.flushCalls.Load(); n != want {
		t.Errorf("FlushStats inner calls = %d, want %d", n, want)
	}
}

// TestCachingStoreForwardsAdminSurface covers U1: every admin-surface method
// is a pure passthrough — exactly one inner call per outer call, nothing
// memoized (plan 004 R7 posture), arguments and results untouched.
func TestCachingStoreForwardsAdminSurface(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		topics, err := s.ListTopics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(topics) != 1 || topics[0].Name != "counted" {
			t.Fatalf("ListTopics passthrough mangled: %#v", topics)
		}
		if _, err := s.ListChannels(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Backlogs(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BacklogsForTopic(ctx, 5); err != nil {
			t.Fatal(err)
		}
		rows, err := s.TopicDailyCounters(ctx, 5, 30)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Publish != 5 || rows[0].Claim != 30 {
			t.Fatalf("TopicDailyCounters passthrough mangled: %#v", rows)
		}
		if _, err := s.ChannelDailyCounters(ctx, 7, 14); err != nil {
			t.Fatal(err)
		}
		dead, err := s.ListDead(ctx, 7, 42, 50)
		if err != nil {
			t.Fatal(err)
		}
		if len(dead) != 1 || dead[0].ID != 42 || dead[0].ChannelID != 7 || dead[0].Attempts != 50 {
			t.Fatalf("ListDead passthrough mangled: %#v", dead)
		}
		if err := s.RequeueDead(ctx, 42, 7, time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteDead(ctx, 42, 7); err != nil {
			t.Fatal(err)
		}
	}

	for name, got := range map[string]int64{
		"ListTopics":           inner.listTopicsCalls.Load(),
		"ListChannels":         inner.listChannelsCalls.Load(),
		"Backlogs":             inner.backlogsCalls.Load(),
		"BacklogsForTopic":     inner.backlogsTopicCalls.Load(),
		"TopicDailyCounters":   inner.topicDailyCalls.Load(),
		"ChannelDailyCounters": inner.channelDailyCalls.Load(),
		"ListDead":             inner.listDeadCalls.Load(),
		"RequeueDead":          inner.requeueDeadCalls.Load(),
		"DeleteDead":           inner.deleteDeadCalls.Load(),
	} {
		if got != 2 {
			t.Errorf("%s inner calls = %d, want 2", name, got)
		}
	}
	if got := inner.lastRequeueID.Load(); got != 42 {
		t.Errorf("RequeueDead deliveryID = %d, want 42", got)
	}
	if got := inner.lastRequeueChan.Load(); got != 7 {
		t.Errorf("RequeueDead channelID = %d, want 7 (channel scope forwarded)", got)
	}
	if got := inner.lastRequeueTTL.Load(); got != int64(time.Hour) {
		t.Errorf("RequeueDead freshTTL = %d, want %d", got, int64(time.Hour))
	}
	if got := inner.lastDeleteDeadID.Load(); got != 42 {
		t.Errorf("DeleteDead deliveryID = %d, want 42", got)
	}
	if got := inner.lastDeleteDeadChan.Load(); got != 7 {
		t.Errorf("DeleteDead channelID = %d, want 7 (channel scope forwarded)", got)
	}
}

// TestCachingStoreDeleteTopicSweepsMemoizedIDs covers U1/KTD9+R11:
// DeleteTopic forwards the id to inner exactly once and first sweeps this
// process's memoized name→id entries — the topic's entry plus every channel
// entry under it — while an unrelated topic's entries stay memoized.
func TestCachingStoreDeleteTopicSweepsMemoizedIDs(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	if _, err := s.EnsureTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "audit"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureTopic(ctx, "billing"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "billing", "email"); err != nil {
		t.Fatal(err)
	}
	ordersID := inner.topicID("orders")

	if err := s.DeleteTopic(ctx, ordersID); err != nil {
		t.Fatal(err)
	}
	if n := inner.deleteTopicCalls.Load(); n != 1 {
		t.Fatalf("DeleteTopic inner calls = %d, want 1", n)
	}
	if got := inner.lastDeleteTopicID.Load(); got != ordersID {
		t.Fatalf("DeleteTopic forwarded id %d, want %d", got, ordersID)
	}

	// Swept entries: re-ensuring the deleted topic and its channels must
	// reach inner again instead of answering from the stale memo.
	topicBase := inner.topicCalls.Load()
	channelBase := inner.channelCalls.Load()
	if _, err := s.EnsureTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "audit"); err != nil {
		t.Fatal(err)
	}
	if n := inner.topicCalls.Load(); n != topicBase+1 {
		t.Fatalf("EnsureTopic after delete = %d inner calls (base %d), want +1 — memo not swept", n, topicBase)
	}
	if n := inner.channelCalls.Load(); n != channelBase+2 {
		t.Fatalf("EnsureChannel after delete = %d inner calls (base %d), want +2 — channel memos not swept", n, channelBase)
	}

	// Untouched entries: the sibling topic and its channel stay memoized.
	if _, err := s.EnsureTopic(ctx, "billing"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "billing", "email"); err != nil {
		t.Fatal(err)
	}
	if n := inner.topicCalls.Load(); n != topicBase+1 {
		t.Fatalf("sibling topic must stay memoized, inner calls = %d, want %d", n, topicBase+1)
	}
	if n := inner.channelCalls.Load(); n != channelBase+2 {
		t.Fatalf("sibling channel must stay memoized, inner calls = %d, want %d", n, channelBase+2)
	}
}

// TestCachingStoreDeleteChannelSweepsMemoizedIDs covers U1: DeleteChannel
// forwards the id and drops only the memoized entry resolving to it — the
// sibling channel under the same topic keeps its memo.
func TestCachingStoreDeleteChannelSweepsMemoizedIDs(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	if _, err := s.EnsureTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	emailID, err := s.EnsureChannel(ctx, "orders", "email")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "audit"); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteChannel(ctx, emailID); err != nil {
		t.Fatal(err)
	}
	if n := inner.deleteChannelCalls.Load(); n != 1 {
		t.Fatalf("DeleteChannel inner calls = %d, want 1", n)
	}
	if got := inner.lastDeleteChannelID.Load(); got != emailID {
		t.Fatalf("DeleteChannel forwarded id %d, want %d", got, emailID)
	}

	channelBase := inner.channelCalls.Load()
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if n := inner.channelCalls.Load(); n != channelBase+1 {
		t.Fatalf("EnsureChannel after delete = %d inner calls (base %d), want +1 — memo not swept", n, channelBase)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "audit"); err != nil {
		t.Fatal(err)
	}
	if n := inner.channelCalls.Load(); n != channelBase+1 {
		t.Fatalf("sibling channel must stay memoized, inner calls = %d, want %d", n, channelBase+1)
	}
}

// TestCachingStoreInvalidateIDs covers U1/KTD9: InvalidateTopicID and
// InvalidateChannelID drop exactly the memoized entry resolving to the id
// (the eviction half of the Client.Publish self-heal) and are a safe no-op
// for unknown ids.
// TestCachingStoreInvalidateTopicByName covers the full self-heal eviction
// (review finding: foreign delete leaves ghost channel cache): after another
// process cascade-deletes a topic, InvalidateTopic must drop the name entry
// AND every channel entry under the name — not only the topic id — so a
// subsequent EnsureChannel/Subscribe cannot be served a deleted channel id
// from cache. Sibling topics sharing a name prefix are untouched.
func TestCachingStoreInvalidateTopicByName(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	if _, err := s.EnsureTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "audit"); err != nil {
		t.Fatal(err)
	}
	// A name-prefix sibling proves the sweep cannot straddle topics.
	if _, err := s.EnsureChannel(ctx, "orders-archive", "email"); err != nil {
		t.Fatal(err)
	}

	topicBase := inner.topicCalls.Load()
	channelBase := inner.channelCalls.Load()

	cs, ok := s.(*store.CachingStore)
	if !ok {
		t.Fatalf("expected *store.CachingStore, got %T", s)
	}
	cs.InvalidateTopic("orders")

	// Both channels under "orders" miss the cache and hit Inner again...
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "audit"); err != nil {
		t.Fatal(err)
	}
	if n := inner.channelCalls.Load(); n != channelBase+2 {
		t.Fatalf("EnsureChannel after InvalidateTopic = %d inner calls (base %d), want +2 — channel memo survived the sweep", n, channelBase)
	}
	// ...the topic name entry was dropped too (EnsureChannel re-warms it)...
	if n := inner.topicCalls.Load(); n != topicBase+1 {
		t.Fatalf("EnsureTopic re-warm after InvalidateTopic = %d inner calls (base %d), want +1", n, topicBase)
	}
	// ...while the "orders-archive" sibling stays memoized.
	if _, err := s.EnsureChannel(ctx, "orders-archive", "email"); err != nil {
		t.Fatal(err)
	}
	if n := inner.channelCalls.Load(); n != channelBase+2 {
		t.Fatalf("name-prefix sibling must stay memoized, inner calls = %d, want %d", n, channelBase+2)
	}

	// Unknown names are a silent no-op.
	cs.InvalidateTopic("never-seen")
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if n := inner.channelCalls.Load(); n != channelBase+2 {
		t.Fatalf("unknown-name invalidate must not evict, inner calls = %d, want %d", n, channelBase+2)
	}
}

func TestCachingStoreInvalidateIDs(t *testing.T) {
	inner := newCounting()
	s := store.WithCache(inner)
	ctx := context.Background()

	topicID, err := s.EnsureTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	channelID, err := s.EnsureChannel(ctx, "orders", "email")
	if err != nil {
		t.Fatal(err)
	}
	cs, ok := s.(*store.CachingStore)
	if !ok {
		t.Fatalf("expected *store.CachingStore, got %T", s)
	}

	topicBase := inner.topicCalls.Load()
	channelBase := inner.channelCalls.Load()

	cs.InvalidateTopicID(topicID)
	if _, err := s.EnsureTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if n := inner.topicCalls.Load(); n != topicBase+1 {
		t.Fatalf("EnsureTopic after InvalidateTopicID = %d inner calls (base %d), want +1", n, topicBase)
	}

	cs.InvalidateChannelID(channelID)
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if n := inner.channelCalls.Load(); n != channelBase+1 {
		t.Fatalf("EnsureChannel after InvalidateChannelID = %d inner calls (base %d), want +1", n, channelBase)
	}

	// Unknown ids must be a silent no-op, and the fresh memos stay valid.
	cs.InvalidateTopicID(1 << 40)
	cs.InvalidateChannelID(1 << 40)
	if _, err := s.EnsureTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "orders", "email"); err != nil {
		t.Fatal(err)
	}
	if n := inner.topicCalls.Load(); n != topicBase+1 {
		t.Fatalf("unknown-id invalidate must not evict, inner topic calls = %d, want %d", n, topicBase+1)
	}
	if n := inner.channelCalls.Load(); n != channelBase+1 {
		t.Fatalf("unknown-id invalidate must not evict, inner channel calls = %d, want %d", n, channelBase+1)
	}
}
