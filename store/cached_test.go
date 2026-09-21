package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novaque/store"
)

type countingStore struct {
	topicCalls   atomic.Int64
	channelCalls atomic.Int64
	topics       sync.Map
	channels     sync.Map
	nextID       atomic.Int64
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

func TestWithCacheIdempotent(t *testing.T) {
	inner := newCounting()
	s1 := store.WithCache(inner)
	s2 := store.WithCache(s1)
	if s1 != s2 {
		t.Fatal("WithCache should not double-wrap")
	}
}
