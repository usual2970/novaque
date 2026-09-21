package store

import (
	"context"
	"sync"
	"time"
)

// CachingStore memos EnsureTopic / EnsureChannel results around an inner Store.
// Topic and channel ids are immutable once created, so no invalidation is needed
// until a future delete API exists. Wrap at Open so all drivers share the cache.
type CachingStore struct {
	Inner Store

	topics   sync.Map // name -> int64
	channels sync.Map // topic\x00channel -> int64
}

// WithCache returns a Store that caches EnsureTopic/EnsureChannel.
func WithCache(inner Store) Store {
	if inner == nil {
		return nil
	}
	if _, ok := inner.(*CachingStore); ok {
		return inner
	}
	return &CachingStore{Inner: inner}
}

func channelKey(topic, channel string) string {
	return topic + "\x00" + channel
}

func (c *CachingStore) Migrate(ctx context.Context) error {
	return c.Inner.Migrate(ctx)
}

func (c *CachingStore) EnsureTopic(ctx context.Context, name string) (int64, error) {
	if v, ok := c.topics.Load(name); ok {
		return v.(int64), nil
	}
	id, err := c.Inner.EnsureTopic(ctx, name)
	if err != nil {
		return 0, err
	}
	c.topics.Store(name, id)
	return id, nil
}

func (c *CachingStore) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	key := channelKey(topic, channel)
	if v, ok := c.channels.Load(key); ok {
		return v.(int64), nil
	}
	// Warm topic cache so a later Publish on the same topic is a memory hit.
	if _, err := c.EnsureTopic(ctx, topic); err != nil {
		return 0, err
	}
	id, err := c.Inner.EnsureChannel(ctx, topic, channel)
	if err != nil {
		return 0, err
	}
	c.channels.Store(key, id)
	return id, nil
}

func (c *CachingStore) Publish(ctx context.Context, topicID int64, body []byte, opts PublishOpts) (int64, error) {
	return c.Inner.Publish(ctx, topicID, body, opts)
}

func (c *CachingStore) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]Delivery, error) {
	return c.Inner.Claim(ctx, channelID, owner, leaseFor, limit)
}

func (c *CachingStore) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	return c.Inner.Ack(ctx, deliveryID, leaseToken)
}

func (c *CachingStore) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	return c.Inner.Requeue(ctx, deliveryID, leaseToken, availableAt)
}

func (c *CachingStore) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	return c.Inner.ReapExpiredLeases(ctx, limit)
}

func (c *CachingStore) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	return c.Inner.PurgeExpired(ctx, limit)
}

// Stats reads, backlog counts, and prune are pure passthroughs: counters and
// delivery rows mutate constantly, so CachingStore must never memoize them.
func (c *CachingStore) ChannelCounters(ctx context.Context, channelID int64) (ChannelCounters, error) {
	return c.Inner.ChannelCounters(ctx, channelID)
}

func (c *CachingStore) TopicCounters(ctx context.Context, topicID int64) (ChannelCounters, error) {
	return c.Inner.TopicCounters(ctx, topicID)
}

func (c *CachingStore) ChannelBacklog(ctx context.Context, channelID int64) (ChannelBacklog, error) {
	return c.Inner.ChannelBacklog(ctx, channelID)
}

func (c *CachingStore) PruneStats(ctx context.Context, retentionDays int) (int64, error) {
	return c.Inner.PruneStats(ctx, retentionDays)
}

func (c *CachingStore) FlushStats(ctx context.Context) error {
	return c.Inner.FlushStats(ctx)
}
