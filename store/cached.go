package store

import (
	"context"
	"sync"
	"time"
)

// CachingStore memos EnsureTopic / EnsureChannel results around an inner Store.
// Topic and channel ids are immutable once created, so no invalidation is needed
// until a future delete API exists. Wrap at Open so all drivers share the cache.
// The caches are sync.Maps, so a CachingStore is safe for concurrent use;
// stats reads, backlog counts, and prune always pass straight through to
// Inner because counters and delivery rows mutate constantly.
type CachingStore struct {
	// Inner is the wrapped Store; every non-cached call lands here.
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

// Migrate runs Inner.Migrate verbatim; schema state is never cached.
func (c *CachingStore) Migrate(ctx context.Context) error {
	return c.Inner.Migrate(ctx)
}

// EnsureTopic returns the topic id, answering from cache after the first
// successful call for a name; a miss delegates to Inner.EnsureTopic and
// caches the result.
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

// EnsureChannel returns the channel id, answering from cache after the first
// successful call for a (topic, channel) pair; a miss delegates to
// Inner.EnsureChannel (warming the topic cache on the way) and caches the
// result.
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

// Publish passes through to Inner.Publish; message and delivery writes are
// never cached.
func (c *CachingStore) Publish(ctx context.Context, topicID int64, body []byte, opts PublishOpts) (int64, error) {
	return c.Inner.Publish(ctx, topicID, body, opts)
}

// Claim passes through to Inner.Claim.
func (c *CachingStore) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]Delivery, error) {
	return c.Inner.Claim(ctx, channelID, owner, leaseFor, limit)
}

// Ack passes through to Inner.Ack.
func (c *CachingStore) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	return c.Inner.Ack(ctx, deliveryID, leaseToken)
}

// Requeue passes through to Inner.Requeue.
func (c *CachingStore) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	return c.Inner.Requeue(ctx, deliveryID, leaseToken, availableAt)
}

// ReapExpiredLeases passes through to Inner.ReapExpiredLeases.
func (c *CachingStore) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	return c.Inner.ReapExpiredLeases(ctx, limit)
}

// PurgeExpired passes through to Inner.PurgeExpired.
func (c *CachingStore) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	return c.Inner.PurgeExpired(ctx, limit)
}

// ChannelCounters passes through to Inner.ChannelCounters; counters mutate
// constantly, so reads are never cached.
func (c *CachingStore) ChannelCounters(ctx context.Context, channelID int64) (ChannelCounters, error) {
	return c.Inner.ChannelCounters(ctx, channelID)
}

// TopicCounters passes through to Inner.TopicCounters; never cached (see
// ChannelCounters).
func (c *CachingStore) TopicCounters(ctx context.Context, topicID int64) (ChannelCounters, error) {
	return c.Inner.TopicCounters(ctx, topicID)
}

// ChannelBacklog passes through to Inner.ChannelBacklog; backlog is a live
// count, never cached.
func (c *CachingStore) ChannelBacklog(ctx context.Context, channelID int64) (ChannelBacklog, error) {
	return c.Inner.ChannelBacklog(ctx, channelID)
}

// PruneStats passes through to Inner.PruneStats; never cached (see
// ChannelCounters).
func (c *CachingStore) PruneStats(ctx context.Context, retentionDays int) (int64, error) {
	return c.Inner.PruneStats(ctx, retentionDays)
}

// FlushStats passes through to Inner.FlushStats, draining Inner's buffered
// counter deltas.
func (c *CachingStore) FlushStats(ctx context.Context) error {
	return c.Inner.FlushStats(ctx)
}
