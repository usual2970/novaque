package store

import (
	"context"
	"strings"
	"sync"
	"time"
)

// CachingStore memos EnsureTopic / EnsureChannel results around an inner Store.
// Topic and channel ids are immutable once created, so entries never go stale
// while the rows exist; the admin delete API is the one source of staleness,
// and it is handled: DeleteTopic / DeleteChannel sweep every memoized entry
// resolving to the deleted id before forwarding, and InvalidateTopicID /
// InvalidateChannelID evict entries explicitly (the Client publish self-heal
// after ErrTopicGone). Wrap at Open so all drivers share the cache.
// The caches are sync.Maps, so a CachingStore is safe for concurrent use;
// every other call — mutations, stats reads, backlog counts, prune, flush,
// and the whole admin surface — passes straight through to Inner because
// counters and delivery rows mutate constantly.
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

// channelKeySep joins a topic and channel name into one cache key; topic
// names cannot contain it (driver name rules), so a topic+sep prefix cannot
// straddle two topics.
const channelKeySep = "\x00"

func channelKey(topic, channel string) string {
	return topic + channelKeySep + channel
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

// --- Admin surface (mountable admin UI): pure passthrough, never memoized
// (plan 004 R7 posture — live listings, counters, and dead rows mutate
// constantly), with cache sweeps on the two delete paths. ---

// ListTopics passes through to Inner.ListTopics.
func (c *CachingStore) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	return c.Inner.ListTopics(ctx)
}

// ListChannels passes through to Inner.ListChannels.
func (c *CachingStore) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	return c.Inner.ListChannels(ctx)
}

// Backlogs passes through to Inner.Backlogs; backlog is a live count, never
// cached.
func (c *CachingStore) Backlogs(ctx context.Context) ([]BacklogRow, error) {
	return c.Inner.Backlogs(ctx)
}

// TopicDailyCounters passes through to Inner.TopicDailyCounters; never cached
// (see ChannelCounters).
func (c *CachingStore) TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]DailyCounters, error) {
	return c.Inner.TopicDailyCounters(ctx, topicID, days)
}

// ChannelDailyCounters passes through to Inner.ChannelDailyCounters; never
// cached (see ChannelCounters).
func (c *CachingStore) ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]DailyCounters, error) {
	return c.Inner.ChannelDailyCounters(ctx, channelID, days)
}

// ListDead passes through to Inner.ListDead.
func (c *CachingStore) ListDead(ctx context.Context, channelID int64, before int64, limit int) ([]DeadDelivery, error) {
	return c.Inner.ListDead(ctx, channelID, before, limit)
}

// RequeueDead passes through to Inner.RequeueDead.
func (c *CachingStore) RequeueDead(ctx context.Context, deliveryID int64, freshTTL time.Duration) error {
	return c.Inner.RequeueDead(ctx, deliveryID, freshTTL)
}

// DeleteDead passes through to Inner.DeleteDead.
func (c *CachingStore) DeleteDead(ctx context.Context, deliveryID int64) error {
	return c.Inner.DeleteDead(ctx, deliveryID)
}

// DeleteTopic evicts this process's memoized ids for the topic — its name
// entry plus every channel entry under it (R11: the deleting process must not
// keep publishing or subscribing against the stale id) — before forwarding to
// Inner.DeleteTopic. Topic names cannot contain \x00 (driver name rules), so
// a topicName\x00 channel-key prefix cannot straddle two topics.
func (c *CachingStore) DeleteTopic(ctx context.Context, topicID int64) error {
	var prefixes []string
	c.topics.Range(func(name, id any) bool {
		if id.(int64) == topicID {
			c.topics.Delete(name)
			prefixes = append(prefixes, name.(string)+channelKeySep)
		}
		return true
	})
	if len(prefixes) > 0 {
		c.channels.Range(func(key, _ any) bool {
			for _, p := range prefixes {
				if strings.HasPrefix(key.(string), p) {
					c.channels.Delete(key)
					break
				}
			}
			return true
		})
	}
	return c.Inner.DeleteTopic(ctx, topicID)
}

// DeleteChannel evicts this process's memoized channel entries resolving to
// channelID before forwarding to Inner.DeleteChannel; entries for sibling
// channels and the topic itself are untouched.
func (c *CachingStore) DeleteChannel(ctx context.Context, channelID int64) error {
	c.InvalidateChannelID(channelID)
	return c.Inner.DeleteChannel(ctx, channelID)
}

// InvalidateTopicID drops the memoized topic name→id entry resolving to
// topicID, if present. It is the eviction half of the Client.Publish
// self-heal after ErrTopicGone — only Client knows the topic name to
// re-Ensure, so the retry lives there. A no-op for unknown ids; channel
// entries are not its concern (DeleteTopic sweeps those on real deletes).
func (c *CachingStore) InvalidateTopicID(topicID int64) {
	c.topics.Range(func(name, id any) bool {
		if id.(int64) == topicID {
			c.topics.Delete(name)
		}
		return true
	})
}

// InvalidateChannelID drops the memoized channel entry resolving to
// channelID, if present; a no-op for unknown ids.
func (c *CachingStore) InvalidateChannelID(channelID int64) {
	c.channels.Range(func(key, id any) bool {
		if id.(int64) == channelID {
			c.channels.Delete(key)
		}
		return true
	})
}
