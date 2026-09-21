// Package store defines dialect-agnostic persistence contracts for novaque.
// Concrete SQL and locking live in driver packages (e.g. driver/mysql).
package store

import (
	"context"
	"time"
)

// Delivery status values shared across drivers.
const (
	StatusPending  = "pending"
	StatusInFlight = "in_flight"
	StatusDead     = "dead"
)

// Delivery is one channel's copy of a published message, possibly claimed.
type Delivery struct {
	ID          int64
	MessageID   int64
	ChannelID   int64
	Topic       string
	Channel     string
	Body        []byte
	Status      string
	Attempts    int
	MaxAttempts int
	LeaseToken  string
	AvailableAt time.Time
	LeaseUntil  time.Time
}

// PublishOpts controls per-publish retention, delay, and poison caps.
type PublishOpts struct {
	// TTL is relative retention from DB clock (Unix seconds); preferred over ExpiresAt.
	TTL time.Duration
	// ExpiresAt is an absolute expiry; used only when TTL is zero and ExpiresAt is set.
	ExpiresAt time.Time
	// Delay is relative time until deliveries become claimable (NSQ DPUB-style). Zero = immediate.
	// Must be <= MaxDelay and leave a non-empty claim window before effective expiry.
	Delay       time.Duration
	MaxAttempts int // zero = driver/client default
}

// ChannelCounters are summed day-bucket event counters (UTC) for one channel
// or topic. Values only grow within a retained day and may fall when the
// retention prune removes old day buckets; they are never decremented by
// ack deletes or message TTL purges.
type ChannelCounters struct {
	Publish int64
	Claim   int64
	Ack     int64
	Requeue int64
	Dead    int64
	Purge   int64
}

// ChannelBacklog is a live snapshot of delivery row counts for one channel.
// Ready is the claimable slice of Pending (delayed publishes count as Pending
// only until available_at passes the DB clock).
type ChannelBacklog struct {
	Pending  int64
	Ready    int64
	InFlight int64
	Dead     int64
}

// Store is the persistence seam used by Client, Consumer, and maintenance loops.
// Method names must stay dialect-agnostic (no SKIP LOCKED / MySQL identifiers).
type Store interface {
	Migrate(ctx context.Context) error

	EnsureTopic(ctx context.Context, name string) (topicID int64, err error)
	EnsureChannel(ctx context.Context, topic, channel string) (channelID int64, err error)

	// Publish inserts a message and one pending delivery per existing channel
	// in a single transaction. topicID must come from EnsureTopic (callers /
	// CachingStore should resolve names once). Zero channels still inserts the message row.
	Publish(ctx context.Context, topicID int64, body []byte, opts PublishOpts) (messageID int64, err error)

	// Claim leases up to limit eligible deliveries for a known channel id
	// (callers must EnsureChannel once — Client caches via CachingStore; hot path must not re-resolve names).
	// Drivers increment attempts on claim; deliveries past MaxAttempts become dead
	// and are not returned. leaseFor is applied using database Unix seconds.
	Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]Delivery, error)

	// Ack completes a delivery when lease_token still matches.
	Ack(ctx context.Context, deliveryID int64, leaseToken string) error

	// Requeue returns an in-flight delivery to pending when lease_token matches.
	Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error

	// ReapExpiredLeases resets in_flight rows whose lease_until is in the past.
	ReapExpiredLeases(ctx context.Context, limit int) (affected int64, err error)

	// PurgeExpired deletes expired messages and their non-live deliveries.
	PurgeExpired(ctx context.Context, limit int) (affected int64, err error)

	// ChannelCounters sums the retained day-bucket event counters for one
	// channel (channelID must come from EnsureChannel). Zero counters are
	// returned for a channel with no recorded events.
	ChannelCounters(ctx context.Context, channelID int64) (ChannelCounters, error)

	// TopicCounters rolls up the retained day-bucket event counters over every
	// row stored for a topic (per-channel rows plus the zero-channel sentinel
	// row that records zero-channel publishes), so a plain SUM is correct.
	TopicCounters(ctx context.Context, topicID int64) (ChannelCounters, error)

	// ChannelBacklog returns live pending / ready / in_flight / dead delivery
	// counts for one channel (channelID must come from EnsureChannel).
	ChannelBacklog(ctx context.Context, channelID int64) (ChannelBacklog, error)

	// PruneStats deletes day-bucket counter rows older than retentionDays
	// (UTC days, boundary from the DB clock) and returns rows deleted.
	PruneStats(ctx context.Context, retentionDays int) (deleted int64, err error)

	// FlushStats drains any buffered counter deltas into the stats table.
	// Drivers may buffer increments in-process and apply them in batches
	// instead of writing counters inside mutation transactions; a no-op
	// return is valid when nothing is buffered.
	FlushStats(ctx context.Context) error
}
