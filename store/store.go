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
	// ID identifies this channel's copy, not the message itself (see
	// MessageID); it is the id presented on Ack and Requeue.
	ID int64
	// MessageID identifies the shared message row that fanned out to this
	// delivery and its sibling deliveries on the topic's other channels.
	MessageID int64
	// ChannelID is the numeric id of the destination channel; the claim hot
	// path addresses channels by id, never by name.
	ChannelID int64
	// Topic and Channel are the resolved destination names of this delivery.
	Topic   string
	Channel string
	// Body is the message payload, identical across every channel copy.
	Body []byte
	// Status is one of StatusPending, StatusInFlight, or StatusDead.
	Status string
	// Attempts is the number of claims so far, counting the claim that
	// produced this row; after MaxAttempts total claims the delivery goes dead.
	Attempts int
	// MaxAttempts is this delivery's claim cap before it becomes dead.
	MaxAttempts int
	// LeaseToken is the fencing token issued by Claim; Ack and Requeue must
	// present it unchanged or the mutation is rejected.
	LeaseToken string
	// AvailableAt is the earliest time this delivery can be claimed, on the
	// database clock; a delayed publish sets it past the publish time.
	AvailableAt time.Time
	// LeaseUntil is when the current claim's lease expires; the reaper
	// returns expired leases to pending for at-least-once redelivery.
	LeaseUntil time.Time
}

// PublishOpts controls per-publish retention, delay, and poison caps.
type PublishOpts struct {
	// TTL is relative retention from DB clock (Unix seconds); preferred over ExpiresAt.
	TTL time.Duration
	// ExpiresAt is an absolute expiry; used only when TTL is zero and ExpiresAt is set.
	ExpiresAt time.Time
	// Delay is relative time until deliveries become claimable (NSQ DPUB-style). Zero = immediate.
	// Must be <= MaxDelay and leave a non-empty claim window before effective expiry.
	Delay time.Duration
	// MaxAttempts is the poison threshold for this message's deliveries;
	// zero falls back to the driver/client default.
	MaxAttempts int
}

// ChannelCounters are summed day-bucket event counters (UTC) for one channel
// or topic. Values only grow within a retained day and may fall when the
// retention prune removes old day buckets; they are never decremented by
// ack deletes or message TTL purges.
type ChannelCounters struct {
	// Publish counts deliveries created by fan-out, one per publish per
	// channel; zero-channel publishes count on a topic-level row only.
	Publish int64
	// Claim counts successful claims, including the final claim that
	// terminalizes a poisoned delivery (which also counts in Dead).
	Claim int64
	// Ack counts deliveries completed by an ack with a matching lease token.
	Ack int64
	// Requeue counts handler-driven requeues only; lease reaps never count.
	Requeue int64
	// Dead counts deliveries that exceeded MaxAttempts.
	Dead int64
	// Purge counts deliveries removed by TTL purge.
	Purge int64
}

// ChannelBacklog is a live snapshot of delivery row counts for one channel.
// Ready is the claimable slice of Pending and mirrors Claim's eligibility:
// available_at has passed the DB clock AND the TTL has not expired (delayed
// publishes count as Pending only until due; expired-but-unpurged rows stay
// Pending but are never Ready).
type ChannelBacklog struct {
	// Pending counts deliveries waiting to be claimed, including delayed
	// ones not yet due and TTL-expired rows not yet purged.
	Pending int64
	// Ready is the claimable slice of Pending: due now and not expired.
	Ready int64
	// InFlight counts deliveries currently leased to a handler.
	InFlight int64
	// Dead counts deliveries that exceeded MaxAttempts.
	Dead int64
}

// Store is the persistence seam used by Client, Consumer, and maintenance loops.
// Method names must stay dialect-agnostic (no SKIP LOCKED / MySQL identifiers).
type Store interface {
	// Migrate creates or upgrades the schema to the current version. It is
	// idempotent — safe to run on every startup — and must succeed before
	// the store is first used.
	Migrate(ctx context.Context) error

	// EnsureTopic returns the numeric id of the named topic, creating the
	// topic when missing. Drivers enforce name rules; the MySQL driver
	// accepts 1..64 characters of [a-zA-Z0-9._-].
	EnsureTopic(ctx context.Context, name string) (topicID int64, err error)

	// EnsureChannel returns the numeric id of the named channel under
	// topic, creating the topic and channel when missing. Name rules are as
	// per EnsureTopic.
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

	// Requeue returns an in-flight delivery to pending when lease_token
	// matches; a token mismatch or non-in_flight state is an error. A zero
	// availableAt makes the delivery available now, judged by the DB clock.
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
