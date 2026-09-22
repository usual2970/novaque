// Package store defines dialect-agnostic persistence contracts for novaque.
// Concrete SQL and locking live in driver packages (e.g. driver/mysql).
package store

import (
	"context"
	"errors"
	"time"
)

// Delivery status values shared across drivers.
const (
	StatusPending  = "pending"
	StatusInFlight = "in_flight"
	StatusDead     = "dead"
)

// Sentinel errors shared across drivers: drivers map dialect-specific
// conditions onto them so callers stay dialect-agnostic.
var (
	// ErrTopicGone is returned by Publish when the resolved topic id no
	// longer exists — another process deleted the topic after this process
	// memoized or resolved the id. Callers may evict the id, re-Ensure the
	// name (re-creating the topic), and retry; Client.Publish does exactly
	// that once.
	ErrTopicGone = errors.New("novaque: topic no longer exists")
	// ErrDeadGone is returned by RequeueDead and DeleteDead when the
	// status = 'dead' guard matched no row — the delivery was already
	// requeued, deleted, or never dead. A second call is idempotent-safe;
	// admin surfaces may map it to success.
	ErrDeadGone = errors.New("novaque: dead delivery no longer exists")
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

// TopicInfo is one topic row for the admin listing surface. Admin reads are
// ID-addressed and never create rows (no Ensure on read).
type TopicInfo struct {
	// ID is the numeric topic id every ID-addressed admin call uses.
	ID int64
	// Name is the topic's unique name.
	Name string
}

// ChannelInfo is one channel row from the all-channels listing; TopicID lets
// the UI group channels under their topic without a second query.
type ChannelInfo struct {
	// ID is the numeric channel id every ID-addressed admin call uses.
	ID int64
	// TopicID is the owning topic's id.
	TopicID int64
	// Name is the channel's name, unique within its topic.
	Name string
}

// BacklogRow is one channel's live backlog snapshot from the batched
// all-channels query; the counts mirror ChannelBacklog's. Rows appear only
// for channels with at least one delivery — callers zero-fill the rest
// against ListChannels.
type BacklogRow struct {
	// ChannelID identifies the channel the counts belong to.
	ChannelID int64
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

// DailyCounters is one retained UTC day bucket of the event counters; the
// fields carry the same meanings as in ChannelCounters, scoped to one day.
// Only days with recorded rows are returned — zero-filling the window is the
// caller's job.
type DailyCounters struct {
	// Day is the bucket's UTC midnight; drivers return it in UTC.
	Day time.Time
	// Publish counts deliveries created by fan-out this day, one per publish
	// per channel (see ChannelCounters.Publish).
	Publish int64
	// Claim counts successful claims this day, including the final claim
	// that terminalizes a poisoned delivery (see ChannelCounters.Claim).
	Claim int64
	// Ack counts deliveries completed by an ack this day.
	Ack int64
	// Requeue counts handler-driven requeues this day, including dead-letter
	// requeues from the admin surface; lease reaps never count (see
	// ChannelCounters.Requeue).
	Requeue int64
	// Dead counts deliveries that exceeded MaxAttempts this day.
	Dead int64
	// Purge counts deliveries removed by TTL purge this day (manual
	// dead-letter deletions count here too — KTD8).
	Purge int64
}

// DeadDelivery is one dead delivery row for the admin dead-letter browse —
// the only admin surface that carries message bodies (R12).
type DeadDelivery struct {
	// ID identifies the delivery row; requeue and delete address it directly.
	ID int64
	// MessageID identifies the shared message row this delivery fanned out
	// from.
	MessageID int64
	// ChannelID is the numeric id of the channel the delivery belongs to.
	ChannelID int64
	// Topic and Channel are the resolved destination names of the delivery.
	Topic   string
	Channel string
	// Body is the message payload, identical across every channel copy. A
	// list read caps it at the caller's body prefix while a per-delivery
	// read returns it in full; BodyLen always carries the full length.
	Body []byte
	// BodyLen is the payload's true byte length — OCTET_LENGTH of the stored
	// body — so a prefixed Body can still report its full size and flag the
	// truncation (review #13: list reads never join full LONGBLOBs).
	BodyLen int64
	// Status is always StatusDead here; carried for uniformity with Delivery.
	Status string
	// Attempts is the number of claims at death; RequeueDead resets it.
	Attempts int
	// MaxAttempts is the claim cap that was exceeded.
	MaxAttempts int
	// AvailableAt is the earliest claim time recorded on the row, on the
	// database clock.
	AvailableAt time.Time
	// ExpiresAt is the delivery's TTL expiry on the database clock; the dead
	// list renders remaining TTL from it and RequeueDead writes a fresh one.
	ExpiresAt time.Time
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

	// --- Admin surface (mountable admin UI). Listing and detail reads are
	// ID-addressed and never create rows — no Ensure on read — and backlog /
	// counter reads are batched, never per-channel N+1. ---

	// ListTopics returns every topic, ascending by name.
	ListTopics(ctx context.Context) ([]TopicInfo, error)

	// ListChannels returns every channel across all topics — each carrying
	// TopicID — ordered by topic name then channel name, so the UI can group
	// without re-sorting.
	ListChannels(ctx context.Context) ([]ChannelInfo, error)

	// Backlogs returns live per-channel backlog counts for every channel in
	// one query. Rows appear only for channels with at least one delivery;
	// callers zero-fill the rest against ListChannels.
	Backlogs(ctx context.Context) ([]BacklogRow, error)

	// BacklogsForTopic scopes that batched backlog aggregate to one topic's
	// channels — the detail-page counterpart of Backlogs, so a single
	// topic/channel page never pays the all-channels GROUP BY. Same row
	// semantics: rows only for channels with at least one delivery, callers
	// zero-fill the rest against ListChannels.
	BacklogsForTopic(ctx context.Context, topicID int64) ([]BacklogRow, error)

	// TopicDailyCounters returns the retained day-bucket counter rows for a
	// topic — its per-channel rows plus the zero-channel sentinel row rolled
	// up per day — over the trailing days-day UTC window ending today (day
	// boundary from the DB clock). Existing-day rows only, ascending by day;
	// zero-filling the window is the caller's job. days must be >= 1.
	TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]DailyCounters, error)

	// ChannelDailyCounters returns the retained day-bucket counter rows for
	// one channel over the trailing days-day UTC window ending today (day
	// boundary from the DB clock). Existing-day rows only, ascending by day;
	// zero-filling the window is the caller's job. days must be >= 1.
	ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]DailyCounters, error)

	// ListDead returns a channel's dead deliveries newest-first (id DESC),
	// keyset-paginated: before > 0 returns only rows with id < before; limit
	// bounds the page, non-positive falling back to a driver default.
	// bodyPrefix > 0 caps each row's Body at that many bytes — pushed into
	// the read itself (review #13) so a polled list page never pulls the
	// full LONGBLOB — while BodyLen always carries the full length;
	// bodyPrefix <= 0 fetches whole bodies.
	ListDead(ctx context.Context, channelID int64, before int64, limit int, bodyPrefix int) ([]DeadDelivery, error)

	// RequeueDead returns one dead delivery of channelID to pending —
	// attempts reset, lease cleared, available now — writing freshTTL as its
	// new expiry on both the delivery and its message. Guarded on
	// channel_id = channelID AND status = dead (review #10: the caller's
	// channel scopes the mutation): ErrDeadGone is returned when no dead row
	// of that channel matched — already requeued or deleted, or the delivery
	// belongs to a different channel — so retries land as idempotent success.
	RequeueDead(ctx context.Context, deliveryID, channelID int64, freshTTL time.Duration) error

	// DeleteDead removes one dead delivery of channelID; the shared message
	// row is reclaimed by the orphan purge once its sibling deliveries are
	// gone. Guarded on channel_id = channelID AND status = dead (review
	// #10): ErrDeadGone when no dead row of that channel matched.
	DeleteDead(ctx context.Context, deliveryID, channelID int64) error

	// DeleteTopic removes the topic and everything under it — channels,
	// messages, deliveries, and retained stats rows including the
	// zero-channel sentinel — in one transaction. Idempotent: deleting an
	// already-deleted id succeeds as a no-op.
	DeleteTopic(ctx context.Context, topicID int64) error

	// DeleteChannel removes one channel's deliveries and stats rows in one
	// transaction, keeping the shared message rows so sibling channels keep
	// their deliveries; orphans are reclaimed by the orphan purge.
	// Idempotent: deleting an already-deleted id succeeds as a no-op.
	DeleteChannel(ctx context.Context, channelID int64) error
}
