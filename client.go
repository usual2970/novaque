// Package novaque provides an embeddable, NSQ-style topic→channel pub/sub
// queue backed by a relational database. The host supplies the *sql.DB and
// novaque runs in-process — no broker daemon. Delivery is at-least-once with
// lease-based claiming and explicit ack, so handlers must be idempotent.
// The MySQL driver under driver/mysql implements the dialect-agnostic
// store.Store seam; other backends plug in the same way.
package novaque

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/usual2970/novaque/store"
)

// Options configure Client defaults. Zero-valued fields fall back to the
// per-field defaults below; a nil Logger becomes a silent Nop.
type Options struct {
	// DefaultTTL is message retention when a publish omits TTL. Default 7d.
	DefaultTTL time.Duration
	// DefaultLease is the claim lease duration and the handler context
	// timeout. Default 30s.
	DefaultLease time.Duration
	// DefaultMaxAttempts is the poison threshold when a publish omits
	// MaxAttempts; deliveries past it go dead. Default 5.
	DefaultMaxAttempts int
	// PollInterval is the consumer idle poll base, applied with ±50% jitter.
	// Default 200ms.
	PollInterval time.Duration
	// MaxInFlight is concurrent claim+handle workers per Consumer (each claims 1).
	// Raise this for in-process concurrency; add more OS processes for multi-node scale.
	// Default 1.
	MaxInFlight int
	// ReapInterval is the expired-lease reaper tick. Default 1s.
	ReapInterval time.Duration
	// PurgeInterval is the TTL cleanup tick. Default 5s.
	PurgeInterval time.Duration
	// MaintenanceBatch is rows per reaper/purge pass. Default 100.
	MaintenanceBatch int
	// StatsRetentionDays keeps day-bucket counter rows for this many UTC days
	// before the prune tick (or an explicit PruneStats call) deletes them.
	// Default 30.
	StatsRetentionDays int
	// StatsFlushInterval is how often the maintenance loop drains the driver's
	// buffered counter deltas into the stats table. Default 2s.
	StatsFlushInterval time.Duration
	// StatsPruneInterval is how often the maintenance loop prunes day-bucket
	// counter rows older than StatsRetentionDays. Dedicated field (not a
	// PurgeInterval co-tick) so prune stays gentle and unit-testable.
	// Default 1h.
	StatsPruneInterval time.Duration
	// Logger receives structured operational logs (Debug on success, Error on
	// swallowed failures; message bodies are never logged). nil = silent
	// built-in zap Nop default; pass Zap(yourZapLogger) to inject.
	Logger Logger
}

func (o Options) withDefaults() Options {
	if o.DefaultTTL <= 0 {
		o.DefaultTTL = 7 * 24 * time.Hour
	}
	if o.DefaultLease <= 0 {
		o.DefaultLease = 30 * time.Second
	}
	if o.DefaultMaxAttempts <= 0 {
		o.DefaultMaxAttempts = 5
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = 1
	}
	if o.ReapInterval <= 0 {
		o.ReapInterval = time.Second
	}
	if o.PurgeInterval <= 0 {
		o.PurgeInterval = 5 * time.Second
	}
	if o.MaintenanceBatch <= 0 {
		o.MaintenanceBatch = 100
	}
	if o.StatsRetentionDays <= 0 {
		o.StatsRetentionDays = 30
	}
	if o.StatsFlushInterval <= 0 {
		o.StatsFlushInterval = 2 * time.Second
	}
	if o.StatsPruneInterval <= 0 {
		o.StatsPruneInterval = time.Hour
	}
	if o.Logger == nil {
		o.Logger = defaultLogger()
	}
	return o
}

// Client is the domain entrypoint; it depends only on store.Store.
type Client struct {
	store store.Store
	opts  Options

	mu      sync.Mutex
	started bool
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

// logger returns the Client's Logger; never nil (silent Nop fallback).
func (c *Client) logger() Logger {
	if c.opts.Logger == nil {
		return defaultLogger()
	}
	return c.opts.Logger
}

// Open constructs a Client from any Store implementation (e.g. mysql.New(db)).
// EnsureTopic/EnsureChannel results are memoized in process for all drivers.
func Open(s store.Store, opts Options) (*Client, error) {
	if s == nil {
		return nil, errors.New("novaque: store is nil")
	}
	return &Client{store: store.WithCache(s), opts: opts.withDefaults()}, nil
}

// Migrate applies the store's schema.
func (c *Client) Migrate(ctx context.Context) error {
	return c.store.Migrate(ctx)
}

// Start begins shared reaper, TTL purge, and stats maintenance loops
// (exactly once per Client). A nil ctx defaults to context.Background();
// a duplicate Start is a no-op.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	base := ctx
	if base == nil {
		base = context.Background()
	}
	runCtx, cancel := context.WithCancel(base)
	c.stop = cancel
	c.started = true

	c.wg.Add(3)
	go c.loopReap(runCtx)
	go c.loopPurge(runCtx)
	go c.loopStats(runCtx)
	c.mu.Unlock()

	// Log after the transition and outside the mutex: duplicate Start stays silent.
	c.logger().Info("client started")
	return nil
}

// Shutdown stops maintenance loops and waits for them to exit, then performs
// one final best-effort FlushStats (5s timeout). ctx is a non-nil wait
// deadline: if it expires before the loops finish, Shutdown returns
// ctx.Err(). Shutdown of a never-started Client is a no-op.
func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	stop := c.stop
	c.started = false
	c.stop = nil
	c.mu.Unlock()

	stop()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// Loops have stopped: one final best-effort drain so graceful shutdown
		// does not lose the last flush window. Failure is logged, not returned.
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.store.FlushStats(flushCtx); err != nil {
			c.logger().Error("stats flush failed", zap.String("op", "flush_stats"), zap.NamedError("err", err))
		}
		cancel()
		c.logger().Info("client shutdown")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) loopReap(ctx context.Context) {
	defer c.wg.Done()
	t := time.NewTicker(c.opts.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.store.ReapExpiredLeases(ctx, c.opts.MaintenanceBatch); err != nil && ctx.Err() == nil {
				// Swallowed failure: only surface when not caused by shutdown cancel.
				c.logger().Error("reap failed", zap.String("op", "reap"), zap.NamedError("err", err))
			}
		}
	}
}

func (c *Client) loopPurge(ctx context.Context) {
	defer c.wg.Done()
	t := time.NewTicker(c.opts.PurgeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.store.PurgeExpired(ctx, c.opts.MaintenanceBatch); err != nil && ctx.Err() == nil {
				// Swallowed failure: only surface when not caused by shutdown cancel.
				c.logger().Error("purge failed", zap.String("op", "purge"), zap.NamedError("err", err))
			}
		}
	}
}

// loopStats drains the driver's buffered counter deltas on StatsFlushInterval
// and prunes day-bucket rows past retention on StatsPruneInterval.
func (c *Client) loopStats(ctx context.Context) {
	defer c.wg.Done()
	flushT := time.NewTicker(c.opts.StatsFlushInterval)
	defer flushT.Stop()
	pruneT := time.NewTicker(c.opts.StatsPruneInterval)
	defer pruneT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-flushT.C:
			if err := c.store.FlushStats(ctx); err != nil && ctx.Err() == nil {
				// Swallowed failure: only surface when not caused by shutdown cancel.
				c.logger().Error("stats flush failed", zap.String("op", "flush_stats"), zap.NamedError("err", err))
			}
		case <-pruneT.C:
			if _, err := c.store.PruneStats(ctx, c.opts.StatsRetentionDays); err != nil && ctx.Err() == nil {
				// Swallowed failure: only surface when not caused by shutdown cancel.
				c.logger().Error("stats prune failed", zap.String("op", "prune_stats"), zap.NamedError("err", err))
			}
		}
	}
}

// --- Stats API: day-bucket counters plus live backlog (ids and counts only,
// never message payloads) ---

// ChannelCounters are summed day-bucket event counters (UTC) for one channel
// or topic (re-export of store.ChannelCounters).
type ChannelCounters = store.ChannelCounters

// ChannelBacklog is a live pending/ready/in_flight/dead snapshot for one
// channel (re-export of store.ChannelBacklog).
type ChannelBacklog = store.ChannelBacklog

// ChannelCounters returns the summed day-bucket event counters retained for
// one channel: publish, claim, ack, requeue, dead, purge. Counters are UTC
// day buckets that survive ack deletes and TTL purges until the retention
// prune removes old days. They are buffered in-process and flushed every
// StatsFlushInterval, so reads are eventually consistent within that window;
// call FlushStats first to force a drain.
//
// Like Publish/Subscribe, the read creates the named topic and channel when
// they do not exist yet (create-on-read) — a typo'd name therefore shows zeros
// AND permanently joins future publish fan-out. Double-check names.
func (c *Client) ChannelCounters(ctx context.Context, topic, channel string) (ChannelCounters, error) {
	channelID, err := c.store.EnsureChannel(ctx, topic, channel)
	if err != nil {
		return ChannelCounters{}, fmt.Errorf("novaque channel counters: %w", err)
	}
	counters, err := c.store.ChannelCounters(ctx, channelID)
	if err != nil {
		return ChannelCounters{}, fmt.Errorf("novaque channel counters: %w", err)
	}
	return counters, nil
}

// TopicCounters rolls the day-bucket counters up over a whole topic: every
// per-channel row plus the topic-level row that zero-channel publishes count
// on. Eventual-consistency window as per ChannelCounters. The read creates the
// topic when missing (see ChannelCounters).
func (c *Client) TopicCounters(ctx context.Context, topic string) (ChannelCounters, error) {
	topicID, err := c.store.EnsureTopic(ctx, topic)
	if err != nil {
		return ChannelCounters{}, fmt.Errorf("novaque topic counters: %w", err)
	}
	counters, err := c.store.TopicCounters(ctx, topicID)
	if err != nil {
		return ChannelCounters{}, fmt.Errorf("novaque topic counters: %w", err)
	}
	return counters, nil
}

// ChannelBacklog returns the live delivery counts for one channel right now:
// pending, ready (the claimable slice of pending — delayed publishes are
// excluded until available_at passes, and expired-but-unpurged rows are not
// claimable), in_flight, and dead. Backlog is a live row count, not a day
// bucket, so it needs no flush. The read creates the topic and channel when
// missing (see ChannelCounters).
func (c *Client) ChannelBacklog(ctx context.Context, topic, channel string) (ChannelBacklog, error) {
	channelID, err := c.store.EnsureChannel(ctx, topic, channel)
	if err != nil {
		return ChannelBacklog{}, fmt.Errorf("novaque channel backlog: %w", err)
	}
	b, err := c.store.ChannelBacklog(ctx, channelID)
	if err != nil {
		return ChannelBacklog{}, fmt.Errorf("novaque channel backlog: %w", err)
	}
	return b, nil
}

// PruneStats deletes day-bucket counter rows older than StatsRetentionDays
// (UTC days) and returns the number of rows deleted. Start runs this on
// StatsPruneInterval; hosts that never Start can call it explicitly.
func (c *Client) PruneStats(ctx context.Context) (int64, error) {
	deleted, err := c.store.PruneStats(ctx, c.opts.StatsRetentionDays)
	if err != nil {
		return 0, fmt.Errorf("novaque prune stats: %w", err)
	}
	return deleted, nil
}

// FlushStats drains the driver's buffered counter deltas into the stats
// table. Start runs this on StatsFlushInterval and Shutdown performs one
// final best-effort flush; hosts that never Start can call it explicitly.
func (c *Client) FlushStats(ctx context.Context) error {
	if err := c.store.FlushStats(ctx); err != nil {
		return fmt.Errorf("novaque flush stats: %w", err)
	}
	return nil
}

// --- Admin API (mountable admin UI): ID-addressed reads and mutations over
// the admin Store surface. Read paths never create rows — no Ensure on read
// (KTD6); ids come from the list methods. CreateTopic/CreateChannel are the
// only Ensure-backed calls, which makes them idempotent by construction. ---

// TopicInfo is one topic row from the admin listing (re-export of
// store.TopicInfo).
type TopicInfo = store.TopicInfo

// ChannelInfo is one channel row from the all-channels admin listing
// (re-export of store.ChannelInfo).
type ChannelInfo = store.ChannelInfo

// BacklogRow is one channel's live backlog snapshot from the batched
// all-channels query (re-export of store.BacklogRow).
type BacklogRow = store.BacklogRow

// DailyCounters is one retained UTC day bucket of the event counters
// (re-export of store.DailyCounters).
type DailyCounters = store.DailyCounters

// DeadDelivery is one dead delivery row for the dead-letter browse — the only
// admin surface carrying message bodies (re-export of store.DeadDelivery).
type DeadDelivery = store.DeadDelivery

var (
	// ErrTopicGone is returned (wrapped) by Publish when the resolved topic
	// id no longer exists; Client.Publish self-heals it once (re-export of
	// store.ErrTopicGone; compare with errors.Is).
	ErrTopicGone = store.ErrTopicGone
	// ErrDeadGone is returned (wrapped) by RequeueDead and DeleteDead when
	// the delivery was no longer dead — already requeued or deleted — so a
	// retry is idempotent-safe (re-export of store.ErrDeadGone).
	ErrDeadGone = store.ErrDeadGone
)

// ListTopics returns every topic, ascending by name (KTD6: a straight store
// read — no Ensure, so listing never creates rows).
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	topics, err := c.store.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("novaque list topics: %w", err)
	}
	return topics, nil
}

// ListChannels returns every channel across all topics — each carrying its
// TopicID — ordered by topic name then channel name (KTD6: no Ensure).
func (c *Client) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	channels, err := c.store.ListChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("novaque list channels: %w", err)
	}
	return channels, nil
}

// Backlogs returns live per-channel backlog counts for every channel in one
// query. Rows appear only for channels with at least one delivery; zero-fill
// the rest against ListChannels. Backlog is a live row count, so it needs no
// FlushStats and never Ensures (KTD6).
func (c *Client) Backlogs(ctx context.Context) ([]BacklogRow, error) {
	rows, err := c.store.Backlogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("novaque backlogs: %w", err)
	}
	return rows, nil
}

// BacklogsForTopic scopes that batched backlog read to one topic's channels
// in a single query — the detail-page counterpart of Backlogs, which pays
// the all-channels aggregate. Row and zero-fill semantics as per Backlogs;
// never Ensures (KTD6).
func (c *Client) BacklogsForTopic(ctx context.Context, topicID int64) ([]BacklogRow, error) {
	rows, err := c.store.BacklogsForTopic(ctx, topicID)
	if err != nil {
		return nil, fmt.Errorf("novaque backlogs for topic: %w", err)
	}
	return rows, nil
}

// ChannelBacklogByID returns the live backlog snapshot for one channel id —
// the ID-addressed, no-Ensure counterpart of the name-addressed
// ChannelBacklog, for surfaces that already resolved the id (the admin
// channel page's single box). A channel with no deliveries zero-fills.
func (c *Client) ChannelBacklogByID(ctx context.Context, channelID int64) (ChannelBacklog, error) {
	b, err := c.store.ChannelBacklog(ctx, channelID)
	if err != nil {
		return ChannelBacklog{}, fmt.Errorf("novaque channel backlog: %w", err)
	}
	return b, nil
}

// CreateTopic creates the named topic under the existing driver name rules
// and returns its id. Ensure-backed, so creating an existing topic resolves
// the same id instead of erroring (idempotent, R5).
func (c *Client) CreateTopic(ctx context.Context, name string) (int64, error) {
	id, err := c.store.EnsureTopic(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("novaque create topic: %w", err)
	}
	return id, nil
}

// CreateChannel creates the named channel under topic (creating the topic
// when missing) and returns the channel id. Ensure-backed and idempotent
// (R5). A channel created after a publish receives only future publishes.
func (c *Client) CreateChannel(ctx context.Context, topic, channel string) (int64, error) {
	id, err := c.store.EnsureChannel(ctx, topic, channel)
	if err != nil {
		return 0, fmt.Errorf("novaque create channel: %w", err)
	}
	return id, nil
}

// DeleteTopic removes the topic and everything under it — channels, messages,
// deliveries, and retained stats rows — in one transaction (R6). Idempotent:
// deleting an already-deleted id succeeds as a no-op. This process's memoized
// ids are evicted; other processes self-heal on their next publish.
func (c *Client) DeleteTopic(ctx context.Context, topicID int64) error {
	if err := c.store.DeleteTopic(ctx, topicID); err != nil {
		return fmt.Errorf("novaque delete topic: %w", err)
	}
	return nil
}

// DeleteChannel removes the channel's deliveries and stats rows, keeping the
// shared messages so sibling channels keep their deliveries (R6). Idempotent
// on already-deleted ids. Consumers of a deleted channel idle until
// restarted — stop and re-Subscribe them.
func (c *Client) DeleteChannel(ctx context.Context, channelID int64) error {
	if err := c.store.DeleteChannel(ctx, channelID); err != nil {
		return fmt.Errorf("novaque delete channel: %w", err)
	}
	return nil
}

// ListDead returns a channel's dead deliveries newest-first, keyset-paginated:
// before > 0 returns only rows with id < before, and limit bounds the page
// (non-positive falls back to a driver default). bodyPrefix > 0 caps each
// Body at that many bytes while BodyLen keeps the full length (review #13 —
// list pages never join full LONGBLOBs); <= 0 reads whole bodies. The
// channel id comes from ListChannels; the read never Ensures (KTD6).
func (c *Client) ListDead(ctx context.Context, channelID, before int64, limit int, bodyPrefix int) ([]DeadDelivery, error) {
	rows, err := c.store.ListDead(ctx, channelID, before, limit, bodyPrefix)
	if err != nil {
		return nil, fmt.Errorf("novaque list dead: %w", err)
	}
	return rows, nil
}

// RequeueDead returns one dead delivery of channelID to pending — attempts
// reset, lease cleared, available now — writing a fresh TTL of the client's
// DefaultTTL on both the delivery and its message (the original per-publish
// TTL is not stored, so the clock restarts). The store guard scopes the
// mutation to that channel (review #10): a delivery dead under a different
// channel matches 0 rows. ErrDeadGone (wrapped) means no dead row of that
// channel matched — already requeued or deleted, or another channel's
// delivery — and a retry is idempotent-safe.
func (c *Client) RequeueDead(ctx context.Context, deliveryID, channelID int64) error {
	if err := c.store.RequeueDead(ctx, deliveryID, channelID, c.opts.DefaultTTL); err != nil {
		return fmt.Errorf("novaque requeue dead: %w", err)
	}
	return nil
}

// DeleteDead removes one dead delivery of channelID; the shared message row
// is reclaimed by the orphan purge once its sibling deliveries are gone. The
// store guard scopes the deletion to that channel (review #10). ErrDeadGone
// (wrapped) when no dead row of that channel matched.
func (c *Client) DeleteDead(ctx context.Context, deliveryID, channelID int64) error {
	if err := c.store.DeleteDead(ctx, deliveryID, channelID); err != nil {
		return fmt.Errorf("novaque delete dead: %w", err)
	}
	return nil
}

// TopicDailyCounters returns the retained day-bucket counter rows for a topic
// rolled up per day over the trailing days-day UTC window ending today.
// Existing-day rows only, ascending by day; zero-filling the window is the
// caller's job. This process's buffered counter deltas are flushed
// best-effort first (KTD12) — other processes' buffers may still lag.
func (c *Client) TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]DailyCounters, error) {
	c.flushStatsBestEffort(ctx)
	rows, err := c.store.TopicDailyCounters(ctx, topicID, days)
	if err != nil {
		return nil, fmt.Errorf("novaque topic daily counters: %w", err)
	}
	return rows, nil
}

// ChannelDailyCounters returns the retained day-bucket counter rows for one
// channel over the trailing days-day UTC window ending today. Flush/lag
// semantics and zero-filling as per TopicDailyCounters.
func (c *Client) ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]DailyCounters, error) {
	c.flushStatsBestEffort(ctx)
	rows, err := c.store.ChannelDailyCounters(ctx, channelID, days)
	if err != nil {
		return nil, fmt.Errorf("novaque channel daily counters: %w", err)
	}
	return rows, nil
}

// flushStatsBestEffort drains this process's buffered counter deltas so the
// daily-counter reads see them (KTD12: the sink is per-process). Failure is
// logged and swallowed — a read must not fail because a flush did.
func (c *Client) flushStatsBestEffort(ctx context.Context) {
	if err := c.store.FlushStats(ctx); err != nil {
		c.logger().Error("stats flush failed", zap.String("op", "flush_stats"), zap.NamedError("err", err))
	}
}

// MaxDelay is the maximum publish Delay (re-export of store.MaxDelay).
const MaxDelay = store.MaxDelay

var (
	// ErrDelayNegative is returned by Publish when Delay is negative
	// (re-export of store.ErrDelayNegative; compare with errors.Is).
	ErrDelayNegative = store.ErrDelayNegative
	// ErrDelayTooLong is returned by Publish when Delay exceeds MaxDelay
	// (re-export of store.ErrDelayTooLong; compare with errors.Is).
	ErrDelayTooLong = store.ErrDelayTooLong
	// ErrDelayExceedsTTL is returned by Publish when Delay would leave no
	// claimable window before the message expires (re-export of
	// store.ErrDelayExceedsTTL; compare with errors.Is).
	ErrDelayExceedsTTL = store.ErrDelayExceedsTTL
)

// PublishOpts are per-message publish options.
type PublishOpts struct {
	// TTL is retention from publish time; zero falls back to DefaultTTL.
	TTL time.Duration
	// Delay is relative time until deliveries become claimable (NSQ
	// DPUB-style); zero = immediate, maximum MaxDelay, and it must leave a
	// claimable window before expiry.
	Delay time.Duration
	// MaxAttempts is the poison threshold for this message's deliveries;
	// zero falls back to DefaultMaxAttempts.
	MaxAttempts int
}

// Publish fans out body to every channel that exists on topic at publish
// time and returns the new message id. Delivery is at-least-once. Fan-out is
// a snapshot: channels created later do not receive the message, and a
// publish to a topic with no channels still stores the message. The topic is
// created on demand. TTL and MaxAttempts default-fill from Options; a Delay
// outside MaxDelay or the effective TTL returns ErrDelayNegative,
// ErrDelayTooLong, or ErrDelayExceedsTTL (compare with errors.Is). When
// another process deleted the topic after its id was resolved, Publish
// evicts the memoized topic and channel ids for the name, re-ensures the
// name (re-creating the topic), and retries once before failing with
// ErrTopicGone.
func (c *Client) Publish(ctx context.Context, topic string, body []byte, opts PublishOpts) (int64, error) {
	topicID, err := c.store.EnsureTopic(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("novaque publish: %w", err)
	}
	po := store.PublishOpts{
		MaxAttempts: opts.MaxAttempts,
		Delay:       opts.Delay,
	}
	if po.MaxAttempts <= 0 {
		po.MaxAttempts = c.opts.DefaultMaxAttempts
	}
	po.TTL = opts.TTL
	if po.TTL <= 0 {
		po.TTL = c.opts.DefaultTTL
	}
	if err := store.ValidatePublishDelay(po, time.Now().Unix()); err != nil {
		return 0, fmt.Errorf("novaque publish: %w", err)
	}
	id, err := c.store.Publish(ctx, topicID, body, po)
	if errors.Is(err, store.ErrTopicGone) {
		// Cross-process delete self-heal (KTD9): another process deleted the
		// topic behind the id after it was resolved here. Evict the memoized
		// id AND every channel id memoized under the name — the foreign
		// cascade delete removed those rows too, so serving them from cache
		// would fan deliveries out to deleted (or later recycled) channel
		// ids — then re-Ensure the name (the Ensure insert re-creates the
		// row — create-on-publish) and retry exactly once. Only Client knows
		// the topic name, so the retry lives here, not in the store; any
		// error on the retry — a second ErrTopicGone included — is returned
		// unchanged.
		if inv, ok := c.store.(interface{ InvalidateTopic(string) }); ok {
			inv.InvalidateTopic(topic)
		}
		topicID, err = c.store.EnsureTopic(ctx, topic)
		if err != nil {
			return 0, fmt.Errorf("novaque publish: %w", err)
		}
		id, err = c.store.Publish(ctx, topicID, body, po)
	}
	if err != nil {
		return 0, fmt.Errorf("novaque publish: %w", err)
	}
	c.logger().Debug("published", zap.String("topic", topic), zap.Int64("message_id", id))
	return id, nil
}

// Handler processes one message. A nil return acks the delivery; a non-nil
// return requeues it immediately. Redelivery is possible (crash before ack,
// lease expiry, dropped finish), so handlers must be idempotent. The ctx is
// a fresh context.Background() bounded by the lease timeout
// (Options.DefaultLease) — not the Start context, so Shutdown does not
// cancel an in-flight handler. After the handler returns, the ack or requeue
// is retried 3 times with 50ms backoff under a 5s cap; if it still fails,
// the failure is logged and the delivery is dropped back to its lease
// expiry, which makes it claimable again.
type Handler func(ctx context.Context, msg *Message) error

// Message is an in-flight delivery handed to a Handler.
type Message struct {
	client     *Client
	deliveryID int64
	leaseToken string

	// MessageID identifies the shared published message.
	MessageID int64
	// Topic and Channel are the delivery's destination names.
	Topic   string
	Channel string
	// Body is the message payload.
	Body []byte
	// Attempts is the number of claims so far, counting the claim that
	// produced this delivery.
	Attempts int
}

// Consumer polls a topic/channel and dispatches to Handler.
type Consumer struct {
	client    *Client
	topic     string
	channel   string
	channelID int64
	handler   Handler
	owner     string

	mu      sync.Mutex
	started bool
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

// Subscribe ensures topic/channel and returns a Consumer (not yet started).
// Call Start to open a batch claimer + MaxInFlight handlers (Solid Queue–style).
func (c *Client) Subscribe(topic, channel string, handler Handler) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("novaque: handler is nil")
	}
	id, err := c.store.EnsureChannel(context.Background(), topic, channel)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		client:    c,
		topic:     topic,
		channel:   channel,
		channelID: id,
		handler:   handler,
		owner:     newOwnerID(),
	}, nil
}

// SubscribeAndStart is Subscribe followed by Start.
func (c *Client) SubscribeAndStart(ctx context.Context, topic, channel string, handler Handler) (*Consumer, error) {
	co, err := c.Subscribe(topic, channel, handler)
	if err != nil {
		return nil, err
	}
	if err := co.Start(ctx); err != nil {
		return nil, err
	}
	return co, nil
}

// Start launches one batch poller and MaxInFlight handler workers. A nil
// ctx defaults to context.Background(); a duplicate Start is a no-op.
// Cancelling ctx (or Shutdown) stops claiming new work but already-claimed
// deliveries keep draining so their leases get acked or requeued.
func (co *Consumer) Start(ctx context.Context) error {
	co.mu.Lock()
	if co.started {
		co.mu.Unlock()
		return nil
	}
	base := ctx
	if base == nil {
		base = context.Background()
	}
	runCtx, cancel := context.WithCancel(base)
	co.stop = cancel
	co.started = true

	n := co.client.opts.MaxInFlight
	if n <= 0 {
		n = 1
	}
	work := make(chan store.Delivery, n)
	co.wg.Add(n + 1)
	for i := 0; i < n; i++ {
		go co.handleLoop(runCtx, work)
	}
	go co.pollLoop(runCtx, work, n)
	co.mu.Unlock()

	// Log after the transition and outside the mutex: duplicate Start stays silent.
	co.logger().Info("consumer started")
	return nil
}

// logger returns a child Logger tagged with this Consumer's topic/channel.
func (co *Consumer) logger() Logger {
	return co.client.logger().With(
		zap.String("topic", co.topic),
		zap.String("channel", co.channel))
}

// Shutdown stops the poller and waits for in-flight handlers. ctx is a
// non-nil wait deadline: if it expires before the workers finish, Shutdown
// returns ctx.Err() while the workers keep draining in the background.
// Stop Consumers before the Client so their final acks land before the
// Client's last counter flush. Shutdown of a never-started Consumer is a
// no-op.
func (co *Consumer) Shutdown(ctx context.Context) error {
	co.mu.Lock()
	if !co.started {
		co.mu.Unlock()
		return nil
	}
	stop := co.stop
	co.started = false
	co.stop = nil
	co.mu.Unlock()
	stop()

	done := make(chan struct{})
	go func() {
		co.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		co.logger().Info("consumer shutdown")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (co *Consumer) pollLoop(ctx context.Context, work chan<- store.Delivery, maxInFlight int) {
	defer co.wg.Done()
	defer close(work)

	base := co.client.opts.PollInterval
	owner := co.owner + "#poller"
	lg := co.logger()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		free := maxInFlight - len(work)
		if free <= 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter(base / 4)):
			}
			continue
		}

		claimed, err := co.client.store.Claim(ctx, co.channelID, owner, co.client.opts.DefaultLease, free)
		if err != nil {
			// Swallowed failure: skip Error when caused by shutdown cancel (noise).
			if ctx.Err() == nil {
				lg.Error("claim failed", zap.String("op", "claim"), zap.NamedError("err", err))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter(base)):
			}
			continue
		}
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter(base)):
			}
			continue
		}
		lg.Debug("claimed", zap.Int("count", len(claimed)))
		for _, d := range claimed {
			select {
			case <-ctx.Done():
				return
			case work <- d:
			}
		}
	}
}

func (co *Consumer) handleLoop(ctx context.Context, work <-chan store.Delivery) {
	defer co.wg.Done()
	for d := range work {
		// Drain claimed work even after cancel so leases get ack/requeue.
		_ = ctx
		co.dispatch(d)
	}
}

func (co *Consumer) dispatch(d store.Delivery) {
	msg := &Message{
		client:     co.client,
		deliveryID: d.ID,
		leaseToken: d.LeaseToken,
		MessageID:  d.MessageID,
		Topic:      d.Topic,
		Channel:    d.Channel,
		Body:       d.Body,
		Attempts:   d.Attempts,
	}
	// Handler deadline is lease-bound; do not tie to poller cancel so Shutdown can drain cleanly.
	hctx, cancel := context.WithTimeout(context.Background(), co.client.opts.DefaultLease)
	err := co.handler(hctx, msg)
	cancel()

	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ackCancel()
	if err == nil {
		if ferr := finishWithRetry(ackCtx, func(ctx context.Context) error {
			return co.client.store.Ack(ctx, msg.deliveryID, msg.leaseToken)
		}); ferr != nil {
			// Swallowed failure after retries (or ackCtx deadline).
			logFinishError(co.client.logger(), "ack failed", "ack", msg.deliveryID, ferr)
		}
		return
	}
	if ferr := finishWithRetry(ackCtx, func(ctx context.Context) error {
		return co.client.store.Requeue(ctx, msg.deliveryID, msg.leaseToken, time.Time{})
	}); ferr != nil {
		// Swallowed failure after retries (or ackCtx deadline). The handler's own
		// error is intentionally not logged; only the Requeue failure is.
		logFinishError(co.client.logger(), "requeue failed", "requeue", msg.deliveryID, ferr)
	}
}

// logFinishError reports a swallowed ack/requeue failure after retries.
func logFinishError(lg Logger, msg, op string, deliveryID int64, err error) {
	lg.Error(msg,
		zap.String("op", op),
		zap.Int64("delivery_id", deliveryID),
		zap.NamedError("err", err))
}

func finishWithRetry(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for i := 0; i < 3; i++ {
		err = fn(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return err
}

func newOwnerID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func jitter(base time.Duration) time.Duration {
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	// ±50% jitter to desynchronize multi-process idle polls.
	var b [1]byte
	_, _ = rand.Read(b[:])
	frac := 0.5 + float64(b[0])/255.0 // 0.5 .. 1.5
	return time.Duration(float64(base) * frac)
}
