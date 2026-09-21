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

// Options configure Client defaults.
type Options struct {
	DefaultTTL         time.Duration
	DefaultLease       time.Duration
	DefaultMaxAttempts int
	PollInterval       time.Duration
	// MaxInFlight is concurrent claim+handle workers per Consumer (each claims 1).
	// Raise this for in-process concurrency; add more OS processes for multi-node scale.
	MaxInFlight      int
	ReapInterval     time.Duration
	PurgeInterval    time.Duration
	MaintenanceBatch int
	// StatsRetentionDays keeps day-bucket counter rows for this many UTC days
	// before the prune tick (or an explicit PruneStats call) deletes them.
	StatsRetentionDays int
	// StatsFlushInterval is how often the maintenance loop drains the driver's
	// buffered counter deltas into the stats table.
	StatsFlushInterval time.Duration
	// StatsPruneInterval is how often the maintenance loop prunes day-bucket
	// counter rows older than StatsRetentionDays. Dedicated field (not a
	// PurgeInterval co-tick) so prune stays gentle and unit-testable.
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
// (exactly once per Client).
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

// Shutdown stops maintenance loops and waits for them to exit.
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
// on. Eventual-consistency window as per ChannelCounters.
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
// excluded until available_at passes), in_flight, and dead. Backlog is a live
// row count, not a day bucket, so it needs no flush.
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

// MaxDelay is the maximum publish Delay (re-export of store.MaxDelay).
const MaxDelay = store.MaxDelay

// Sentinel errors for publish Delay validation (re-exported from store).
var (
	ErrDelayNegative   = store.ErrDelayNegative
	ErrDelayTooLong    = store.ErrDelayTooLong
	ErrDelayExceedsTTL = store.ErrDelayExceedsTTL
)

// PublishOpts are per-message publish options.
type PublishOpts struct {
	TTL         time.Duration
	Delay       time.Duration // relative; 0 = immediate; max MaxDelay
	MaxAttempts int
}

// Publish fans out body to all existing channels on topic.
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
	if err != nil {
		return 0, fmt.Errorf("novaque publish: %w", err)
	}
	c.logger().Debug("published", zap.String("topic", topic), zap.Int64("message_id", id))
	return id, nil
}

// Handler processes one message. nil error acks; non-nil requeues.
type Handler func(ctx context.Context, msg *Message) error

// Message is an in-flight delivery handed to a Handler.
type Message struct {
	client     *Client
	deliveryID int64
	leaseToken string

	MessageID int64
	Topic     string
	Channel   string
	Body      []byte
	Attempts  int
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

// Start launches one batch poller and MaxInFlight handler workers.
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

// Shutdown stops the poller and waits for in-flight handlers.
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
