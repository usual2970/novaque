package novaque

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

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

// Start begins shared reaper and TTL maintenance loops (exactly once per Client).
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	base := ctx
	if base == nil {
		base = context.Background()
	}
	runCtx, cancel := context.WithCancel(base)
	c.stop = cancel
	c.started = true

	c.wg.Add(2)
	go c.loopReap(runCtx)
	go c.loopPurge(runCtx)
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
			_, _ = c.store.ReapExpiredLeases(ctx, c.opts.MaintenanceBatch)
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
			_, _ = c.store.PurgeExpired(ctx, c.opts.MaintenanceBatch)
		}
	}
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
	defer co.mu.Unlock()
	if co.started {
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
	return nil
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
		_ = finishWithRetry(ackCtx, func(ctx context.Context) error {
			return co.client.store.Ack(ctx, msg.deliveryID, msg.leaseToken)
		})
		return
	}
	_ = finishWithRetry(ackCtx, func(ctx context.Context) error {
		return co.client.store.Requeue(ctx, msg.deliveryID, msg.leaseToken, time.Time{})
	})
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
