package novaque

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"novaque/store"
)

// Options configure Client defaults.
type Options struct {
	DefaultTTL         time.Duration
	DefaultLease       time.Duration
	DefaultMaxAttempts int
	PollInterval       time.Duration
	MaxInFlight        int
	ReapInterval       time.Duration
	PurgeInterval      time.Duration
	MaintenanceBatch   int
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
func Open(s store.Store, opts Options) (*Client, error) {
	if s == nil {
		return nil, errors.New("novaque: store is nil")
	}
	return &Client{store: s, opts: opts.withDefaults()}, nil
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

// PublishOpts are per-message publish options.
type PublishOpts struct {
	TTL         time.Duration
	MaxAttempts int
}

// Publish fans out body to all existing channels on topic.
func (c *Client) Publish(ctx context.Context, topic string, body []byte, opts PublishOpts) (int64, error) {
	po := store.PublishOpts{MaxAttempts: opts.MaxAttempts}
	if po.MaxAttempts <= 0 {
		po.MaxAttempts = c.opts.DefaultMaxAttempts
	}
	po.TTL = opts.TTL
	if po.TTL <= 0 {
		po.TTL = c.opts.DefaultTTL
	}
	id, err := c.store.Publish(ctx, topic, body, po)
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
	client  *Client
	topic   string
	channel string
	handler Handler

	owner string

	mu      sync.Mutex
	started bool
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

// Subscribe ensures topic/channel and returns a Consumer (not yet started).
func (c *Client) Subscribe(topic, channel string, handler Handler) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("novaque: handler is nil")
	}
	if _, err := c.store.EnsureChannel(context.Background(), topic, channel); err != nil {
		return nil, err
	}
	return &Consumer{
		client:  c,
		topic:   topic,
		channel: channel,
		handler: handler,
		owner:   fmt.Sprintf("%p", c),
	}, nil
}

// Start begins polling.
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
	co.wg.Add(1)
	go co.loop(runCtx)
	return nil
}

// Shutdown stops polling and waits for in-flight handler return.
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

func (co *Consumer) loop(ctx context.Context) {
	defer co.wg.Done()
	backoff := co.client.opts.PollInterval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Claim one at a time so MaxInFlight>1 does not hoard leases during serial handling.
		claimed, err := co.client.store.Claim(ctx, co.topic, co.channel, co.owner, co.client.opts.DefaultLease, 1)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}

		d := claimed[0]
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
		hctx, cancel := context.WithTimeout(ctx, co.client.opts.DefaultLease)
		err = co.handler(hctx, msg)
		cancel()

		ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err == nil {
			_ = finishWithRetry(ackCtx, func(ctx context.Context) error {
				return co.client.store.Ack(ctx, msg.deliveryID, msg.leaseToken)
			})
		} else {
			_ = finishWithRetry(ackCtx, func(ctx context.Context) error {
				return co.client.store.Requeue(ctx, msg.deliveryID, msg.leaseToken, time.Time{})
			})
		}
		ackCancel()
	}
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
