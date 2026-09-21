package novaque_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/usual2970/novaque"

	"github.com/usual2970/novaque/store"
)

// fakeStore is an in-memory Store for domain unit tests (no MySQL import).
type fakeStore struct {
	mu       sync.Mutex
	topics   map[string]int64
	channels map[string]map[string]int64 // topic -> channel -> id
	messages []fakeMsg
	nextID   int64

	failPublish bool
	lastOpts    store.PublishOpts
	publishN    int
}

type fakeMsg struct {
	id       int64
	topic    string
	body     []byte
	channels []string // channel names present at publish
}

func newFake() *fakeStore {
	return &fakeStore{
		topics:   map[string]int64{},
		channels: map[string]map[string]int64{},
		nextID:   1,
	}
}

func (f *fakeStore) Migrate(context.Context) error { return nil }

func (f *fakeStore) EnsureTopic(_ context.Context, name string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.topics[name]; ok {
		return id, nil
	}
	id := f.nextID
	f.nextID++
	f.topics[name] = id
	f.channels[name] = map[string]int64{}
	return id, nil
}

func (f *fakeStore) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	if _, err := f.EnsureTopic(ctx, topic); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.channels[topic][channel]; ok {
		return id, nil
	}
	id := f.nextID
	f.nextID++
	f.channels[topic][channel] = id
	return id, nil
}

func (f *fakeStore) Publish(_ context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	if f.failPublish {
		return 0, errors.New("boom")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastOpts = opts
	f.publishN++
	var topic string
	for name, id := range f.topics {
		if id == topicID {
			topic = name
			break
		}
	}
	var chans []string
	for name := range f.channels[topic] {
		chans = append(chans, name)
	}
	id := f.nextID
	f.nextID++
	f.messages = append(f.messages, fakeMsg{id: id, topic: topic, body: append([]byte(nil), body...), channels: chans})
	return id, nil
}

func (f *fakeStore) Claim(context.Context, int64, string, time.Duration, int) ([]store.Delivery, error) {
	return nil, nil
}
func (f *fakeStore) Ack(context.Context, int64, string) error { return nil }
func (f *fakeStore) Requeue(context.Context, int64, string, time.Time) error {
	return nil
}
func (f *fakeStore) ReapExpiredLeases(context.Context, int) (int64, error) { return 0, nil }
func (f *fakeStore) PurgeExpired(context.Context, int) (int64, error)      { return 0, nil }

func TestOpenRejectsNilStore(t *testing.T) {
	if _, err := novaque.Open(nil, novaque.Options{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishUsesStoreWithoutMySQL(t *testing.T) {
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.Subscribe("t", "c", func(context.Context, *novaque.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	id, err := c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected id")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.messages) != 1 || len(f.messages[0].channels) != 1 {
		t.Fatalf("unexpected fanout %#v", f.messages)
	}
}

func TestPublishErrorSurfaced(t *testing.T) {
	f := newFake()
	f.failPublish = true
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(context.Background(), "t", []byte("x"), novaque.PublishOpts{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishDelayValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("forwards delay", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: time.Hour,
			TTL:   2 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.publishN != 1 || f.lastOpts.Delay != time.Hour || f.lastOpts.TTL != 2*time.Hour {
			t.Fatalf("opts %#v n=%d", f.lastOpts, f.publishN)
		}
	})

	t.Run("zero delay", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{}); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.lastOpts.Delay != 0 {
			t.Fatalf("delay %v", f.lastOpts.Delay)
		}
	})

	t.Run("over max", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: novaque.MaxDelay + time.Second,
			TTL:   61 * 24 * time.Hour,
		})
		if !errors.Is(err, novaque.ErrDelayTooLong) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})

	t.Run("negative", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{Delay: -time.Second, TTL: time.Hour})
		if !errors.Is(err, novaque.ErrDelayNegative) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})

	t.Run("default ttl vs long delay", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{}) // DefaultTTL 7d
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{Delay: 8 * 24 * time.Hour})
		if !errors.Is(err, novaque.ErrDelayExceedsTTL) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})

	t.Run("max delay accepted", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: novaque.MaxDelay,
			TTL:   61 * 24 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("same second collapse", func(t *testing.T) {
		f := newFake()
		c, err := novaque.Open(f, novaque.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Publish(ctx, "t", []byte("x"), novaque.PublishOpts{
			Delay: 2 * time.Second,
			TTL:   2500 * time.Millisecond,
		})
		if !errors.Is(err, novaque.ErrDelayExceedsTTL) {
			t.Fatalf("got %v", err)
		}
		if f.publishN != 0 {
			t.Fatal("store should not be called")
		}
	})
}

var _ store.Store = (*fakeStore)(nil)
