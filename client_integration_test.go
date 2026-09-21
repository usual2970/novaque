//go:build integration

package novaque_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"novaque"
	mysqldriver "novaque/driver/mysql"
	"novaque/internal/testmysql"
)

func TestClientConsumerEndToEnd(t *testing.T) {
	db := testmysql.Open(t)
	store := mysqldriver.New(db)
	client, err := novaque.Open(store, novaque.Options{
		DefaultLease:  5 * time.Second,
		PollInterval:  50 * time.Millisecond,
		ReapInterval:  100 * time.Millisecond,
		PurgeInterval: time.Hour,
		MaxInFlight:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	topic := "e2e_" + time.Now().Format("150405.000")
	var got []string
	var mu sync.Mutex
	done := make(chan struct{}, 2)

	makeConsumer := func(ch string) *novaque.Consumer {
		c, err := client.Subscribe(topic, ch, func(_ context.Context, msg *novaque.Message) error {
			mu.Lock()
			got = append(got, ch+":"+string(msg.Body))
			mu.Unlock()
			done <- struct{}{}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return c
	}
	a := makeConsumer("A")
	b := makeConsumer("B")
	defer a.Shutdown(context.Background())
	defer b.Shutdown(context.Background())

	if _, err := client.Publish(ctx, topic, []byte("ping"), novaque.PublishOpts{}); err != nil {
		t.Fatal(err)
	}

	timeout := time.After(10 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatalf("timeout waiting for fan-out; got=%v", got)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("want 2 deliveries, got %v", got)
	}
}

func TestHighConcurrencyCompete(t *testing.T) {
	db := testmysql.Open(t)
	store := mysqldriver.New(db)
	client, err := novaque.Open(store, novaque.Options{
		DefaultLease:  10 * time.Second,
		PollInterval:  20 * time.Millisecond,
		MaxInFlight:   8,
		ReapInterval:  time.Hour,
		PurgeInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "conc_" + time.Now().Format("150405.000")
	const n = 40
	seen := make(map[int64]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(n)

	cons, err := client.SubscribeAndStart(ctx, topic, "workers", func(_ context.Context, msg *novaque.Message) error {
		mu.Lock()
		seen[msg.MessageID]++
		mu.Unlock()
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Shutdown(context.Background())

	for i := 0; i < n; i++ {
		if _, err := client.Publish(ctx, topic, []byte{byte(i)}, novaque.PublishOpts{}); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("timeout; got %d unique of %d", len(seen), n)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("want %d unique messages, got %d", n, len(seen))
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("message %d delivered %d times", id, c)
		}
	}
}
