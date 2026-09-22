//go:build integration

package novaque_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/usual2970/novaque"
	mysqldriver "github.com/usual2970/novaque/driver/mysql"
	"github.com/usual2970/novaque/internal/testmysql"
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

// TestClientStatsEndToEnd covers U3 through the public Client API: buffered
// counters become visible after an explicit FlushStats and after a Start tick
// (StatsFlushInterval wiring), Client reads equal direct store reads, backlog
// tracks the pending/ready/in_flight transitions including delayed publishes,
// and PruneStats removes only expired day buckets.
func TestClientStatsEndToEnd(t *testing.T) {
	db := testmysql.Open(t)
	st := mysqldriver.New(db)
	client, err := novaque.Open(st, novaque.Options{
		DefaultLease:       10 * time.Second,
		PollInterval:       20 * time.Millisecond,
		StatsFlushInterval: 50 * time.Millisecond, // loop drains counters without explicit flush
		StatsPruneInterval: time.Hour,
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

	topic := "clstats_" + time.Now().Format("150405.000")

	// Zero-channel publishes count on the topic-level row only, and the
	// explicit FlushStats makes them immediately visible via the public API.
	for i := 0; i < 2; i++ {
		if _, err := client.Publish(ctx, topic, []byte("early"), novaque.PublishOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	tc, err := client.TopicCounters(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if tc.Publish != 2 {
		t.Fatalf("topic counters after zero-channel publishes = %+v, want publish=2", tc)
	}

	// A blocking handler holds deliveries so the backlog transition is
	// observable through the public API.
	release := make(chan struct{})
	handled := make(chan struct{}, 2)
	cons, err := client.Subscribe(topic, "workers", func(_ context.Context, _ *novaque.Message) error {
		<-release
		handled <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cons.Start(ctx); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := client.Publish(ctx, topic, []byte("work"), novaque.PublishOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	// MaxInFlight=1: the poller still claims both (one in handler, one parked
	// in the work buffer), so the channel settles at in_flight=2, pending=0.
	waitFor(t, 5*time.Second, "in_flight backlog transition", func() bool {
		b, err := client.ChannelBacklog(ctx, topic, "workers")
		return err == nil && b.InFlight == 2 && b.Pending == 0 && b.Ready == 0 && b.Dead == 0
	})
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case <-handled:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for handled deliveries")
		}
	}

	// Counters show up through the public API after a Start tick alone.
	waitFor(t, 5*time.Second, "flushed channel counters", func() bool {
		cc, err := client.ChannelCounters(ctx, topic, "workers")
		return err == nil && cc == (novaque.ChannelCounters{Publish: 2, Claim: 2, Ack: 2})
	})
	tc, err = client.TopicCounters(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if tc.Publish != 4 { // 2 zero-channel + 2 fan-out
		t.Fatalf("topic counters = %+v, want publish=4", tc)
	}

	// Client-channel read equals the direct store read for the same ids.
	chID, err := st.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	directCC, err := st.ChannelCounters(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	clientCC, err := client.ChannelCounters(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	if directCC != clientCC {
		t.Fatalf("client counters %+v != direct store %+v", clientCC, directCC)
	}
	directBL, err := st.ChannelBacklog(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	clientBL, err := client.ChannelBacklog(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	if directBL != clientBL {
		t.Fatalf("client backlog %+v != direct store %+v", clientBL, directBL)
	}

	// Acks drain the backlog to zero.
	waitFor(t, 5*time.Second, "drained backlog", func() bool {
		b, err := client.ChannelBacklog(ctx, topic, "workers")
		return err == nil && b == (novaque.ChannelBacklog{})
	})

	// Delayed publish: pending but not ready until available_at passes.
	if err := cons.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Publish(ctx, topic, []byte("later"), novaque.PublishOpts{Delay: time.Second}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "delayed pending-not-ready", func() bool {
		b, err := client.ChannelBacklog(ctx, topic, "workers")
		return err == nil && b.Pending == 1 && b.Ready == 0
	})
	waitFor(t, 5*time.Second, "delayed turns ready", func() bool {
		b, err := client.ChannelBacklog(ctx, topic, "workers")
		return err == nil && b.Pending == 1 && b.Ready == 1
	})

	// Explicit PruneStats through the Client removes only expired day buckets.
	topicID, err := st.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO novaque_stats_daily (day_utc, topic_id, channel_id, publish)
		VALUES (UTC_DATE() - INTERVAL 40 DAY, ?, 0, 9)`, topicID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PruneStats(ctx); err != nil {
		t.Fatal(err)
	}
	var oldRows, todayRows int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM novaque_stats_daily
		WHERE topic_id = ? AND day_utc < UTC_DATE()`, topicID).Scan(&oldRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM novaque_stats_daily
		WHERE topic_id = ? AND day_utc = UTC_DATE()`, topicID).Scan(&todayRows); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 || todayRows < 1 {
		t.Fatalf("after prune: expired rows=%d today rows=%d, want 0/>=1", oldRows, todayRows)
	}
}

// TestClientPublishSelfHealAfterForeignTopicDelete covers AE9 (R11): two
// Clients share one DB (separate CachingStores); B memoizes a topic id via a
// publish, A deletes the topic, and B's next publish self-heals — evict the
// memo, re-Ensure the name, retry once — landing on a re-created topic row
// with a new id.
func TestClientPublishSelfHealAfterForeignTopicDelete(t *testing.T) {
	db := testmysql.Open(t)
	mk := func() *novaque.Client {
		client, err := novaque.Open(mysqldriver.New(db), novaque.Options{
			PurgeInterval: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	procA, procB := mk(), mk()
	ctx := context.Background()
	if err := procA.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	topic := "heal_" + time.Now().Format("150405.000")

	// B memoizes the topic id in its own CachingStore via a first publish.
	if _, err := procB.Publish(ctx, topic, []byte("one"), novaque.PublishOpts{}); err != nil {
		t.Fatal(err)
	}

	// A resolves the id the way the admin surface does — from the list — and
	// deletes the topic (a foreign delete as far as B's memo is concerned).
	topics, err := procA.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var oldID int64
	for _, ti := range topics {
		if ti.Name == topic {
			oldID = ti.ID
		}
	}
	if oldID == 0 {
		t.Fatalf("topic %q not listed", topic)
	}
	if err := procA.DeleteTopic(ctx, oldID); err != nil {
		t.Fatal(err)
	}

	// B publishes again: the memoized id is stale, the driver maps the topic
	// FK failure to ErrTopicGone, and the client retry re-creates the row.
	msgID, err := procB.Publish(ctx, topic, []byte("two"), novaque.PublishOpts{})
	if err != nil {
		t.Fatalf("publish after foreign delete must self-heal, got %v", err)
	}

	// The retried publish landed on a re-created row with a new id.
	topics, err = procA.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var newID int64
	for _, ti := range topics {
		if ti.Name == topic {
			newID = ti.ID
		}
	}
	if newID == 0 || newID == oldID {
		t.Fatalf("topic re-created with id %d, want a new id (was %d)", newID, oldID)
	}
	var gotTopicID int64
	if err := db.QueryRowContext(ctx, `SELECT topic_id FROM novaque_messages WHERE id = ?`, msgID).Scan(&gotTopicID); err != nil {
		t.Fatal(err)
	}
	if gotTopicID != newID {
		t.Fatalf("message %d lives on topic %d, want re-created topic %d", msgID, gotTopicID, newID)
	}
}
