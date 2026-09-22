//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usual2970/novaque/driver/postgres"
	"github.com/usual2970/novaque/internal/testpostgres"
	"github.com/usual2970/novaque/store"
)

// TestStatsMultiWriterTwoStores covers multi-writer correctness: two
// independent *sql.DB handles (two postgres.Store instances over the same
// shared PostgreSQL) concurrently publish to the SAME channel and
// concurrently claim from it. Batched INSERT ... ON CONFLICT flushes from the
// two connections must not lose updates, and after FlushStats on BOTH stores
// (each keeps its own in-process sink) the channel counters must equal the
// total successful mutations exactly: publish = successful publishes, claim =
// rows actually leased (sum of Claim result lengths, not attempts), ack =
// rows acked.
func TestStatsMultiWriterTwoStores(t *testing.T) {
	db1 := testpostgres.Open(t)
	db2 := testpostgres.Open(t)
	s1 := postgres.New(db1)
	s2 := postgres.New(db2)
	ctx := context.Background()
	if err := s1.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Same schema through the second connection; Migrate is idempotent.
	if err := s2.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statmulti_" + time.Now().Format("150405.000")
	chID, err := s1.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s1.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	const perSide = 40
	var pubOK, claimed, acked atomic.Int64
	var pubDone, claimDone sync.WaitGroup
	pubDone.Add(2)
	claimDone.Add(2)

	// One goroutine per store: publish, then claim until the channel drains
	// (both barriers keep the counts deterministic), then ack every lease this
	// side took. A mutation error degrades the wanted totals but never skips
	// them — the assertions compare against what actually succeeded.
	side := func(s *postgres.Store, owner string) {
		for i := 0; i < perSide; i++ {
			if _, err := s.Publish(ctx, topicID, []byte(owner), store.PublishOpts{TTL: time.Hour}); err != nil {
				t.Errorf("publish via %s: %v", owner, err)
				continue
			}
			pubOK.Add(1)
		}
		pubDone.Done()
		pubDone.Wait() // both sides settled before any claiming starts

		var mine []store.Delivery
		deadline := time.Now().Add(30 * time.Second)
		for claimed.Load() < pubOK.Load() && time.Now().Before(deadline) {
			got, err := s.Claim(ctx, chID, owner, 30*time.Second, 5)
			if err != nil {
				t.Errorf("claim via %s: %v", owner, err)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if len(got) == 0 {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			claimed.Add(int64(len(got)))
			mine = append(mine, got...)
		}
		claimDone.Done()
		claimDone.Wait() // no more claiming while acking runs

		for _, d := range mine {
			if err := s.Ack(ctx, d.ID, d.LeaseToken); err != nil {
				t.Errorf("ack via %s: %v", owner, err)
				continue
			}
			acked.Add(1)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); side(s1, "w1") }()
	go func() { defer wg.Done(); side(s2, "w2") }()
	wg.Wait()

	// Each store buffers in its own sink: drain both before reading counters.
	if err := s1.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s2.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}

	if got := pubOK.Load(); got != 2*perSide {
		t.Errorf("successful publishes = %d, want %d", got, 2*perSide)
	}
	if got := claimed.Load(); got != pubOK.Load() {
		t.Errorf("rows leased across both stores = %d, want %d (each successful publish claimed exactly once)", got, pubOK.Load())
	}
	if got := acked.Load(); got != claimed.Load() {
		t.Errorf("rows acked = %d, want %d", got, claimed.Load())
	}
	want := store.ChannelCounters{Publish: pubOK.Load(), Claim: claimed.Load(), Ack: acked.Load()}
	if c, err := s1.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("channel counters after multi-writer run = %+v err=%v, want %+v", c, err, want)
	}
	// Single-channel topic: the rollup (one channel row, no sentinel) equals it.
	if tc, err := s1.TopicCounters(ctx, topicID); err != nil || tc != want {
		t.Fatalf("topic counters = %+v err=%v, want %+v", tc, err, want)
	}
	if b, err := s1.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("backlog after acking everything = %+v err=%v, want zeros", b, err)
	}
}

// TestStatsSingleStoreConcurrentPublish covers single-store concurrency: 8
// goroutines publish through ONE postgres.Store while a background goroutine
// keeps flushing the sink mid-stream. recordStat's mutex plus the
// swap-under-lock drain must keep every delta exactly once — the publish
// counter equals the count of successful Publish calls. Run with -race to
// prove the sink is data-race free under this load.
func TestStatsSingleStoreConcurrentPublish(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statconc_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const perWorker = 15
	var pubOK atomic.Int64
	var wg sync.WaitGroup
	stopFlush := make(chan struct{})
	var flushWG sync.WaitGroup
	flushWG.Add(1)
	go func() {
		defer flushWG.Done()
		for {
			select {
			case <-stopFlush:
				return
			default:
			}
			// A failed flush re-merges its deltas (at-least-once), so errors
			// are loud but the totals below stay exact either way.
			if err := s.FlushStats(ctx); err != nil {
				t.Errorf("mid-stream flush: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if _, err := s.Publish(ctx, topicID, []byte("m"), store.PublishOpts{TTL: time.Hour}); err != nil {
					t.Errorf("publish: %v", err)
					continue
				}
				pubOK.Add(1)
			}
		}()
	}
	wg.Wait()
	close(stopFlush)
	flushWG.Wait()

	// Final drain lands whatever the racing flusher did not pick up.
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	if got := pubOK.Load(); got != workers*perWorker {
		t.Errorf("successful publishes = %d, want %d", got, workers*perWorker)
	}
	want := store.ChannelCounters{Publish: pubOK.Load()}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after concurrent publish = %+v err=%v, want %+v", c, err, want)
	}
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc != want {
		t.Fatalf("topic counters = %+v err=%v, want %+v", tc, err, want)
	}
}

// TestStatsChannelIsolation covers channel isolation: on one topic,
// claim/ack/purge on channel A never touch channel B's channel-scoped
// counters or its backlog (and vice versa). Fan-out publishes DO bump both
// channels' publish counters — one delivery per channel is the unit — so
// isolation is asserted on the claim/ack/requeue/dead/purge columns and on
// backlog. A zero-channel publish (before any channel exists) leaves both
// real channels at zero while the topic rollup still sees it on the sentinel.
func TestStatsChannelIsolation(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statiso_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Zero-channel publish first: it must land on the channel_id=0 sentinel
	// only, so channels created afterwards start at zero everything.
	if _, err := s.Publish(ctx, topicID, []byte("ghost"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	aID, err := s.EnsureChannel(ctx, topic, "A")
	if err != nil {
		t.Fatal(err)
	}
	bID, err := s.EnsureChannel(ctx, topic, "B")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{aID, bID} {
		if c, err := s.ChannelCounters(ctx, id); err != nil || c != (store.ChannelCounters{}) {
			t.Fatalf("fresh channel counters = %+v err=%v, want zeros (zero-channel publish skipped real channels)", c, err)
		}
		if b, err := s.ChannelBacklog(ctx, id); err != nil || b != (store.ChannelBacklog{}) {
			t.Fatalf("fresh channel backlog = %+v err=%v, want zeros", b, err)
		}
	}
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc != (store.ChannelCounters{Publish: 1}) {
		t.Fatalf("topic counters after ghost publish = %+v err=%v, want publish=1 (sentinel row)", tc, err)
	}

	// Claim/ack on A only: both channels gain publish=2 from the fan-out, but
	// every channel-scoped counter and the whole backlog of B stay untouched.
	for i := 0; i < 2; i++ {
		if _, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Claim(ctx, aID, "a-worker", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("claim on A got %d deliveries, want 2", len(got))
	}
	for _, d := range got {
		if err := s.Ack(ctx, d.ID, d.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	if c, err := s.ChannelCounters(ctx, aID); err != nil || c != (store.ChannelCounters{Publish: 2, Claim: 2, Ack: 2}) {
		t.Fatalf("channel A counters = %+v err=%v, want publish=2 claim=2 ack=2", c, err)
	}
	if c, err := s.ChannelCounters(ctx, bID); err != nil || c != (store.ChannelCounters{Publish: 2}) {
		t.Fatalf("channel B counters after A's claim/ack = %+v err=%v, want publish=2 only", c, err)
	}
	if b, err := s.ChannelBacklog(ctx, bID); err != nil || b != (store.ChannelBacklog{Pending: 2, Ready: 2}) {
		t.Fatalf("channel B backlog after A's claim/ack = %+v err=%v, want pending=2 ready=2 (untouched)", b, err)
	}
	if b, err := s.ChannelBacklog(ctx, aID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("channel A backlog after acks = %+v err=%v, want zeros", b, err)
	}

	// Purge isolation: publish one short-TTL message; B's copy is claimed and
	// held on a valid lease (in_flight rows with live leases are never purged)
	// while A's copy expires. PurgeExpired is global, but its counter must
	// attribute only A's row to A — B keeps purge=0 and its leased delivery.
	if _, err := s.Publish(ctx, topicID, []byte("short"), store.PublishOpts{TTL: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	bGot, err := s.Claim(ctx, bID, "b-worker", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(bGot) != 3 {
		t.Fatalf("claim on B got %d deliveries, want 3 (two jobs + the short one)", len(bGot))
	}
	var shortLease store.Delivery
	for _, d := range bGot {
		if string(d.Body) == "short" {
			shortLease = d
			continue
		}
		if err := s.Ack(ctx, d.ID, d.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	if shortLease.ID == 0 {
		t.Fatal("claim on B did not return the short delivery")
	}
	// 3.6s for a 2s TTL (same second-flooring margin as the other timing
	// tests): A's short delivery is strictly past expires_at; B's leased copy
	// is inside its 30s lease and must survive.
	time.Sleep(3600 * time.Millisecond)
	if _, err := s.PurgeExpired(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	if c, err := s.ChannelCounters(ctx, aID); err != nil || c != (store.ChannelCounters{Publish: 3, Claim: 2, Ack: 2, Purge: 1}) {
		t.Fatalf("channel A counters after purge = %+v err=%v, want publish=3 claim=2 ack=2 purge=1", c, err)
	}
	if c, err := s.ChannelCounters(ctx, bID); err != nil || c != (store.ChannelCounters{Publish: 3, Claim: 3, Ack: 2}) {
		t.Fatalf("channel B counters after purge = %+v err=%v, want publish=3 claim=3 ack=2 and no purge", c, err)
	}
	if b, err := s.ChannelBacklog(ctx, bID); err != nil || b != (store.ChannelBacklog{InFlight: 1}) {
		t.Fatalf("channel B backlog after purge = %+v err=%v, want in_flight=1 (leased copy survived)", b, err)
	}
	if b, err := s.ChannelBacklog(ctx, aID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("channel A backlog after purge = %+v err=%v, want zeros", b, err)
	}

	// Topic rollup = channel A row + channel B row + the zero-channel sentinel.
	wantTopic := store.ChannelCounters{Publish: 7, Claim: 5, Ack: 4, Purge: 1}
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc != wantTopic {
		t.Fatalf("topic counters = %+v err=%v, want %+v", tc, err, wantTopic)
	}
}

// TestStatsConcurrentFlushLockOrder: both stores' sinks hold the same coords
// and their FlushStats run concurrently round after round. FlushStats sorts
// coords by (topicID, channelID) before batching, so every multi-row upsert —
// whichever store, tick, or process issued it — locks rows in one global
// order and two overlapping flushes cannot AB/BA deadlock (PostgreSQL detects
// that pattern as SQLSTATE 40P01; lock waits can also time out). A deadlock
// surfaces here as a failed flush round (kept loud on purpose); the totals
// assert no delta was lost or doubled either way.
func TestStatsConcurrentFlushLockOrder(t *testing.T) {
	db1 := testpostgres.Open(t)
	db2 := testpostgres.Open(t)
	s1 := postgres.New(db1)
	s2 := postgres.New(db2)
	ctx := context.Background()
	if err := s1.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s2.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statlock_" + time.Now().Format("150405.000")
	var chIDs []int64
	for _, name := range []string{"a", "b", "c"} {
		id, err := s1.EnsureChannel(ctx, topic, name)
		if err != nil {
			t.Fatal(err)
		}
		chIDs = append(chIDs, id)
	}
	topicID, err := s1.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	const rounds = 20
	for r := 0; r < rounds; r++ {
		// Each side fans a fresh publish out to all three channels, so both
		// sinks hold the same three coords before the flushes race.
		for _, s := range []*postgres.Store{s1, s2} {
			if _, err := s.Publish(ctx, topicID, []byte("m"), store.PublishOpts{TTL: time.Hour}); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		for _, s := range []*postgres.Store{s1, s2} {
			go func(s *postgres.Store) {
				defer wg.Done()
				if err := s.FlushStats(ctx); err != nil {
					t.Errorf("round %d concurrent flush: %v", r, err)
				}
			}(s)
		}
		wg.Wait()
	}

	// Both sinks drained: each channel saw 2 fan-out publishes per round.
	for _, id := range chIDs {
		if c, err := s1.ChannelCounters(ctx, id); err != nil || c.Publish != 2*rounds {
			t.Fatalf("channel %d counters = %+v err=%v, want publish=%d", id, c, err, 2*rounds)
		}
	}
	if tc, err := s1.TopicCounters(ctx, topicID); err != nil || tc.Publish != 2*rounds*int64(len(chIDs)) {
		t.Fatalf("topic rollup = %+v err=%v, want publish=%d", tc, err, 2*rounds*int64(len(chIDs)))
	}
}
