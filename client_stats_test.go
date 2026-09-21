package novaque_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/usual2970/novaque"
)

// Stats-side test helpers and the U3 stats API tests, split from
// client_test.go to keep both under 1000 lines.

// flushCount returns how many FlushStats calls reached the fake.
func (f *fakeStore) flushCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushN
}

// prunedWith reports whether PruneStats was ever called with retention.
func (f *fakeStore) prunedWith(retention int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.pruneRetentions {
		if r == retention {
			return true
		}
	}
	return false
}

// --- Stats API (U3): forwarding, retention option, flush/prune loops ---

// TestStatsReadsForwardResolvedIDs: the public read methods resolve names
// through the store and hand back its counters/backlog unchanged.
func TestStatsReadsForwardResolvedIDs(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}

	cc, err := c.ChannelCounters(ctx, "st", "a")
	if err != nil {
		t.Fatal(err)
	}
	// The fake assigns ids in creation order: topic "st" -> 1, channel "a" -> 2.
	if cc.Publish != 1002 {
		t.Fatalf("channel counters = %+v, want publish=1002 (channel id 2)", cc)
	}
	tc, err := c.TopicCounters(ctx, "st")
	if err != nil {
		t.Fatal(err)
	}
	if tc.Publish != 2001 {
		t.Fatalf("topic counters = %+v, want publish=2001 (topic id 1)", tc)
	}
	bl, err := c.ChannelBacklog(ctx, "st", "a")
	if err != nil {
		t.Fatal(err)
	}
	if bl.Pending != 3002 {
		t.Fatalf("backlog = %+v, want pending=3002 (channel id 2)", bl)
	}

	f.mu.Lock()
	gotCh, gotTopic, gotBacklog := f.lastChannelCountersID, f.lastTopicCountersID, f.lastBacklogID
	f.mu.Unlock()
	if gotCh != 2 || gotTopic != 1 || gotBacklog != 2 {
		t.Fatalf("store saw channel/topic/backlog ids %d/%d/%d, want 2/1/2", gotCh, gotTopic, gotBacklog)
	}
}

// TestStatsReadErrorsWrapped: store failures come back novaque-prefixed.
func TestStatsReadErrorsWrapped(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.failStatsRead, true)
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		call func() error
		want string
	}{
		{"channel counters", func() error {
			_, err := c.ChannelCounters(ctx, "st", "a")
			return err
		}, "novaque channel counters: stats boom"},
		{"topic counters", func() error {
			_, err := c.TopicCounters(ctx, "st")
			return err
		}, "novaque topic counters: stats boom"},
		{"backlog", func() error {
			_, err := c.ChannelBacklog(ctx, "st", "a")
			return err
		}, "novaque channel backlog: stats boom"},
	} {
		err := tc.call()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s error = %v, want prefix %q", tc.name, err, tc.want)
		}
	}
}

// TestPruneStatsForwardsRetention: the default retention (30) and an override
// reach the store, and the deleted-row count is returned.
func TestPruneStatsForwardsRetention(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{}) // StatsRetentionDays defaults to 30
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.PruneStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("prune deleted = %d, want the store's 42", n)
	}
	if !f.prunedWith(30) {
		t.Fatalf("default prune retention must be 30, saw %v", f.pruneRetentions)
	}

	f2 := newFake()
	c2, err := novaque.Open(f2, novaque.Options{StatsRetentionDays: 7})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.PruneStats(ctx); err != nil {
		t.Fatal(err)
	}
	if !f2.prunedWith(7) {
		t.Fatalf("override prune retention must be 7, saw %v", f2.pruneRetentions)
	}

	f.setFlag(&f.failPruneStats, true)
	if _, err := c.PruneStats(ctx); err == nil || !strings.Contains(err.Error(), "novaque prune stats: prune boom") {
		t.Fatalf("prune error = %v, want novaque-prefixed wrap", err)
	}
}

// TestFlushStatsForwards: the explicit drain reaches the store and errors wrap.
func TestFlushStatsForwards(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.flushCount(); got != 1 {
		t.Fatalf("flush count = %d, want 1", got)
	}

	f.setFlag(&f.failFlushStats, true)
	if err := c.FlushStats(ctx); err == nil || !strings.Contains(err.Error(), "novaque flush stats: flush boom") {
		t.Fatalf("flush error = %v, want novaque-prefixed wrap", err)
	}
}

// TestStartTicksStatsFlushAndPrune: the maintenance loop drains counters on
// StatsFlushInterval and prunes with the configured retention.
func TestStartTicksStatsFlushAndPrune(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{
		StatsFlushInterval: 2 * time.Millisecond,
		StatsPruneInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "at least 2 stats flush ticks", func() bool {
		return f.flushCount() >= 2
	})
	waitFor(t, 5*time.Second, "stats prune tick with retention 30", func() bool {
		return f.prunedWith(30)
	})
}

// TestStatsLoopErrorsLogged: swallowed flush/prune tick failures emit Errors
// with the matching op, like reap/purge.
func TestStatsLoopErrorsLogged(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.setFlag(&f.failFlushStats, true)
	f.setFlag(&f.failPruneStats, true)
	spy := newSpyLogger()
	c, err := novaque.Open(f, novaque.Options{
		Logger:             spy,
		StatsFlushInterval: 2 * time.Millisecond,
		StatsPruneInterval: 3 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Shutdown(ctx) }()

	waitFor(t, 5*time.Second, "stats flush Error", func() bool {
		return spy.sink.countOp("error", "flush_stats") >= 1
	})
	waitFor(t, 5*time.Second, "stats prune Error", func() bool {
		return spy.sink.countOp("error", "prune_stats") >= 1
	})
}

// TestShutdownFlushesStatsOnce: after the loops stop, Shutdown performs
// exactly one final flush; a Client that never started flushes nothing.
func TestShutdownFlushesStatsOnce(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{
		StatsFlushInterval: time.Hour, // no tick may fire before Shutdown
		StatsPruneInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.flushCount(); got != 1 {
		t.Fatalf("final flush count = %d, want exactly 1", got)
	}
	f.mu.Lock()
	prunes := len(f.pruneRetentions)
	f.mu.Unlock()
	if prunes != 0 {
		t.Fatalf("shutdown must not prune, saw %d prune calls", prunes)
	}

	// Second Shutdown (not started anymore) adds no further flush.
	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.flushCount(); got != 1 {
		t.Fatalf("flush count after second Shutdown = %d, want 1", got)
	}

	// Never-started Client: fully silent, no flush.
	f2 := newFake()
	c2, err := novaque.Open(f2, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := f2.flushCount(); got != 0 {
		t.Fatalf("never-started Client flush count = %d, want 0", got)
	}
}
