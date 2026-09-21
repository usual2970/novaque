//go:build integration

package mysql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/usual2970/novaque/driver/mysql"
	"github.com/usual2970/novaque/internal/testmysql"
	"github.com/usual2970/novaque/store"
)

func TestMigrateIdempotentAndUniqueChannel(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "t1", "c1"); err != nil {
		t.Fatal(err)
	}
	id2, err := s.EnsureChannel(ctx, "t1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	id1, err := s.EnsureChannel(ctx, "t1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("expected same channel id, got %d vs %d", id1, id2)
	}
}

// TestMigrateCreatesStatsDailyTable covers U1: the day-bucket stats table is
// created by Migrate and a second Migrate stays idempotent.
func TestMigrateCreatesStatsDailyTable(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'novaque_stats_daily'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("novaque_stats_daily tables = %d, want 1", n)
	}
	var rows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM novaque_stats_daily`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("fresh stats table rows = %d, want 0", rows)
	}
}

// TestStatsReadBacklogAndPruneSmoke exercises the U1 read/prune/backlog SQL
// against real MySQL: zero reads on a fresh channel, the backlog Ready-vs-
// Pending split (KTD6), and day pruning that keeps recent buckets (KTD2).
// Counter bumps themselves arrive with U2; stale buckets are seeded directly.
func TestStatsReadBacklogAndPruneSmoke(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "stats_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh channel: every read is zeros, no rows churned.
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != (store.ChannelCounters{}) {
		t.Fatalf("fresh channel counters = %+v err=%v, want zeros", c, err)
	}
	if c, err := s.TopicCounters(ctx, topicID); err != nil || c != (store.ChannelCounters{}) {
		t.Fatalf("fresh topic counters = %+v err=%v, want zeros", c, err)
	}
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("fresh backlog = %+v err=%v, want zeros", b, err)
	}

	// Backlog split: one delayed publish (pending, not ready) + one immediate.
	if _, err := s.Publish(ctx, topicID, []byte("later"), store.PublishOpts{Delay: time.Minute, TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("now"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	b, err := s.ChannelBacklog(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	if b.Pending != 2 || b.Ready != 1 || b.InFlight != 0 || b.Dead != 0 {
		t.Fatalf("backlog = %+v, want pending=2 ready=1 in_flight=0 dead=0", b)
	}

	// Claiming the ready row moves it to in_flight; the delayed row waits.
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Body) != "now" {
		t.Fatalf("claim got %#v, want the immediate delivery", got)
	}
	b, err = s.ChannelBacklog(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	if b.Pending != 1 || b.Ready != 0 || b.InFlight != 1 || b.Dead != 0 {
		t.Fatalf("backlog after claim = %+v, want pending=1 ready=0 in_flight=1 dead=0", b)
	}

	// Prune deletes only buckets strictly older than retention.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO novaque_stats_daily (day_utc, topic_id, channel_id, publish)
		VALUES (UTC_DATE() - INTERVAL 40 DAY, ?, ?, 3), (UTC_DATE(), ?, ?, 1)`,
		topicID, chID, topicID, chID); err != nil {
		t.Fatal(err)
	}
	n, err := s.PruneStats(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("prune deleted %d rows, want 1", n)
	}
	var left int64
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(publish), 0) FROM novaque_stats_daily
		WHERE topic_id = ? AND channel_id = ?`, topicID, chID).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatalf("retained publish sum = %d, want 1 (today's bucket)", left)
	}
	if n, err := s.PruneStats(ctx, 30); err != nil || n != 0 {
		t.Fatalf("second prune deleted %d err=%v, want 0", n, err)
	}
}

func TestPublishFanoutAndNoRetroactive(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "fanout_" + time.Now().Format("150405.000")
	aID, err := s.EnsureChannel(ctx, topic, "A")
	if err != nil {
		t.Fatal(err)
	}
	bID, err := s.EnsureChannel(ctx, topic, "B")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	msgID, err := s.Publish(ctx, topicID, []byte("hello"), store.PublishOpts{
		TTL:         time.Hour,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if msgID == 0 {
		t.Fatal("expected message id")
	}

	a, err := s.Claim(ctx, aID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Claim(ctx, bID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("want 1 delivery each, got A=%d B=%d", len(a), len(b))
	}
	if string(a[0].Body) != "hello" || string(b[0].Body) != "hello" {
		t.Fatalf("body mismatch")
	}

	lateID, err := s.EnsureChannel(ctx, topic, "late")
	if err != nil {
		t.Fatal(err)
	}
	late, err := s.Claim(ctx, lateID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(late) != 0 {
		t.Fatalf("late channel should have 0 historical deliveries, got %d", len(late))
	}

	if _, err := s.Publish(ctx, topicID, []byte("next"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	late2, err := s.Claim(ctx, lateID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(late2) != 1 || string(late2[0].Body) != "next" {
		t.Fatalf("late should receive new publish, got %#v", late2)
	}
}

func TestClaimCompeteAndLeaseRedelivery(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "compete_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := s.Publish(ctx, topicID, []byte{byte(i)}, store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	c1, err := s.Claim(ctx, chID, "a", time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.Claim(ctx, chID, "b", time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(c1)+len(c2) != 4 {
		t.Fatalf("want partition of 4, got %d+%d", len(c1), len(c2))
	}
	seen := map[int64]bool{}
	for _, d := range append(c1, c2...) {
		if seen[d.ID] {
			t.Fatalf("duplicate delivery id %d", d.ID)
		}
		seen[d.ID] = true
	}

	d := c1[0]
	time.Sleep(2500 * time.Millisecond)
	if _, err := s.ReapExpiredLeases(ctx, 100); err != nil {
		t.Fatal(err)
	}
	again, err := s.Claim(ctx, chID, "c", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range again {
		if x.MessageID == d.MessageID {
			found = true
		}
	}
	if !found {
		t.Fatal("expected redelivery after lease expiry")
	}
	if err := s.Ack(ctx, d.ID, d.LeaseToken); err == nil {
		t.Fatal("expected stale ack to fail")
	}
}

func TestPublishDelayClaimAndReject(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "delay_" + time.Now().Format("150405.000")
	aID, err := s.EnsureChannel(ctx, topic, "A")
	if err != nil {
		t.Fatal(err)
	}
	bID, err := s.EnsureChannel(ctx, topic, "B")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Publish(ctx, topicID, []byte("too-long"), store.PublishOpts{
		Delay: 8 * 24 * time.Hour,
		TTL:   7 * 24 * time.Hour,
	})
	if !errors.Is(err, store.ErrDelayExceedsTTL) {
		t.Fatalf("want ErrDelayExceedsTTL, got %v", err)
	}

	msgID, err := s.Publish(ctx, topicID, []byte("later"), store.PublishOpts{
		Delay:       2 * time.Second,
		TTL:         time.Hour,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if msgID == 0 {
		t.Fatal("expected message id")
	}

	earlyA, err := s.Claim(ctx, aID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	earlyB, err := s.Claim(ctx, bID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(earlyA) != 0 || len(earlyB) != 0 {
		t.Fatalf("expected empty claims before delay, got A=%d B=%d", len(earlyA), len(earlyB))
	}

	time.Sleep(2500 * time.Millisecond)

	readyA, err := s.Claim(ctx, aID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	readyB, err := s.Claim(ctx, bID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(readyA) != 1 || len(readyB) != 1 {
		t.Fatalf("want one delivery each after delay, got A=%d B=%d", len(readyA), len(readyB))
	}
	if string(readyA[0].Body) != "later" || string(readyB[0].Body) != "later" {
		t.Fatalf("bodies %#v %#v", readyA[0].Body, readyB[0].Body)
	}

	// Immediate requeue after delayed claim (AE7).
	if err := s.Requeue(ctx, readyA[0].ID, readyA[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	again, err := s.Claim(ctx, aID, "w2", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].MessageID != readyA[0].MessageID {
		t.Fatalf("expected immediate reclaim, got %#v", again)
	}
}

func TestPublishNoDelayStillImmediate(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "nodelay_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("now"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want immediate claim, got %d", len(got))
	}
}
