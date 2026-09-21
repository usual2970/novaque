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

// TestStatsCountersFanoutPublish covers AE1: a single publish fans out to two
// channels, so each channel's publish counter is 1 (per-delivery unit, KTD3)
// and each pending backlog is 1.
func TestStatsCountersFanoutPublish(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statfan_" + time.Now().Format("150405.000")
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
	if _, err := s.Publish(ctx, topicID, []byte("hello"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{aID, bID} {
		c, err := s.ChannelCounters(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if c != (store.ChannelCounters{Publish: 1}) {
			t.Fatalf("channel %d counters = %+v, want publish=1 only", id, c)
		}
		b, err := s.ChannelBacklog(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if b != (store.ChannelBacklog{Pending: 1, Ready: 1}) {
			t.Fatalf("channel %d backlog = %+v, want pending=1 ready=1", id, b)
		}
	}
	tc, err := s.TopicCounters(ctx, topicID)
	if err != nil {
		t.Fatal(err)
	}
	if tc != (store.ChannelCounters{Publish: 2}) {
		t.Fatalf("topic counters = %+v, want publish=2", tc)
	}
}

// TestStatsCountersZeroChannelPublish covers AE2: a publish with no channels
// bumps the channel_id=0 sentinel row only; real channels keep zero backlog.
func TestStatsCountersZeroChannelPublish(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statzero_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("nobody"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	var sentinel int64
	if err := db.QueryRowContext(ctx, `
		SELECT publish FROM novaque_stats_daily
		WHERE topic_id = ? AND channel_id = 0`, topicID).Scan(&sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel != 1 {
		t.Fatalf("sentinel publish = %d, want 1", sentinel)
	}
	var rows int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM novaque_stats_daily WHERE topic_id = ?`, topicID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("topic stats rows = %d, want 1 (sentinel only)", rows)
	}
	tc, err := s.TopicCounters(ctx, topicID)
	if err != nil {
		t.Fatal(err)
	}
	if tc != (store.ChannelCounters{Publish: 1}) {
		t.Fatalf("topic counters = %+v, want publish=1 only", tc)
	}
	// A channel created after the publish sees neither backlog nor counters.
	chID, err := s.EnsureChannel(ctx, topic, "late")
	if err != nil {
		t.Fatal(err)
	}
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("late channel backlog = %+v err=%v, want zeros", b, err)
	}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != (store.ChannelCounters{}) {
		t.Fatalf("late channel counters = %+v err=%v, want zeros", c, err)
	}
}

// TestStatsCountersClaimAckReack covers AE3: claim then ack bump claim/ack and
// drain in_flight; a stale re-ack errors and does not double-count.
func TestStatsCountersClaimAckReack(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statack_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claim got %d deliveries, want 1", len(got))
	}
	if err := s.Ack(ctx, got[0].ID, got[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	want := store.ChannelCounters{Publish: 1, Claim: 1, Ack: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after ack = %+v err=%v, want %+v", c, err, want)
	}
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("backlog after ack = %+v err=%v, want zeros", b, err)
	}
	if err := s.Ack(ctx, got[0].ID, got[0].LeaseToken); err == nil {
		t.Fatal("expected stale re-ack to fail")
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after re-ack = %+v err=%v, want unchanged %+v", c, err, want)
	}
}

// TestStatsCountersRequeueNotReap covers AE4: only handler Requeue bumps the
// requeue counter; lease reap of the same in_flight->pending transition bumps
// nothing at all (KTD5).
func TestStatsCountersRequeueNotReap(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statreq_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w", time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claim got %d deliveries, want 1", len(got))
	}
	// 2.5s for a 1s lease: DB second flooring means a shorter sleep can land
	// on the same DB second as lease_until (same margin as the compete test).
	time.Sleep(2500 * time.Millisecond)
	if n, err := s.ReapExpiredLeases(ctx, 100); err != nil || n != 1 {
		t.Fatalf("reap affected %d err=%v, want 1", n, err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	afterReap := store.ChannelCounters{Publish: 1, Claim: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != afterReap {
		t.Fatalf("counters after reap = %+v err=%v, want %+v (reap records nothing)", c, err, afterReap)
	}
	got2, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 1 {
		t.Fatalf("reclaim got %d deliveries, want 1", len(got2))
	}
	if err := s.Requeue(ctx, got2[0].ID, got2[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	afterRequeue := store.ChannelCounters{Publish: 1, Claim: 2, Requeue: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != afterRequeue {
		t.Fatalf("counters after requeue = %+v err=%v, want %+v", c, err, afterRequeue)
	}
}

// TestStatsCountersPoisonDead covers AE5: a delivery past max_attempts is
// terminalized inside Claim, bumping both claim and dead (KTD4), never handed
// to the handler, and visible as dead backlog.
func TestStatsCountersPoisonDead(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statpoison_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("poison"), store.PublishOpts{TTL: time.Hour, MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim got %d deliveries, want 1", len(first))
	}
	if err := s.Requeue(ctx, first[0].ID, first[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("poison claim handed %d deliveries to handler, want 0", len(second))
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	want := store.ChannelCounters{Publish: 1, Claim: 2, Requeue: 1, Dead: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after poison = %+v err=%v, want %+v", c, err, want)
	}
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{Dead: 1}) {
		t.Fatalf("backlog after poison = %+v err=%v, want dead=1", b, err)
	}
}

// TestStatsCountersPurge covers AE6: purge deletes expired deliveries and
// attributes purged per channel; live backlog drops to zero while same-day
// publish counters survive until the day prune.
func TestStatsCountersPurge(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statpurge_" + time.Now().Format("150405.000")
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
	if _, err := s.Publish(ctx, topicID, []byte("short"), store.PublishOpts{TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	// 2.6s for a 1s TTL: expires_at floors to publish-second+1, so the DB
	// clock needs a full extra second of margin to pass it strictly.
	time.Sleep(2600 * time.Millisecond)
	if _, err := s.PurgeExpired(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{aID, bID} {
		want := store.ChannelCounters{Publish: 1, Purge: 1}
		if c, err := s.ChannelCounters(ctx, id); err != nil || c != want {
			t.Fatalf("channel %d counters after purge = %+v err=%v, want %+v", id, c, err, want)
		}
		if b, err := s.ChannelBacklog(ctx, id); err != nil || b != (store.ChannelBacklog{}) {
			t.Fatalf("channel %d backlog after purge = %+v err=%v, want zeros", id, b, err)
		}
	}
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc != (store.ChannelCounters{Publish: 2, Purge: 2}) {
		t.Fatalf("topic counters after purge = %+v err=%v, want publish=2 purge=2", tc, err)
	}
}

// TestStatsLeaseMismatchNoCounters: rejected ack/requeue with a wrong lease
// token record nothing (R5 RowsAffected gate).
func TestStatsLeaseMismatchNoCounters(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statmismatch_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claim got %d deliveries, want 1", len(got))
	}
	if err := s.Ack(ctx, got[0].ID, "not-the-token"); err == nil {
		t.Fatal("expected mismatched ack to fail")
	}
	if err := s.Requeue(ctx, got[0].ID, "not-the-token", time.Time{}); err == nil {
		t.Fatal("expected mismatched requeue to fail")
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	want := store.ChannelCounters{Publish: 1, Claim: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after mismatches = %+v err=%v, want %+v (no ack/requeue)", c, err, want)
	}
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{InFlight: 1}) {
		t.Fatalf("backlog after mismatches = %+v err=%v, want in_flight=1", b, err)
	}
}

// TestStatsEmptyMutationsNoRows: an empty Claim and a PurgeExpired with
// nothing doomed create no stats rows at all.
func TestStatsEmptyMutationsNoRows(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statempty_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("claim on empty channel got %d deliveries, want 0", len(got))
	}
	if _, err := s.PurgeExpired(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	var rows int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM novaque_stats_daily WHERE topic_id = ?`, topicID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("stats rows after empty mutations = %d, want 0", rows)
	}
}

// TestStatsFlushFailureRecovery proves the re-merge path: a failed flush
// (canceled context) keeps every delta buffered, and the next flush with a
// live context lands all of it.
func TestStatsFlushFailureRecovery(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statflush_" + time.Now().Format("150405.000")
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
	for i := 0; i < 2; i++ {
		if _, err := s.Publish(ctx, topicID, []byte("m"), store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Claim(ctx, aID, "w", 10*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claim got %d deliveries, want 1", len(got))
	}
	if err := s.Ack(ctx, got[0].ID, got[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.FlushStats(cancelCtx); err == nil {
		t.Fatal("expected flush with canceled context to fail")
	}
	// The failed flush landed nothing: table reads still see zeros.
	if c, err := s.ChannelCounters(ctx, aID); err != nil || c != (store.ChannelCounters{}) {
		t.Fatalf("counters after failed flush = %+v err=%v, want zeros", c, err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	wantA := store.ChannelCounters{Publish: 2, Claim: 1, Ack: 1}
	if c, err := s.ChannelCounters(ctx, aID); err != nil || c != wantA {
		t.Fatalf("channel A counters after recovery = %+v err=%v, want %+v", c, err, wantA)
	}
	wantB := store.ChannelCounters{Publish: 2}
	if c, err := s.ChannelCounters(ctx, bID); err != nil || c != wantB {
		t.Fatalf("channel B counters after recovery = %+v err=%v, want %+v", c, err, wantB)
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
