//go:build integration

package sqlite_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/usual2970/novaque/driver/sqlite"
	"github.com/usual2970/novaque/internal/testsqlite"
	"github.com/usual2970/novaque/store"
)

// Sequential stats integration tests (U5), ported structurally from
// driver/mysql/stats_integration_test.go; concurrency coverage lives in
// stats_concurrency_integration_test.go.

// TestMigrateCreatesStatsDailyTable covers U1: the day-bucket stats table is
// created by Migrate and a second Migrate stays idempotent.
func TestMigrateCreatesStatsDailyTable(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'novaque_stats_daily'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("novaque_stats_daily tables = %d, want 1", n)
	}
}

// TestStatsReadBacklogAndPruneSmoke exercises the U1 read/prune/backlog SQL
// against real SQLite: zero reads on a fresh channel, the backlog Ready-vs-
// Pending split (KTD6), and day pruning that keeps recent buckets (KTD2).
// Counter bumps themselves arrive with U2; stale buckets are seeded directly.
func TestStatsReadBacklogAndPruneSmoke(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
		VALUES (date('now', '-40 days'), ?, ?, 3), (date('now'), ?, ?, 1)`,
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
		t.Fatalf("reap affected %d err=%v, want 1 (own lease reaped)", n, err)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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

// TestBacklogReadyExcludesExpired: Ready must mirror Claim's eligibility
// exactly (claim.go). Between TTL expiry and the purge tick, an expired
// pending row is still Pending (the row exists) but must never be Ready — no
// consumer can lease it — and Claim really returns nothing for it. Purge then
// drains both.
func TestBacklogReadyExcludesExpired(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statready_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("expiring"), store.PublishOpts{TTL: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}

	// Before expiry the delivery is both Pending and Ready.
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{Pending: 1, Ready: 1}) {
		t.Fatalf("backlog before expiry = %+v err=%v, want pending=1 ready=1", b, err)
	}

	// 3.6s for a 2s TTL (same second-flooring margin as the other timing
	// tests): the row is strictly past expires_at but still unpurged.
	time.Sleep(3600 * time.Millisecond)
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{Pending: 1}) {
		t.Fatalf("backlog after expiry = %+v err=%v, want pending=1 ready=0 (expired rows are not claimable)", b, err)
	}
	got, err := s.Claim(ctx, chID, "w", 30*time.Second, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("claim on expired-only channel returned %d deliveries, want 0", len(got))
	}

	if _, err := s.PurgeExpired(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if b, err := s.ChannelBacklog(ctx, chID); err != nil || b != (store.ChannelBacklog{}) {
		t.Fatalf("backlog after purge = %+v err=%v, want zeros", b, err)
	}
}

// TestStatsFlushMultiBatch: 501 channels under one topic, one publish -> 501
// stat coords in the sink -> FlushStats splits into a full 500-row batch (the
// pre-rendered statement) plus a 1-row tail, landing every coord exactly once.
// Covers the statsFlushBatch boundary small-cardinality tests never reach.
func TestStatsFlushMultiBatch(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statbatch_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	const channels = 501
	ids := make([]int64, 0, channels)
	for i := 0; i < channels; i++ {
		id, err := s.EnsureChannel(ctx, topic, fmt.Sprintf("c%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := s.Publish(ctx, topicID, []byte("fan"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}

	// Rollup exact: 501 fan-out publishes means no coord was lost or doubled
	// across the batch boundary.
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc.Publish != channels {
		t.Fatalf("topic rollup after multi-batch flush = %+v err=%v, want publish=%d", tc, err, channels)
	}
	// Every channel exactly once — the rollup alone could hide a 2/0 swap.
	for _, id := range ids {
		if c, err := s.ChannelCounters(ctx, id); err != nil || c.Publish != 1 {
			t.Fatalf("channel %d counters = %+v err=%v, want publish=1", id, c, err)
		}
	}
}

// TestStatsFlushPartialBatchFailureReMerge: fail ONLY the tail batch — after
// the full 500-row batch already committed — with an unambiguous SERVER-SIDE
// failure. SQLite has no FOR UPDATE and readers do not block writers, so the
// MySQL X-lock/ER-1205 mechanics do not exist; the SQLite-shaped equivalent is
// a BEFORE UPDATE trigger on the tail coord that RAISEs ABORT. Rows are
// sorted by (topicID, channelID) before batching, so the highest channel id is
// the tail batch's only coord: its day row is pre-inserted (so the upsert
// takes the conflict-update path and the trigger fires), the trigger aborts
// the tail statement server-side (code 1811; statement-level rollback), and
// dropping the trigger lets the retry land. The re-merge must restore exactly
// the tail's deltas (never the committed batch-1 ones), so the retry lands
// every delta exactly once.
func TestStatsFlushPartialBatchFailureReMerge(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "statfailtail_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	const channels = 501
	ids := make([]int64, 0, channels)
	for i := 0; i < channels; i++ {
		id, err := s.EnsureChannel(ctx, topic, fmt.Sprintf("c%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := s.Publish(ctx, topicID, []byte("fan"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}

	// Pre-insert the tail coord's day row, then arm a trigger that aborts any
	// update of it. Trigger definitions take no bound parameters, so the
	// validated-from-our-side channel id is rendered directly.
	lastID := ids[channels-1]
	if _, err := db.ExecContext(ctx, `
		INSERT INTO novaque_stats_daily (day_utc, topic_id, channel_id)
		VALUES (date('now'), ?, ?)`, topicID, lastID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TRIGGER novaque_block_tail
		BEFORE UPDATE ON novaque_stats_daily
		WHEN NEW.channel_id = %s
		BEGIN
			SELECT RAISE(ABORT, 'tail blocked');
		END`, strconv.FormatInt(lastID, 10))); err != nil {
		t.Fatal(err)
	}

	// Batch 1 (500 unlocked rows) commits; the tail statement aborts
	// server-side on the trigger — an unambiguous statement rollback, so the
	// retry cannot double-count it.
	ferr := s.FlushStats(ctx)
	if ferr == nil {
		t.Fatal("tail flush must fail while the trigger aborts its update")
	}
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc.Publish > channels-1 {
		t.Fatalf("partial flush overshot: topic rollup = %+v err=%v, want <= %d before the retry", tc, err, channels-1)
	}

	if _, err := db.ExecContext(ctx, `DROP TRIGGER novaque_block_tail`); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	// Exactly once despite the retry: a re-merge that swept batch-1 deltas
	// back in would double the rollup; one that lost the tail would under-land.
	if tc, err := s.TopicCounters(ctx, topicID); err != nil || tc.Publish != channels {
		t.Fatalf("topic rollup after retry = %+v err=%v, want publish=%d", tc, err, channels)
	}
	if c, err := s.ChannelCounters(ctx, lastID); err != nil || c.Publish != 1 {
		t.Fatalf("locked tail channel counters = %+v err=%v, want publish=1 exactly once", c, err)
	}
	if c, err := s.ChannelCounters(ctx, ids[0]); err != nil || c.Publish != 1 {
		t.Fatalf("batch-1 channel counters after retry = %+v err=%v, want publish=1 (no double-count)", c, err)
	}
}
