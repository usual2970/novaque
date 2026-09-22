//go:build integration

package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/usual2970/novaque/driver/sqlite"
	"github.com/usual2970/novaque/internal/testsqlite"
	"github.com/usual2970/novaque/store"
)

// TestMigrateFailsWithoutForeignKeys (#1): a file database opened directly
// through the registered modernc driver with a DSN that omits the
// foreign_keys pragma must be rejected by Migrate's FK gate. The error must
// name the disabled pragma and the fix (_pragma=foreign_keys(1)). The
// internal/testsqlite helper is deliberately not used — it always enables FK.
func TestMigrateFailsWithoutForeignKeys(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "nofk.db") +
		"?_pragma=busy_timeout(10000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := sqlite.New(db)
	err = s.Migrate(context.Background())
	if err == nil {
		t.Fatal("Migrate on a DSN without foreign_keys: want error, got nil")
	}
	if !strings.Contains(err.Error(), "foreign_keys") {
		t.Fatalf("Migrate error %q does not mention foreign_keys", err)
	}
	if !strings.Contains(err.Error(), "_pragma=foreign_keys(1)") {
		t.Fatalf("Migrate error %q does not mention _pragma=foreign_keys(1)", err)
	}
}

// TestBacklogsForTopicMultiTopic (#2) exercises BacklogsForTopic across two
// topics with channels in every state: ready, delayed (pending/not ready),
// in_flight, dead, and TTL-expired-but-unpurged (pending/not ready), plus a
// delivery-less channel that must produce no row. Per-channel counts and the
// ready slice are asserted per topic; the global Backlogs batch must agree.
func TestBacklogsForTopicMultiTopic(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")

	x1, err := s.EnsureChannel(ctx, "bltopicX_"+suffix, "x1")
	if err != nil {
		t.Fatal(err)
	}
	x2, err := s.EnsureChannel(ctx, "bltopicX_"+suffix, "x2")
	if err != nil {
		t.Fatal(err)
	}
	x3, err := s.EnsureChannel(ctx, "bltopicX_"+suffix, "x3")
	if err != nil {
		t.Fatal(err)
	}
	topicX, err := s.EnsureTopic(ctx, "bltopicX_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	y1, err := s.EnsureChannel(ctx, "bltopicY_"+suffix, "y1")
	if err != nil {
		t.Fatal(err)
	}
	y2, err := s.EnsureChannel(ctx, "bltopicY_"+suffix, "y2")
	if err != nil {
		t.Fatal(err)
	}
	topicY, err := s.EnsureTopic(ctx, "bltopicY_"+suffix)
	if err != nil {
		t.Fatal(err)
	}

	// All channels exist before any publish, so each publish fans out to
	// every channel; keepChannelDelivery prunes the sibling deliveries so
	// each seed message drives exactly one channel's state.

	// x1: one ready + one delayed (pending, not ready).
	msgReady, err := s.Publish(ctx, topicX, []byte("ready"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	keepChannelDelivery(ctx, t, db, msgReady, x1)
	msgDelayed, err := s.Publish(ctx, topicX, []byte("delayed"), store.PublishOpts{Delay: time.Hour, TTL: 2 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	keepChannelDelivery(ctx, t, db, msgDelayed, x1)
	// x2: one leased in_flight, then one dead (MaxAttempts=1: claim, requeue,
	// the terminal claim moves it to dead).
	msgFlight, err := s.Publish(ctx, topicX, []byte("flight"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	keepChannelDelivery(ctx, t, db, msgFlight, x2)
	if got, err := s.Claim(ctx, x2, "wf", 10*time.Second, 10); err != nil || len(got) != 1 {
		t.Fatalf("x2 lease first = %v err=%v, want 1", got, err)
	}
	msgPoison, err := s.Publish(ctx, topicX, []byte("poison"), store.PublishOpts{TTL: time.Hour, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	keepChannelDelivery(ctx, t, db, msgPoison, x2)
	first, err := s.Claim(ctx, x2, "wd", 10*time.Second, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("x2 poison first claim = %v err=%v, want 1", first, err)
	}
	if err := s.Requeue(ctx, first[0].ID, first[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Claim(ctx, x2, "wd", 10*time.Second, 10); err != nil || len(got) != 0 {
		t.Fatalf("x2 terminal claim = %v err=%v, want 0 (dead)", got, err)
	}
	// y1: one ready.
	msgYReady, err := s.Publish(ctx, topicY, []byte("yready"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	keepChannelDelivery(ctx, t, db, msgYReady, y1)
	// y2: one TTL-expired-but-unpurged pending row (message + delivery).
	msgExpired, err := s.Publish(ctx, topicY, []byte("expired"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	keepChannelDelivery(ctx, t, db, msgExpired, y2)
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_messages SET expires_at = unixepoch() - 10 WHERE id = ?`,
		msgExpired); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_deliveries SET expires_at = unixepoch() - 10
		WHERE message_id = ? AND channel_id = ?`,
		msgExpired, y2); err != nil {
		t.Fatal(err)
	}

	want := map[int64]store.BacklogRow{
		x1: {ChannelID: x1, Pending: 2, Ready: 1},
		x2: {ChannelID: x2, InFlight: 1, Dead: 1},
		y1: {ChannelID: y1, Pending: 1, Ready: 1},
		y2: {ChannelID: y2, Pending: 1, Ready: 0},
	}

	checkTopic := func(topicID int64, channels []int64) {
		t.Helper()
		rows, err := s.BacklogsForTopic(ctx, topicID)
		if err != nil {
			t.Fatalf("BacklogsForTopic(%d): %v", topicID, err)
		}
		if len(rows) != len(channels) {
			t.Fatalf("topic %d backlog rows = %d, want %d", topicID, len(rows), len(channels))
		}
		seen := make(map[int64]bool, len(channels))
		for _, r := range rows {
			seen[r.ChannelID] = true
			exp, ok := want[r.ChannelID]
			if !ok {
				t.Fatalf("topic %d: unexpected row for channel %d", topicID, r.ChannelID)
			}
			if r != exp {
				t.Fatalf("topic %d channel %d row = %+v, want %+v", topicID, r.ChannelID, r, exp)
			}
		}
		for _, ch := range channels {
			if !seen[ch] {
				t.Fatalf("topic %d: missing row for channel %d", topicID, ch)
			}
		}
	}

	// x3 has no deliveries: no row in the topic-scoped batch.
	checkTopic(topicX, []int64{x1, x2})
	checkTopic(topicY, []int64{y1, y2})

	// Global batch must contain the same four rows (and none for x3).
	all, err := s.Backlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotGlobal := make(map[int64]store.BacklogRow, len(all))
	for _, r := range all {
		gotGlobal[r.ChannelID] = r
	}
	if _, ok := gotGlobal[x3]; ok {
		t.Fatal("delivery-less channel x3 produced a global backlog row")
	}
	for ch, exp := range want {
		got, ok := gotGlobal[ch]
		if !ok {
			t.Fatalf("global Backlogs missing channel %d", ch)
		}
		if got != exp {
			t.Fatalf("global Backlogs channel %d = %+v, want %+v", ch, got, exp)
		}
	}
}

// TestPublishAbsoluteExpiresAt (#6): with TTL unset and an absolute
// PublishOpts.ExpiresAt, the raw novaque_messages.expires_at (and the
// denormalized delivery expires_at) are that exact unix second; a future
// expiry is claimable, a past expiry is excluded from Ready/Claim and is
// removed by PurgeExpired (delivery plus the orphaned message).
func TestPublishAbsoluteExpiresAt(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, "absexp_"+suffix, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, "absexp_"+suffix)
	if err != nil {
		t.Fatal(err)
	}

	future := time.Unix(time.Now().Unix()+3600, 0).UTC()
	msgFuture, err := s.Publish(ctx, topicID, []byte("future"), store.PublishOpts{ExpiresAt: future})
	if err != nil {
		t.Fatal(err)
	}
	var msgExpires int64
	if err := db.QueryRowContext(ctx, `
		SELECT expires_at FROM novaque_messages WHERE id = ?`, msgFuture).Scan(&msgExpires); err != nil {
		t.Fatal(err)
	}
	if msgExpires != future.Unix() {
		t.Fatalf("messages.expires_at = %d, want absolute %d", msgExpires, future.Unix())
	}
	var delExpires int64
	if err := db.QueryRowContext(ctx, `
		SELECT expires_at FROM novaque_deliveries WHERE message_id = ? AND channel_id = ?`,
		msgFuture, chID).Scan(&delExpires); err != nil {
		t.Fatal(err)
	}
	if delExpires != future.Unix() {
		t.Fatalf("deliveries.expires_at = %d, want absolute %d", delExpires, future.Unix())
	}
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Body) != "future" {
		t.Fatalf("future-expiry claim = %#v, want the future delivery", got)
	}

	past := time.Unix(time.Now().Unix()-60, 0).UTC()
	msgPast, err := s.Publish(ctx, topicID, []byte("past"), store.PublishOpts{ExpiresAt: past})
	if err != nil {
		t.Fatal(err)
	}
	var pastDeliveryID int64
	if err := db.QueryRowContext(ctx, `
		SELECT id FROM novaque_deliveries WHERE message_id = ? AND channel_id = ?`,
		msgPast, chID).Scan(&pastDeliveryID); err != nil {
		t.Fatal(err)
	}
	b, err := s.ChannelBacklog(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	// The first delivery is in_flight; the past one is pending but not ready.
	if b.Pending != 1 || b.Ready != 0 || b.InFlight != 1 {
		t.Fatalf("backlog after past publish = %+v, want Pending=1 Ready=0 InFlight=1", b)
	}
	empty, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("claim past-expiry delivery = %#v, want empty", empty)
	}

	n, err := s.PurgeExpired(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	// Expired pending delivery + its now-orphaned expired message.
	if n != 2 {
		t.Fatalf("PurgeExpired affected %d rows, want 2", n)
	}
	if c := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, pastDeliveryID); c != 0 {
		t.Fatalf("expired delivery survived purge, count=%d", c)
	}
	if c := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, msgPast); c != 0 {
		t.Fatalf("expired message survived purge, count=%d", c)
	}
}

// TestDelayedRequeue (#4): after a claim, requeue with a future availableAt —
// Pending counts the row, Ready excludes it, and Claim returns nothing. Moving
// available_at to the past via raw SQL makes it claimable again (same row).
func TestDelayedRequeue(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, "delayedrq_"+suffix, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, "delayedrq_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	msgID, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, chID, "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim = %d deliveries, want 1", len(first))
	}
	d := first[0]

	if err := s.Requeue(ctx, d.ID, d.LeaseToken, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	b, err := s.ChannelBacklog(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	if b.Pending != 1 || b.Ready != 0 || b.InFlight != 0 {
		t.Fatalf("backlog after future requeue = %+v, want Pending=1 Ready=0 InFlight=0", b)
	}
	empty, err := s.Claim(ctx, chID, "w2", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("claim before future availableAt = %#v, want empty", empty)
	}

	// Release the delay explicitly (raw SQL), then it is claimable again.
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_deliveries SET available_at = unixepoch() - 1 WHERE id = ?`,
		d.ID); err != nil {
		t.Fatal(err)
	}
	again, err := s.Claim(ctx, chID, "w3", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].ID != d.ID || again[0].MessageID != msgID {
		t.Fatalf("reclaim after moving available_at = %#v, want delivery %d (message %d)", again, d.ID, msgID)
	}
}

// TestPublishDefaultTTL (#11): with TTL=0 and no ExpiresAt, the stored expiry
// is the publish-time DB unix second plus DefaultPublishTTL seconds; the
// delivery is immediately claimable.
func TestPublishDefaultTTL(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, "defaultttl_"+suffix, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, "defaultttl_"+suffix)
	if err != nil {
		t.Fatal(err)
	}

	defaultTTL := store.DurationSec(store.DefaultPublishTTL)
	var before int64
	if err := db.QueryRowContext(ctx, `SELECT unixepoch()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	msgID, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{})
	if err != nil {
		t.Fatal(err)
	}
	var after, expiresAt int64
	if err := db.QueryRowContext(ctx, `SELECT unixepoch()`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT expires_at FROM novaque_messages WHERE id = ?`, msgID).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	// expires_at must equal the DB publish second + DefaultPublishTTL,
	// bracketed by DB seconds sampled immediately before/after publish.
	if expiresAt < before+defaultTTL || expiresAt > after+defaultTTL {
		t.Fatalf("expires_at=%d, want publish-time + %d in [%d, %d]",
			expiresAt, defaultTTL, before+defaultTTL, after+defaultTTL)
	}

	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("default-TTL claim = %d deliveries, want 1", len(got))
	}
}

// TestPublishNilBody (#12): a nil body publishes and delivers without error;
// the delivered body has length zero.
func TestPublishNilBody(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, "nilbody_"+suffix, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, "nilbody_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, nil, store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatalf("claim nil-body delivery: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("claim = %d deliveries, want 1", len(got))
	}
	if len(got[0].Body) != 0 {
		t.Fatalf("delivered body length = %d, want 0", len(got[0].Body))
	}
}

// TestNameValidation (#13): EnsureTopic/EnsureChannel reject names outside
// the 1..64-char [a-zA-Z0-9._-] rule; a valid name still succeeds.
func TestNameValidation(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	wantErr := func(name string, fn func() error) {
		t.Helper()
		if err := fn(); err == nil {
			t.Fatalf("name %q: want validation error, got nil", name)
		}
	}

	wantErr("<empty topic>", func() error {
		_, err := s.EnsureTopic(ctx, "")
		return err
	})
	wantErr("65-char topic", func() error {
		_, err := s.EnsureTopic(ctx, strings.Repeat("a", 65))
		return err
	})
	wantErr("topic with space", func() error {
		_, err := s.EnsureTopic(ctx, "bad name")
		return err
	})
	wantErr("topic with slash", func() error {
		_, err := s.EnsureTopic(ctx, "bad/name")
		return err
	})

	wantErr("<empty channel>", func() error {
		_, err := s.EnsureChannel(ctx, "nvtopic", "")
		return err
	})
	wantErr("65-char channel", func() error {
		_, err := s.EnsureChannel(ctx, "nvtopic", strings.Repeat("c", 65))
		return err
	})
	wantErr("channel with '!'", func() error {
		_, err := s.EnsureChannel(ctx, "nvtopic", "channel!")
		return err
	})
	wantErr("empty topic via EnsureChannel", func() error {
		_, err := s.EnsureChannel(ctx, "", "c")
		return err
	})

	// Sanity: boundary-valid names succeed.
	if _, err := s.EnsureTopic(ctx, "valid_Topic-1.2"); err != nil {
		t.Fatalf("valid topic rejected: %v", err)
	}
	if _, err := s.EnsureChannel(ctx, "valid_Topic-1.2", "chan_0-X.Y"); err != nil {
		t.Fatalf("valid channel rejected: %v", err)
	}
}

// TestPurgeExpiredInFlightAfterLease (#9): a claimed delivery is in_flight
// with a future lease_until; once both expires_at (message + delivery) and
// lease_until are pushed into the past via raw SQL, PurgeExpired removes the
// in_flight delivery and its orphaned message, and the purged counter moves.
func TestPurgeExpiredInFlightAfterLease(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, "ifpurge_"+suffix, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, "ifpurge_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	msgID, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("claim = %d deliveries, want 1", len(first))
	}
	d := first[0]

	// Expire the row and let its live lease lapse.
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_deliveries
		SET expires_at = unixepoch() - 10, lease_until = unixepoch() - 10
		WHERE id = ?`, d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_messages SET expires_at = unixepoch() - 10 WHERE id = ?`,
		msgID); err != nil {
		t.Fatal(err)
	}

	n, err := s.PurgeExpired(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	// One expired in_flight delivery + its now-orphaned message.
	if n != 2 {
		t.Fatalf("PurgeExpired affected %d rows, want 2", n)
	}
	if c := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, d.ID); c != 0 {
		t.Fatalf("in_flight delivery survived purge, count=%d", c)
	}
	if c := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, msgID); c != 0 {
		t.Fatalf("orphaned message survived purge, count=%d", c)
	}

	// The purged counter must attribute one to the channel.
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := s.ChannelCounters(ctx, chID)
	if err != nil {
		t.Fatal(err)
	}
	if c.Purge != 1 {
		t.Fatalf("purged counter = %d, want 1", c.Purge)
	}
}

// keepChannelDelivery removes every fan-out delivery of msgID except the one
// on keepChannel, so one published message can drive a single channel's state
// in a multi-channel backlog test.
func keepChannelDelivery(ctx context.Context, t *testing.T, db *sql.DB, msgID, keepChannel int64) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		DELETE FROM novaque_deliveries WHERE message_id = ? AND channel_id != ?`,
		msgID, keepChannel); err != nil {
		t.Fatalf("prune sibling deliveries of message %d: %v", msgID, err)
	}
}
