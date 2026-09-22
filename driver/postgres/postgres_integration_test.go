//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/usual2970/novaque/driver/postgres"
	"github.com/usual2970/novaque/internal/testpostgres"
	"github.com/usual2970/novaque/store"
)

// TestMigrateIdempotent runs Migrate twice in a row on the same database;
// every DDL statement is IF NOT EXISTS so the second run must be a no-op.
func TestMigrateIdempotent(t *testing.T) {
	db := testpostgres.Open(t)
	ctx := context.Background()
	s := postgres.New(db)

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

// TestMigrateCreatesTables verifies the five parity tables exist after Migrate.
func TestMigrateCreatesTables(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	rows, err := db.Query(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close tables query: %v", err)
	}
	sort.Strings(got)

	want := []string{
		"novaque_channels",
		"novaque_deliveries",
		"novaque_messages",
		"novaque_stats_daily",
		"novaque_topics",
	}
	if len(got) != len(want) {
		t.Fatalf("tables = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables = %v, want %v", got, want)
		}
	}
}

// TestMigrateCreatesIndexes verifies the claim/expires/stats indexes exist.
func TestMigrateCreatesIndexes(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	rows, err := db.Query(`
		SELECT indexname FROM pg_catalog.pg_indexes
		WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	got := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		got[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close indexes query: %v", err)
	}

	want := []string{
		"idx_novaque_deliveries_claim",
		"idx_novaque_messages_expires",
		"idx_novaque_stats_daily_channel",
		"idx_novaque_stats_daily_topic",
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("index %q missing; have %v", name, got)
		}
	}
}

// TestMigrateSessionUTC verifies sessions on the helper DSN run in UTC so the
// UTC day buckets match the MySQL driver.
func TestMigrateSessionUTC(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var tz string
	if err := db.QueryRow(`SHOW TIME ZONE`).Scan(&tz); err != nil {
		t.Fatalf("show time zone: %v", err)
	}
	if tz != "UTC" {
		t.Fatalf("session time zone = %q, want UTC", tz)
	}
}

// countRow scans a single BIGINT count, failing the test on any error.
func countRow(t *testing.T, ctx context.Context, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", q, err)
	}
	return n
}

// TestEnsureTopicChannelIdempotent ports the ensure half of the MySQL suite:
// repeated EnsureTopic / EnsureChannel calls resolve the same ids, and an
// invalid name is rejected before any SQL runs (same validateName rules).
func TestEnsureTopicChannelIdempotent(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "ensure_" + time.Now().Format("150405.000")

	t1, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if t1 != t2 {
		t.Fatalf("EnsureTopic id drift: %d vs %d", t1, t2)
	}

	c1, err := s.EnsureChannel(ctx, topic, "c1")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.EnsureChannel(ctx, topic, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatalf("EnsureChannel id drift: %d vs %d", c1, c2)
	}

	if _, err := s.EnsureTopic(ctx, "bad name"); err == nil {
		t.Fatal("EnsureTopic with invalid name: want error, got nil")
	}
	if _, err := s.EnsureChannel(ctx, topic, "bad/name"); err == nil {
		t.Fatal("EnsureChannel with invalid name: want error, got nil")
	}
}

// TestPublishFanout (AE1): one publish on a topic with two channels creates
// exactly one pending delivery per channel, bodies identical to the message.
// The fan-out is asserted directly against the deliveries table.
func TestPublishFanout(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
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

	if n := countRow(t, ctx, db,
		`SELECT COUNT(*) FROM novaque_deliveries WHERE message_id = $1`, msgID); n != 2 {
		t.Fatalf("want 2 deliveries for message, got %d", n)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT d.channel_id, d.status, d.attempts, d.max_attempts,
		       d.expires_at, m.body, m.expires_at
		FROM novaque_deliveries d
		INNER JOIN novaque_messages m ON m.id = d.message_id
		WHERE d.message_id = $1
		ORDER BY d.channel_id`, msgID)
	if err != nil {
		t.Fatal(err)
	}
	type fanRow struct {
		channelID       int64
		status          string
		attempts        int
		maxAttempts     int
		deliveryExpires int64
		body            []byte
		messageExpires  int64
	}
	var got []fanRow
	for rows.Next() {
		var r fanRow
		if err := rows.Scan(&r.channelID, &r.status, &r.attempts, &r.maxAttempts,
			&r.deliveryExpires, &r.body, &r.messageExpires); err != nil {
			t.Fatalf("scan delivery: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	wantChannels := []int64{aID, bID}
	if len(got) != 2 {
		t.Fatalf("want 2 delivery rows, got %d", len(got))
	}
	for i, r := range got {
		if r.channelID != wantChannels[i] {
			t.Fatalf("delivery %d channel = %d, want %d", i, r.channelID, wantChannels[i])
		}
		if r.status != store.StatusPending {
			t.Fatalf("channel %d status = %q, want pending", r.channelID, r.status)
		}
		if r.attempts != 0 {
			t.Fatalf("channel %d attempts = %d, want 0", r.channelID, r.attempts)
		}
		if r.maxAttempts != 5 {
			t.Fatalf("channel %d max_attempts = %d, want 5", r.channelID, r.maxAttempts)
		}
		if string(r.body) != "hello" {
			t.Fatalf("channel %d body = %q, want hello", r.channelID, r.body)
		}
		// Fan-out copies message expires_at onto the delivery (claim hot path).
		if r.deliveryExpires != r.messageExpires {
			t.Fatalf("channel %d delivery expires_at = %d, want message expires_at %d",
				r.channelID, r.deliveryExpires, r.messageExpires)
		}
	}
}

// TestPublishZeroChannels: a topic with no channels still inserts the message
// row and creates no deliveries.
func TestPublishZeroChannels(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "zerochan_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	msgID, err := s.Publish(ctx, topicID, []byte("lonely"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	var body []byte
	var mTopicID int64
	if err := db.QueryRowContext(ctx, `
		SELECT topic_id, body FROM novaque_messages WHERE id = $1`, msgID).
		Scan(&mTopicID, &body); err != nil {
		t.Fatalf("message row: %v", err)
	}
	if mTopicID != topicID || string(body) != "lonely" {
		t.Fatalf("message row = topic %d body %q", mTopicID, body)
	}
	if n := countRow(t, ctx, db,
		`SELECT COUNT(*) FROM novaque_deliveries WHERE message_id = $1`, msgID); n != 0 {
		t.Fatalf("want 0 deliveries, got %d", n)
	}
}

// TestPublishLateChannelNoHistory (AE4): a channel created after a publish
// must not see the old message; subsequent publishes fan out to it.
// Delivery membership is asserted with raw SQL.
func TestPublishLateChannelNoHistory(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "late_" + time.Now().Format("150405.000")
	aID, err := s.EnsureChannel(ctx, topic, "A")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	oldID, err := s.Publish(ctx, topicID, []byte("old"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	lateID, err := s.EnsureChannel(ctx, topic, "late")
	if err != nil {
		t.Fatal(err)
	}
	if n := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_deliveries
		WHERE channel_id = $1 AND message_id = $2`, lateID, oldID); n != 0 {
		t.Fatalf("late channel saw %d historical deliveries, want 0", n)
	}

	newID, err := s.Publish(ctx, topicID, []byte("next"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if n := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_deliveries
		WHERE channel_id = $1 AND message_id = $2`, lateID, newID); n != 1 {
		t.Fatalf("late channel got %d deliveries for new publish, want 1", n)
	}
	if n := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_deliveries
		WHERE channel_id = $1 AND message_id = $2`, aID, newID); n != 1 {
		t.Fatalf("existing channel got %d deliveries for new publish, want 1", n)
	}
}

// TestPublishDelayExceedsTTLRejected: delay validation runs inside the
// publish transaction before the message insert, so the rejected publish
// leaves no rows behind (mirrors the reject half of the MySQL delay test).
func TestPublishDelayExceedsTTLRejected(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "delayrej_" + time.Now().Format("150405.000")
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
	if n := countRow(t, ctx, db,
		`SELECT COUNT(*) FROM novaque_messages WHERE topic_id = $1`, topicID); n != 0 {
		t.Fatalf("rejected publish left %d message rows, want 0", n)
	}
}

// TestPublishInvalidTopicID rejects a non-positive topic id before the
// transaction starts; it must not map to store.ErrTopicGone.
func TestPublishInvalidTopicID(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := s.Publish(ctx, 0, []byte("x"), store.PublishOpts{TTL: time.Hour})
	if err == nil {
		t.Fatal("Publish with topic id 0: want error, got nil")
	}
	if errors.Is(err, store.ErrTopicGone) {
		t.Fatalf("topic id 0 mapped to ErrTopicGone: %v", err)
	}
}

// TestPublishDeletedTopicErrTopicGone seeds the self-heal flow at the driver
// level: publishing to a topic id whose row was deleted (raw SQL, bypassing
// the driver) fails the topic FK with PostgreSQL SQLSTATE 23503
// (foreign_key_violation), which Publish maps to the dialect-agnostic
// store.ErrTopicGone sentinel by SQLSTATE code — never by string matching.
// Re-ensuring the name yields a fresh id the same publish succeeds against.
func TestPublishDeletedTopicErrTopicGone(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	name := "gone_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	// Delete the topic out from under the resolved id with raw SQL so the
	// driver sees only the vanished parent. The topic has no channels or
	// messages, so the direct delete is unblocked; message deliveries cascade.
	if _, err := db.ExecContext(ctx, `DELETE FROM novaque_topics WHERE id = $1`, topicID); err != nil {
		t.Fatalf("raw delete topic: %v", err)
	}

	_, err = s.Publish(ctx, topicID, []byte("too late"), store.PublishOpts{TTL: time.Hour})
	if !errors.Is(err, store.ErrTopicGone) {
		t.Fatalf("publish to deleted topic = %v, want store.ErrTopicGone", err)
	}

	// Self-heal seed: re-ensure resolves a new id and the retry publish lands.
	newID, err := s.EnsureTopic(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if newID == topicID {
		t.Fatalf("re-ensure returned the deleted id %d", newID)
	}
	if _, err := s.Publish(ctx, newID, []byte("healed"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatalf("retry publish on re-ensured topic: %v", err)
	}
}

// TestClaimCompetePartition (AE2): two distinguishable consumers repeatedly
// Claim on one channel; the N messages must partition with no overlap (union
// size N, intersection empty), and every delivery must be ackable exactly once.
func TestClaimCompetePartition(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
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
	const n = 6
	for i := 0; i < n; i++ {
		if _, err := s.Publish(ctx, topicID, []byte{byte(i)}, store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}

	var a, b []store.Delivery
	// Two consumers alternate claims until the channel drains completely,
	// so each claims repeatedly rather than just once.
	turn := 0
	for {
		owner := "consumer-a"
		if turn%2 == 1 {
			owner = "consumer-b"
		}
		got, err := s.Claim(ctx, chID, owner, 30*time.Second, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			break
		}
		if turn%2 == 0 {
			a = append(a, got...)
		}
		if turn%2 == 1 {
			b = append(b, got...)
		}
		turn++
		if turn > 2*n {
			t.Fatal("claim loop did not drain")
		}
	}

	setA := map[int64]store.Delivery{}
	for _, d := range a {
		setA[d.ID] = d
	}
	setB := map[int64]store.Delivery{}
	for _, d := range b {
		setB[d.ID] = d
	}
	union := map[int64]bool{}
	for id := range setA {
		union[id] = true
	}
	for id := range setB {
		union[id] = true
	}
	if len(union) != n {
		t.Fatalf("union size = %d, want %d (A=%d B=%d)", len(union), n, len(a), len(b))
	}
	for id := range setA {
		if _, dup := setB[id]; dup {
			t.Fatalf("delivery %d claimed by both consumers", id)
		}
	}
	if len(a) == 0 || len(b) == 0 {
		t.Fatalf("expected both consumers to lease work, got A=%d B=%d", len(a), len(b))
	}

	// Every delivery is ackable exactly once with its own token.
	for _, dls := range [][]store.Delivery{a, b} {
		for _, d := range dls {
			if err := s.Ack(ctx, d.ID, d.LeaseToken); err != nil {
				t.Fatalf("ack delivery %d: %v", d.ID, err)
			}
			if err := s.Ack(ctx, d.ID, d.LeaseToken); err == nil {
				t.Fatalf("delivery %d acked twice", d.ID)
			}
		}
	}
	if left := countRow(t, ctx, db,
		`SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = $1`, chID); left != 0 {
		t.Fatalf("want 0 deliveries after acks, got %d", left)
	}
}

// TestClaimAckRequeueWrongToken: Ack and Requeue with a bogus lease token are
// rejected, and the delivery row is unchanged (still in_flight under the
// original lease); the genuine token then fences correctly.
func TestClaimAckRequeueWrongToken(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "wrongtok_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("x"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 claim, got %d", len(got))
	}
	d := got[0]
	const bogus = "0123456789abcdef0123456789abcdef"

	if err := s.Ack(ctx, d.ID, bogus); err == nil {
		t.Fatal("Ack with bogus token: want error, got nil")
	}
	if err := s.Requeue(ctx, d.ID, bogus, time.Time{}); err == nil {
		t.Fatal("Requeue with bogus token: want error, got nil")
	}

	// Row unchanged: still in_flight with the original lease coordinates.
	var status, token, owner string
	var attempts int
	var leaseUntil sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, attempts, lease_token, lease_owner, lease_until
		FROM novaque_deliveries WHERE id = $1`, d.ID).
		Scan(&status, &attempts, &token, &owner, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if status != store.StatusInFlight || token != d.LeaseToken || owner != "w1" || attempts != 1 {
		t.Fatalf("row mutated by rejected calls: status=%s attempts=%d token=%s owner=%s",
			status, attempts, token, owner)
	}
	if !leaseUntil.Valid || leaseUntil.Int64 <= time.Now().Unix() {
		t.Fatalf("lease_until not in the future: %#v", leaseUntil)
	}

	// Genuine requeue works and makes the delivery claimable again.
	if err := s.Requeue(ctx, d.ID, d.LeaseToken, time.Time{}); err != nil {
		t.Fatalf("Requeue with real token: %v", err)
	}
	again, err := s.Claim(ctx, chID, "w2", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].ID != d.ID {
		t.Fatalf("expected reclaim of %d, got %#v", d.ID, again)
	}
	if again[0].Attempts != 2 {
		t.Fatalf("want attempts=2 after requeue, got %d", again[0].Attempts)
	}
	if err := s.Ack(ctx, again[0].ID, again[0].LeaseToken); err != nil {
		t.Fatalf("final ack: %v", err)
	}
}

// TestRequeueFlows covers the handler-error flow: an immediate requeue
// (zero time) resets available_at to now, and a delayed requeue stamps the
// given time which Claim honors until it passes.
func TestRequeueFlows(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "rqflows_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Immediate requeue after a handler error.
	if _, err := s.Publish(ctx, topicID, []byte("one"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	d1, err := s.Claim(ctx, chID, "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(d1) != 1 {
		t.Fatalf("want 1 claim, got %d", len(d1))
	}
	if err := s.Requeue(ctx, d1[0].ID, d1[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	var avail int64
	var leaseTok sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT available_at, lease_token FROM novaque_deliveries WHERE id = $1`, d1[0].ID).
		Scan(&avail, &leaseTok); err != nil {
		t.Fatal(err)
	}
	if avail < time.Now().Unix()-1 || avail > time.Now().Unix()+1 {
		t.Fatalf("immediate requeue available_at = %d, want ~now", avail)
	}
	if leaseTok.Valid {
		t.Fatal("immediate requeue did not clear lease_token")
	}
	back, err := s.Claim(ctx, chID, "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 || back[0].ID != d1[0].ID {
		t.Fatalf("expected immediate reclaim, got %#v", back)
	}

	// Delayed requeue: delivery hidden until the given available time.
	if _, err := s.Publish(ctx, topicID, []byte("two"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	d2, err := s.Claim(ctx, chID, "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(d2) != 1 {
		t.Fatalf("want 1 claim, got %d", len(d2))
	}
	wakeAt := time.Now().Add(2 * time.Second)
	if err := s.Requeue(ctx, d2[0].ID, d2[0].LeaseToken, wakeAt); err != nil {
		t.Fatal(err)
	}
	early, err := s.Claim(ctx, chID, "w2", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(early) != 0 {
		t.Fatalf("delayed requeue visible early: %#v", early)
	}
	time.Sleep(2500 * time.Millisecond)
	ready, err := s.Claim(ctx, chID, "w2", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range ready {
		if x.ID == d2[0].ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("delivery %d not claimable after delayed available_at (got %#v)", d2[0].ID, ready)
	}
}

// TestClaimLeaseExpiryNoReaper (AE3): Claim with a tiny lease, wait for
// lease_until to pass. While nothing reaps the expired lease the row stays
// in_flight, so a fresh Claim must NOT redeliver it (the poll filters on
// status=pending). Because Ack fences only on token + in_flight status
// (never lease_until — matching driver/mysql), the OLD token still acks.
// The full AE3 redelivery flow (ReapExpiredLeases -> reclaim -> stale
// token rejected) is covered by the reaper tests.
func TestClaimLeaseExpiryNoReaper(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "leaseexp_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("x"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, chID, "w1", time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 claim, got %d", len(got))
	}
	d := got[0]

	time.Sleep(2500 * time.Millisecond)

	// Reaper absent: row still in_flight and Claim will not touch it.
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status FROM novaque_deliveries WHERE id = $1`, d.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != store.StatusInFlight {
		t.Fatalf("status = %s, want in_flight without reaping", status)
	}
	again, err := s.Claim(ctx, chID, "w2", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range again {
		if x.ID == d.ID {
			t.Fatal("fresh Claim redelivered an in_flight row without the reaper")
		}
	}

	// Ack ignores lease_until: the old token still fences while status is
	// in_flight (identical to driver/mysql Ack). The stale-ack-rejected
	// assertion happens after reaping, in the reaper tests.
	if err := s.Ack(ctx, d.ID, d.LeaseToken); err != nil {
		t.Fatalf("old-token ack after lease expiry: %v", err)
	}
}

// TestClaimPoisonToDead (edge supplement): when a delivery's attempts exceeds
// max_attempts on a claim, the claim flips it to dead with lease fields nulled
// and it never reaches the consumer.
func TestClaimPoisonToDead(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "poison_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("x"), store.PublishOpts{
		TTL:         time.Hour,
		MaxAttempts: 1,
	}); err != nil {
		t.Fatal(err)
	}

	first, err := s.Claim(ctx, chID, "w1", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Attempts != 1 {
		t.Fatalf("first claim = %#v", first)
	}
	if err := s.Requeue(ctx, first[0].ID, first[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Second claim leases attempts=2 > max=1: poison. No delivery returned;
	// the row is dead with nulled lease fields.
	second, err := s.Claim(ctx, chID, "w2", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("poison delivery handed to consumer: %#v", second)
	}
	var status string
	var leaseOwner, leaseToken sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner, lease_token, lease_until
		FROM novaque_deliveries WHERE id = $1`, first[0].ID).
		Scan(&status, &leaseOwner, &leaseToken, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if status != store.StatusDead {
		t.Fatalf("status = %s, want dead", status)
	}
	if leaseOwner.Valid || leaseToken.Valid || leaseUntil.Valid {
		t.Fatal("poison row kept lease fields")
	}
}

// TestClaimConcurrentHammer (AE5, Execution note): two separate *sql.DB pools
// and many goroutines hammer concurrent Claim on ONE channel (limit 3) until
// all N=200 messages are leased. Against a single real PostgreSQL database,
// FOR UPDATE SKIP LOCKED must guarantee no delivery is leased twice.
func TestClaimConcurrentHammer(t *testing.T) {
	dsn := testpostgres.DSN(t)
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "hammer_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		if _, err := s.Publish(ctx, topicID, []byte{byte(i % 256)}, store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}

	// Two independent pools, both pointed at the one database.
	pool1, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool2, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool1.SetMaxOpenConns(8)
	pool2.SetMaxOpenConns(8)
	t.Cleanup(func() {
		_ = pool1.Close()
		_ = pool2.Close()
	})

	var mu sync.Mutex
	leased := make([]store.Delivery, 0, n)
	var claimErr error
	var leasedCount int64
	var wg sync.WaitGroup

	hammer := func(storeOnPool *postgres.Store, owner string) {
		defer wg.Done()
		for atomic.LoadInt64(&leasedCount) < int64(n) {
			got, err := storeOnPool.Claim(ctx, chID, owner, 30*time.Second, 3)
			if err != nil {
				mu.Lock()
				if claimErr == nil {
					claimErr = err
				}
				mu.Unlock()
				return
			}
			if len(got) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			mu.Lock()
			leased = append(leased, got...)
			mu.Unlock()
			atomic.AddInt64(&leasedCount, int64(len(got)))
		}
	}

	s1 := postgres.New(pool1)
	s2 := postgres.New(pool2)
	const workersPerPool = 8
	for i := 0; i < workersPerPool; i++ {
		wg.Add(2)
		go hammer(s1, "pool1-w")
		go hammer(s2, "pool2-w")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("hammer timed out; leased %d of %d", atomic.LoadInt64(&leasedCount), n)
	}
	if claimErr != nil {
		t.Fatalf("concurrent Claim error: %v", claimErr)
	}

	// No delivery leased twice: unique delivery ids == N.
	ids := make(map[int64]bool, len(leased))
	for _, d := range leased {
		if ids[d.ID] {
			t.Fatalf("delivery %d leased twice", d.ID)
		}
		ids[d.ID] = true
	}
	if len(ids) != n {
		t.Fatalf("unique leased = %d, want %d (total claims %d)", len(ids), n, len(leased))
	}

	// Exact in_flight accounting: every leased row in_flight with attempts=1;
	// no pending or dead leftovers (each row leased exactly once, well under
	// max_attempts, so nothing could go dead).
	statusRows, err := db.QueryContext(ctx, `
		SELECT status, COUNT(*) FROM novaque_deliveries WHERE channel_id = $1 GROUP BY status`, chID)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for statusRows.Next() {
		var st string
		var c int64
		if err := statusRows.Scan(&st, &c); err != nil {
			t.Fatal(err)
		}
		counts[st] = c
	}
	if err := statusRows.Close(); err != nil {
		t.Fatal(err)
	}
	if counts[store.StatusInFlight] != n {
		t.Fatalf("in_flight = %d, want %d (counts=%v)", counts[store.StatusInFlight], n, counts)
	}
	if counts[store.StatusPending] != 0 {
		t.Fatalf("pending leftovers = %d, want 0", counts[store.StatusPending])
	}
	if counts[store.StatusDead] != 0 {
		t.Fatalf("dead leftovers = %d, want 0", counts[store.StatusDead])
	}
	var multiAttempt int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM novaque_deliveries
		WHERE channel_id = $1 AND attempts <> 1`, chID).Scan(&multiAttempt); err != nil {
		t.Fatal(err)
	}
	if multiAttempt != 0 {
		t.Fatalf("%d rows with attempts != 1", multiAttempt)
	}

	// Every leased delivery acks exactly once under real contention.
	var ackFail int64
	for _, d := range leased {
		if err := s.Ack(ctx, d.ID, d.LeaseToken); err != nil {
			ackFail++
		}
	}
	if ackFail != 0 {
		t.Fatalf("%d acks rejected after hammer", ackFail)
	}
	if left := countRow(t, ctx, db,
		`SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = $1`, chID); left != 0 {
		t.Fatalf("want 0 deliveries after acks, got %d", left)
	}
}

// TestReapExpiredLeasesRedelivery (AE3, full): Claim with a 1s lease, wait
// past lease_until, then ReapExpiredLeases resets the row to pending with all
// lease fields cleared (and does not bump attempts). The ORIGINAL stale token
// can no longer ack or requeue (both mutations fence on status=in_flight plus
// token; the row is pending); a fresh Claim redelivers the same message with
// attempts incremented and the NEW token works. Reap itself records no stats
// (reap != requeue), mirroring driver/mysql/maintenance.go.
func TestReapExpiredLeasesRedelivery(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "reapredeliver_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
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
	got, err := s.Claim(ctx, chID, "w1", time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 claim, got %d", len(got))
	}
	d := got[0]
	if d.Attempts != 1 {
		t.Fatalf("first claim attempts = %d, want 1", d.Attempts)
	}
	staleToken := d.LeaseToken

	// 2.5s for a 1s lease: DB-second flooring means a shorter sleep can land
	// on the same DB second as lease_until (same margin as driver/mysql).
	time.Sleep(2500 * time.Millisecond)

	n, err := s.ReapExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n < 1 {
		t.Fatalf("reap affected %d rows, want >= 1", n)
	}

	// Back to pending with lease fields cleared; attempts unchanged.
	var status string
	var attempts int
	var availableAt int64
	var leaseOwner, leaseToken sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, attempts, available_at, lease_owner, lease_token, lease_until
		FROM novaque_deliveries WHERE id = $1`, d.ID).
		Scan(&status, &attempts, &availableAt, &leaseOwner, &leaseToken, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if status != store.StatusPending {
		t.Fatalf("status = %s, want pending", status)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (reap does not bump attempts)", attempts)
	}
	if availableAt > time.Now().Unix() {
		t.Fatalf("available_at = %d, want <= now", availableAt)
	}
	if leaseOwner.Valid || leaseToken.Valid || leaseUntil.Valid {
		t.Fatalf("lease fields not cleared: owner=%#v token=%#v until=%#v",
			leaseOwner, leaseToken, leaseUntil)
	}

	// The ORIGINAL stale token can no longer drive ack/requeue.
	if err := s.Ack(ctx, d.ID, staleToken); err == nil {
		t.Fatal("stale-token ack after reap: want error, got nil")
	}
	if err := s.Requeue(ctx, d.ID, staleToken, time.Time{}); err == nil {
		t.Fatal("stale-token requeue after reap: want error, got nil")
	}

	// Redelivery: same message, attempts incremented, a fresh token.
	again, err := s.Claim(ctx, chID, "w2", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("redelivery claim got %d rows, want 1", len(again))
	}
	if again[0].MessageID != d.MessageID {
		t.Fatalf("redelivered message %d, want original %d", again[0].MessageID, d.MessageID)
	}
	if again[0].Attempts != 2 {
		t.Fatalf("redelivery attempts = %d, want 2", again[0].Attempts)
	}
	if again[0].LeaseToken == "" || again[0].LeaseToken == staleToken {
		t.Fatalf("redelivery token not refreshed: %q", again[0].LeaseToken)
	}
	// The NEW token works.
	if err := s.Ack(ctx, again[0].ID, again[0].LeaseToken); err != nil {
		t.Fatalf("new-token ack: %v", err)
	}
}

// TestReapExpiredLeasesLimit (limit behavior): five expired leases reaped
// with limit 2 — per call at most 2 of this channel's rows reset, and the
// remainder survives as in_flight between calls until drained. Counts are
// scoped to this channel, so the reaper's global reach over the shared test
// database cannot perturb them.
func TestReapExpiredLeasesLimit(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "reaplimit_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	const total = 5
	for i := 0; i < total; i++ {
		if _, err := s.Publish(ctx, topicID, []byte{byte(i)}, store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Claim(ctx, chID, "w1", time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != total {
		t.Fatalf("want %d leased, got %d", total, len(got))
	}
	time.Sleep(2500 * time.Millisecond)

	pending := int64(0)
	for rounds := 0; ; rounds++ {
		if _, err := s.ReapExpiredLeases(ctx, 2); err != nil {
			t.Fatalf("reap round %d: %v", rounds, err)
		}
		newPending := countRow(t, ctx, db, `
			SELECT COUNT(*) FROM novaque_deliveries
			WHERE channel_id = $1 AND status = $2`, chID, store.StatusPending)
		inFlight := countRow(t, ctx, db, `
			SELECT COUNT(*) FROM novaque_deliveries
			WHERE channel_id = $1 AND status = $2`, chID, store.StatusInFlight)
		reaped := newPending - pending
		if reaped < 0 || reaped > 2 {
			t.Fatalf("round %d reset %d of this channel's rows, want 0..2", rounds, reaped)
		}
		if rounds == 0 {
			// First batch is capped: at most 2 reset, at least 3 survive.
			if newPending > 2 {
				t.Fatalf("first reap reset %d rows, limit is 2", newPending)
			}
			if inFlight < 3 {
				t.Fatalf("remainder after first reap = %d, want >= 3", inFlight)
			}
		}
		pending = newPending
		if inFlight == 0 {
			break
		}
		if rounds > 10 {
			t.Fatal("reap did not drain within 11 rounds")
		}
	}
	if pending != total {
		t.Fatalf("reaped %d rows total, want %d", pending, total)
	}
}

// TestPurgeExpiredTTL (AE6): an expired delivery (TTL 1s) is removed by
// PurgeExpired and afterward is NOT claimable; in the same run a still-valid
// delivery survives and is claimable. The expired message row is reaped by
// the orphan-message pass once its delivery is gone; the valid message stays
// with its delivery.
func TestPurgeExpiredTTL(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "purgettl_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
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
	if _, err := s.Publish(ctx, topicID, []byte("valid"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}

	// 2.6s for a 1s TTL: expires_at floors to publish-second+1, so the DB
	// clock needs a full extra second of margin to pass it strictly.
	time.Sleep(2600 * time.Millisecond)

	n, err := s.PurgeExpired(ctx, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n < 1 {
		t.Fatalf("purge removed %d rows, want >= 1", n)
	}

	// Only the still-valid delivery is claimable.
	got, err := s.Claim(ctx, chID, "w", 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Body) != "valid" {
		t.Fatalf("claim after purge = %#v, want one 'valid' delivery", got)
	}

	// One delivery left (the valid one); the expired message row was removed
	// by the orphan pass, the valid message survives with its delivery.
	if left := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = $1`, chID); left != 1 {
		t.Fatalf("deliveries after purge = %d, want 1", left)
	}
	if msgs := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_messages WHERE topic_id = $1`, topicID); msgs != 1 {
		t.Fatalf("messages after purge = %d, want 1", msgs)
	}
	if err := s.Ack(ctx, got[0].ID, got[0].LeaseToken); err != nil {
		t.Fatalf("final ack: %v", err)
	}
}

// TestPurgeExpiredLimit (limit behavior): five expired deliveries purged
// with limit 2 — per call at most 2 of this channel's rows are deleted and
// the remainder survives between calls until drained. Each delivery's
// message becomes an orphan and is removed by the same call's orphan pass,
// so message and delivery counts for this topic stay in lockstep. Counts are
// scoped to this channel/topic.
func TestPurgeExpiredLimit(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "purgelimit_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	const total = 5
	for i := 0; i < total; i++ {
		if _, err := s.Publish(ctx, topicID, []byte{byte(i)}, store.PublishOpts{TTL: time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2600 * time.Millisecond)

	remaining := int64(total)
	for rounds := 0; ; rounds++ {
		if _, err := s.PurgeExpired(ctx, 2); err != nil {
			t.Fatalf("purge round %d: %v", rounds, err)
		}
		left := countRow(t, ctx, db, `
			SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = $1`, chID)
		msgs := countRow(t, ctx, db, `
			SELECT COUNT(*) FROM novaque_messages WHERE topic_id = $1`, topicID)
		gone := remaining - left
		if gone < 0 || gone > 2 {
			t.Fatalf("round %d deleted %d of this channel's rows, want 0..2", rounds, gone)
		}
		if msgs != left {
			t.Fatalf("round %d: messages = %d but deliveries = %d (orphan pass mismatch)",
				rounds, msgs, left)
		}
		if rounds == 0 {
			// First batch capped: at most 2 deleted, at least 3 survive.
			if left < 3 {
				t.Fatalf("remainder after first purge = %d, want >= 3", left)
			}
		}
		remaining = left
		if left == 0 {
			break
		}
		if rounds > 10 {
			t.Fatal("purge did not drain within 11 rounds")
		}
	}
}

// TestMaintenanceEdgeDefaultsAndErrors covers happy/edge/error paths: a
// non-positive limit defaults without error while this channel's live row is
// untouched (no expired lease, no expired TTL), and both methods surface an
// error when the database handle is closed.
func TestMaintenanceEdgeDefaultsAndErrors(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "maintedge_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "c")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("live"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}

	// Default-limit path; this channel owns no qualifying row. The reaper is
	// global, so the fate of this channel's single row is pinned directly
	// rather than asserting a global zero count.
	if _, err := s.ReapExpiredLeases(ctx, 0); err != nil {
		t.Fatalf("reap with default limit on clean channel: %v", err)
	}
	if left := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_deliveries
		WHERE channel_id = $1 AND status = $2`, chID, store.StatusPending); left != 1 {
		t.Fatalf("reap touched the live row; pending left = %d, want 1", left)
	}
	if _, err := s.PurgeExpired(ctx, -1); err != nil {
		t.Fatalf("purge with default limit on clean channel: %v", err)
	}
	if left := countRow(t, ctx, db, `
		SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = $1`, chID); left != 1 {
		t.Fatalf("purge touched the live row; left = %d, want 1", left)
	}

	// Error path: closed handle.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReapExpiredLeases(ctx, 10); err == nil {
		t.Fatal("reap on closed db: want error, got nil")
	}
	if _, err := s.PurgeExpired(ctx, 10); err == nil {
		t.Fatal("purge on closed db: want error, got nil")
	}
}
