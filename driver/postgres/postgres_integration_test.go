//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"sort"
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
// Claim/Ack arrive in a later unit, so the fan-out is asserted directly
// against the deliveries table.
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
// must not see the old message; subsequent publishes fan out to it. Claim
// arrives in a later unit, so delivery membership is asserted with raw SQL.
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
