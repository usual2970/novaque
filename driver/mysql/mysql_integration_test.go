//go:build integration

package mysql_test

import (
	"context"
	"testing"
	"time"

	"novaque/driver/mysql"
	"novaque/internal/testmysql"
	"novaque/store"
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

func TestPublishFanoutAndNoRetroactive(t *testing.T) {
	db := testmysql.Open(t)
	s := mysql.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "fanout_" + time.Now().Format("150405.000")
	if _, err := s.EnsureChannel(ctx, topic, "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, topic, "B"); err != nil {
		t.Fatal(err)
	}
	msgID, err := s.Publish(ctx, topic, []byte("hello"), store.PublishOpts{
		ExpiresAt:   time.Now().UTC().Add(time.Hour),
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if msgID == 0 {
		t.Fatal("expected message id")
	}

	a, err := s.Claim(ctx, topic, "A", "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Claim(ctx, topic, "B", "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("want 1 delivery each, got A=%d B=%d", len(a), len(b))
	}
	if string(a[0].Body) != "hello" || string(b[0].Body) != "hello" {
		t.Fatalf("body mismatch")
	}

	// Late channel should not see historical message.
	if _, err := s.EnsureChannel(ctx, topic, "late"); err != nil {
		t.Fatal(err)
	}
	late, err := s.Claim(ctx, topic, "late", "w1", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(late) != 0 {
		t.Fatalf("late channel should have 0 historical deliveries, got %d", len(late))
	}

	msgID2, err := s.Publish(ctx, topic, []byte("next"), store.PublishOpts{
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = msgID2
	late2, err := s.Claim(ctx, topic, "late", "w1", 10*time.Second, 10)
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
	if _, err := s.EnsureChannel(ctx, topic, "workers"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := s.Publish(ctx, topic, []byte{byte(i)}, store.PublishOpts{
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	c1, err := s.Claim(ctx, topic, "workers", "a", time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.Claim(ctx, topic, "workers", "b", time.Second, 2)
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

	// Short lease: do not ack first claim; reap then redeliver.
	d := c1[0]
	time.Sleep(1200 * time.Millisecond)
	if _, err := s.ReapExpiredLeases(ctx, 100); err != nil {
		t.Fatal(err)
	}
	again, err := s.Claim(ctx, topic, "workers", "c", 10*time.Second, 10)
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
	// Late ack with old token must fail.
	if err := s.Ack(ctx, d.ID, d.LeaseToken); err == nil {
		t.Fatal("expected stale ack to fail")
	}
}
