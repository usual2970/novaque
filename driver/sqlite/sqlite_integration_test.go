//go:build integration

package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	sqlite3 "modernc.org/sqlite"

	"github.com/usual2970/novaque/driver/sqlite"
	"github.com/usual2970/novaque/internal/testsqlite"
	"github.com/usual2970/novaque/store"
)

func TestMigrateIdempotentAndUniqueChannel(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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

func TestPublishNoDelayStillImmediate(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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

// TestPublishDeletedTopicErrTopicGone seeds the KTD9 self-heal flow at the
// driver level: publishing to a topic id whose row was deleted (raw SQL,
// bypassing the driver) fails the topic FK with SQLITE_CONSTRAINT_FOREIGNKEY
// (787), which Publish maps to the dialect-agnostic store.ErrTopicGone
// sentinel by extended error code — never by string matching. Re-ensuring
// the name yields a fresh id the same publish succeeds against (the Client
// evict-retry of U3 builds on exactly this).
func TestPublishDeletedTopicErrTopicGone(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	// driver sees only the vanished parent.
	killTopicRaw(t, ctx, db, topicID)

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

// killTopicRaw deletes a topic and its children with raw SQL in child-first
// order, bypassing the driver entirely (used to stage FK-violation and
// idempotency scenarios). The channels FK has no ON DELETE action, so the
// channels (and their messages/stats) must be removed before the topic.
func killTopicRaw(t *testing.T, ctx context.Context, db *sql.DB, topicID int64) {
	t.Helper()
	for _, q := range []string{
		`DELETE FROM novaque_messages WHERE topic_id = ?`,
		`DELETE FROM novaque_channels WHERE topic_id = ?`,
		`DELETE FROM novaque_stats_daily WHERE topic_id = ?`,
		`DELETE FROM novaque_topics WHERE id = ?`,
	} {
		if _, err := db.ExecContext(ctx, q, topicID); err != nil {
			t.Fatalf("raw delete %q: %v", q, err)
		}
	}
}

func TestClaimCompeteAndLeaseRedelivery(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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
	// U4 ships ReapExpiredLeases; until then drive the identical reap with
	// raw SQL so the lease-redelivery path is still exercised end to end.
	reapExpiredLeasesRaw(t, ctx, db, 100)
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
	db := testsqlite.Open(t)
	s := sqlite.New(db)
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

// TestClaimConcurrentNoDuplicateLease hammers Claim from 8 goroutines on one
// shared pool (AE5 best-effort). SQLite has no row locks: concurrent claim
// transactions take snapshots, and a transaction whose snapshot went stale
// before its first write fails SQLITE_BUSY_SNAPSHOT (517), which busy_timeout
// does not cover — Claim itself retries the whole transaction. Any busy error
// that escapes Claim is retried here too; a double lease must never happen.
func TestClaimConcurrentNoDuplicateLease(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "hammer_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	const total = 200
	for i := 0; i < total; i++ {
		if _, err := s.Publish(ctx, topicID, []byte{byte(i >> 8), byte(i)}, store.PublishOpts{TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu     sync.Mutex
		leased = make(map[int64]int)
	)
	errCh := make(chan error, 8)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", g)
			backoff := time.Millisecond
			for {
				got, err := s.Claim(ctx, chID, owner, 30*time.Second, 10)
				if err != nil {
					if isClaimBusy(err) {
						time.Sleep(backoff)
						if backoff < 50*time.Millisecond {
							backoff *= 2
						}
						continue
					}
					errCh <- err
					return
				}
				if len(got) == 0 {
					return
				}
				backoff = time.Millisecond
				mu.Lock()
				for _, d := range got {
					leased[d.ID]++
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	for id, n := range leased {
		if n > 1 {
			t.Errorf("delivery %d leased %d times, want at most 1", id, n)
		}
	}
	if len(leased) != total {
		t.Fatalf("claimed %d unique deliveries, want %d", len(leased), total)
	}
}

// isClaimBusy reports whether err is a SQLite lock/busy code: SQLITE_BUSY (5)
// or the extended SQLITE_BUSY_SNAPSHOT (517). The test retries those, since
// they are expected under snapshot contention rather than correctness errors.
func isClaimBusy(err error) bool {
	var e *sqlite3.Error
	if errors.As(err, &e) {
		return e.Code() == 5 || e.Code() == 517
	}
	return false
}

// TestMaintenanceReapExpiredLeases covers the lease half of maintenance: an
// in_flight delivery whose lease_until is past is reset to a claimable pending
// row (lease fields cleared, available_at <= now), reclaiming redelivers it,
// and the stale lease token can no longer ack. The reap itself records no
// stats (that is asserted at the stats layer in U5, like MySQL's suite).
func TestMaintenanceReapExpiredLeases(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "reap_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "workers")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	msgID, err := s.Publish(ctx, topicID, []byte("job"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, chID, "w1", time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("claim got %d deliveries, want 1", len(first))
	}
	d := first[0]

	time.Sleep(2500 * time.Millisecond)

	n, err := s.ReapExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if n < 1 {
		t.Fatalf("reap affected %d rows, want >= 1", n)
	}

	// Back to a claimable pending row with every lease field cleared.
	var ready int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM novaque_deliveries
		WHERE id = ? AND status = ?
		  AND available_at <= unixepoch()
		  AND lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL`,
		d.ID, store.StatusPending).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	if ready != 1 {
		t.Fatalf("reaped delivery not a clean pending row (count=%d)", ready)
	}

	again, err := s.Claim(ctx, chID, "w2", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].MessageID != msgID {
		t.Fatalf("expected redelivery of message %d, got %#v", msgID, again)
	}

	if err := s.Ack(ctx, d.ID, d.LeaseToken); err == nil {
		t.Fatal("expected stale ack after reap to fail")
	}
}

// TestMaintenancePurgeExpired covers AE6 with raw-SQL past-expiry staging:
// pending and dead expired deliveries disappear, cannot be claimed, and their
// now-orphaned messages go on the same call's orphan pass; an unexpired
// control survives; a seed orphan expired message is collected; an expired
// message whose delivery is still unexpired survives (delivery state guard)
// until its delivery is expired and the next purge takes both.
func TestMaintenancePurgeExpired(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ts := time.Now().Format("150405.000")
	aID, err := s.EnsureChannel(ctx, "purgeA_"+ts, "A")
	if err != nil {
		t.Fatal(err)
	}
	bID, err := s.EnsureChannel(ctx, "purgeB_"+ts, "B")
	if err != nil {
		t.Fatal(err)
	}
	topicAID, err := s.EnsureTopic(ctx, "purgeA_"+ts)
	if err != nil {
		t.Fatal(err)
	}
	topicBID, err := s.EnsureTopic(ctx, "purgeB_"+ts)
	if err != nil {
		t.Fatal(err)
	}

	// Three doomed deliveries on A: two pending, one dead.
	pend1, err := publishDeliveryIDs(ctx, s, db, topicAID, aID, []byte("p1"))
	if err != nil {
		t.Fatal(err)
	}
	pend2, err := publishDeliveryIDs(ctx, s, db, topicAID, aID, []byte("p2"))
	if err != nil {
		t.Fatal(err)
	}
	dead1, err := publishDeliveryIDs(ctx, s, db, topicAID, aID, []byte("d1"))
	if err != nil {
		t.Fatal(err)
	}
	// Unexpired control on A.
	ctrl, err := publishDeliveryIDs(ctx, s, db, topicAID, aID, []byte("ctrl"))
	if err != nil {
		t.Fatal(err)
	}
	// Expired message with an unexpired delivery on B (state-guard pair).
	guarded, err := publishDeliveryIDs(ctx, s, db, topicBID, bID, []byte("guard"))
	if err != nil {
		t.Fatal(err)
	}

	// Stage dead status, then expire the doomed rows (message + delivery)
	// strictly into the past with raw SQL.
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_deliveries SET status = ? WHERE id = ?`,
		store.StatusDead, dead1.deliveryID); err != nil {
		t.Fatal(err)
	}
	for _, x := range []pubPair{pend1, pend2, dead1} {
		if _, err := db.ExecContext(ctx, `
			UPDATE novaque_messages SET expires_at = unixepoch() - 10 WHERE id = ?`,
			x.messageID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE novaque_deliveries SET expires_at = unixepoch() - 10 WHERE id = ?`,
			x.deliveryID); err != nil {
			t.Fatal(err)
		}
	}

	// The guarded pair: message expired, delivery not.
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_messages SET expires_at = unixepoch() - 10 WHERE id = ?`,
		guarded.messageID); err != nil {
		t.Fatal(err)
	}

	// Seed orphan: expired message with no deliveries at all.
	var orphanID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO novaque_messages (topic_id, body, expires_at)
		VALUES (?, ?, unixepoch() - 10) RETURNING id`,
		topicAID, []byte("orphan")).Scan(&orphanID); err != nil {
		t.Fatal(err)
	}

	n, err := s.PurgeExpired(ctx, 100)
	if err != nil {
		t.Fatalf("PurgeExpired: %v", err)
	}
	// 3 expired deliveries + seed orphan + the 3 messages orphaned by the
	// delivery delete (orphan pass runs after it in the same call) = 7.
	if n != 7 {
		t.Fatalf("purge affected %d rows, want 7", n)
	}

	// Doomed deliveries and their messages are gone; the seed orphan is gone.
	for _, x := range []pubPair{pend1, pend2, dead1} {
		if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, x.deliveryID); n != 0 {
			t.Fatalf("expired delivery %d survived purge, count=%d", x.deliveryID, n)
		}
		if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, x.messageID); n != 0 {
			t.Fatalf("orphaned message %d survived purge, count=%d", x.messageID, n)
		}
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, orphanID); n != 0 {
		t.Fatalf("seed orphan message %d survived purge, count=%d", orphanID, n)
	}

	// The purged deliveries are not claimable: A claims only the control.
	gotA, err := s.Claim(ctx, aID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotA) != 1 || gotA[0].ID != ctrl.deliveryID {
		t.Fatalf("claim on A after purge = %#v, want only control %d", gotA, ctrl.deliveryID)
	}

	// The guarded pair on B survives: expired message, unexpired delivery.
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, guarded.messageID); n != 1 {
		t.Fatalf("guarded message count=%d, want 1", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, guarded.deliveryID); n != 1 {
		t.Fatalf("guarded delivery count=%d, want 1", n)
	}

	// Once the delivery is expired too, the next purge takes both: the
	// delivery delete orphans the message and the same call's orphan pass
	// collects it.
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_deliveries SET expires_at = unixepoch() - 10
		WHERE id = ? AND channel_id = ?`, guarded.deliveryID, bID); err != nil {
		t.Fatal(err)
	}
	n2, err := s.PurgeExpired(ctx, 100)
	if err != nil {
		t.Fatalf("second PurgeExpired: %v", err)
	}
	if n2 != 2 {
		t.Fatalf("second purge affected %d rows, want 2", n2)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, guarded.messageID); n != 0 {
		t.Fatalf("guarded message survived second purge, count=%d", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, guarded.deliveryID); n != 0 {
		t.Fatalf("guarded delivery survived second purge, count=%d", n)
	}

	// The unexpired control still exists (claimed above, in_flight).
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, ctrl.messageID); n != 1 {
		t.Fatalf("control message count=%d, want 1", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, ctrl.deliveryID); n != 1 {
		t.Fatalf("control delivery count=%d, want 1", n)
	}
}

type pubPair struct {
	messageID  int64
	deliveryID int64
}

// publishDeliveryIDs publishes one message (1h TTL) and returns its message
// id and the delivery id created for channelID.
func publishDeliveryIDs(ctx context.Context, s *sqlite.Store, db *sql.DB, topicID, channelID int64, body []byte) (pubPair, error) {
	msgID, err := s.Publish(ctx, topicID, body, store.PublishOpts{TTL: time.Hour})
	if err != nil {
		return pubPair{}, err
	}
	var deliveryID int64
	if err := db.QueryRowContext(ctx, `
		SELECT id FROM novaque_deliveries WHERE message_id = ? AND channel_id = ?`,
		msgID, channelID).Scan(&deliveryID); err != nil {
		return pubPair{}, err
	}
	return pubPair{messageID: msgID, deliveryID: deliveryID}, nil
}

// countRows runs a COUNT(*) query with the given args.
func countRows(ctx context.Context, t *testing.T, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", q, err)
	}
	return n
}

// reapExpiredLeasesRaw applies the ReapExpiredLeases update (U4) with raw SQL
// so pre-U4 tests can stage lease expiry without depending on the driver
// method. The eligibility clause mirrors driver/mysql/maintenance.go; unlike
// MySQL, SQLite has no UPDATE ... ORDER BY ... LIMIT here, so the bounded batch
// is selected via an id subquery (U4's reap must adapt identically).
func reapExpiredLeasesRaw(t *testing.T, ctx context.Context, db *sql.DB, limit int) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		UPDATE novaque_deliveries
		SET status = ?, available_at = unixepoch(),
		    lease_owner = NULL, lease_token = NULL, lease_until = NULL
		WHERE id IN (
			SELECT id FROM novaque_deliveries
			WHERE status = ? AND lease_until IS NOT NULL AND lease_until < unixepoch()
			ORDER BY lease_until ASC
			LIMIT ?
		)`,
		store.StatusPending, store.StatusInFlight, limit); err != nil {
		t.Fatalf("raw reap: %v", err)
	}
}
