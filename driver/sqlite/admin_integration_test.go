//go:build integration

package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/usual2970/novaque/driver/sqlite"
	"github.com/usual2970/novaque/internal/testsqlite"
	"github.com/usual2970/novaque/store"
)

// Admin-surface integration tests (U6): listings, batched backlogs, daily
// counters, dead-letter pagination and guarded dead ops, cascade deletes
// (KTD7), and the requeue-dead TTL semantics (KTD8). Each test opens its own
// temp-file database, so raw-SQL assertions see only that test's rows;
// uniquely prefixed names keep repeated runs independent.
//
// Shared helpers live in sqlite_integration_test.go — countRows scans a
// single INTEGER count and killTopicRaw deletes a topic raw, child-first — so
// they are reused here rather than duplicated.

// dbDay returns a UTC-midnight time for a SQL date expression (e.g.
// "date('now','-2 days')", read from the DB clock so expectations never
// drift from the engine's bucketing. The expressions are fixed test literals;
// day_utc is already 'YYYY-MM-DD' TEXT, so no formatting wrapper is needed.
func dbDay(t *testing.T, ctx context.Context, db *sql.DB, expr string) time.Time {
	t.Helper()
	var s string
	if err := db.QueryRowContext(ctx, "SELECT "+expr).Scan(&s); err != nil {
		t.Fatalf("read day %q: %v", expr, err)
	}
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatalf("parse day %q: %v", s, err)
	}
	return d
}

// TestAdminListTopicsAndChannels: ListTopics is ascending by name;
// ListChannels carries TopicID and orders by topic name then channel name.
func TestAdminListTopicsAndChannels(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	topicA := "admlist_a_" + suffix // lexicographically before topicB
	topicB := "admlist_b_" + suffix

	if _, err := s.EnsureChannel(ctx, topicA, "beta"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, topicA, "alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, topicB, "zeta"); err != nil {
		t.Fatal(err)
	}

	topics, err := s.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ti := range topics {
		if ti.Name == topicA || ti.Name == topicB {
			got = append(got, ti.Name)
		}
	}
	if len(got) != 2 || got[0] != topicA || got[1] != topicB {
		t.Fatalf("ListTopics order for this test's topics = %v, want [%s %s]", got, topicA, topicB)
	}
	var idA int64
	for _, ti := range topics {
		if ti.Name == topicA {
			idA = ti.ID
		}
	}
	if idA == 0 {
		t.Fatal("ListTopics did not return an id for topic A")
	}

	channels, err := s.ListChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type namePair struct{ topic, channel string }
	var order []namePair
	for _, c := range channels {
		if c.TopicID == idA {
			order = append(order, namePair{topicA, c.Name})
		}
	}
	if len(order) != 2 || order[0].channel != "alpha" || order[1].channel != "beta" {
		t.Fatalf("ListChannels order under topic A = %v, want [alpha beta]", order)
	}
	// Cross-topic ordering: every channel of topicA precedes topicB's in the
	// full listing.
	seenB := false
	for _, c := range channels {
		if c.Name == "zeta" && c.TopicID != idA {
			seenB = true
		}
		if seenB && c.TopicID == idA {
			t.Fatalf("channel of topic A (%q) listed after topic B's channel", c.Name)
		}
	}
	if !seenB {
		t.Fatal("ListChannels did not return topic B's channel")
	}
}

// TestAdminBacklogs: one batched query returns per-channel pending/ready/
// in_flight/dead counts mirroring ChannelBacklog exactly, and channels with
// no deliveries produce no row (callers zero-fill). Publishes fan out to
// every channel of a topic, so each scenario lives on its own topic to keep
// the per-channel expectations independent.
func TestAdminBacklogs(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().Format("150405.000")
	mk := func(name string) (chID, topicID int64) {
		t.Helper()
		var err error
		chID, err = s.EnsureChannel(ctx, "admback_"+name+"_"+suffix, name)
		if err != nil {
			t.Fatal(err)
		}
		topicID, err = s.EnsureTopic(ctx, "admback_"+name+"_"+suffix)
		if err != nil {
			t.Fatal(err)
		}
		return chID, topicID
	}
	aID, aTopic := mk("delayed")
	bID, bTopic := mk("leased")
	cID, cTopic := mk("poison")
	dID, _ := mk("empty")

	// A: one delayed (pending, not ready) + one immediate (ready).
	if _, err := s.Publish(ctx, aTopic, []byte("later"), store.PublishOpts{Delay: time.Minute, TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, aTopic, []byte("now"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	// B: claimed -> in_flight.
	if _, err := s.Publish(ctx, bTopic, []byte("leased"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Claim(ctx, bID, "w", 10*time.Second, 10); err != nil || len(got) != 1 {
		t.Fatalf("claim on leased channel = %v err=%v, want 1", got, err)
	}
	// C: poison -> dead (claim, requeue, claim terminalizes).
	if _, err := s.Publish(ctx, cTopic, []byte("poison"), store.PublishOpts{TTL: time.Hour, MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, cID, "w", 10*time.Second, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first poison claim = %v err=%v, want 1", first, err)
	}
	if err := s.Requeue(ctx, first[0].ID, first[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Claim(ctx, cID, "w", 10*time.Second, 10); err != nil || len(got) != 0 {
		t.Fatalf("terminal claim = %v err=%v, want 0 (dead)", got, err)
	}

	rows, err := s.Backlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byChannel := make(map[int64]store.BacklogRow)
	for _, r := range rows {
		byChannel[r.ChannelID] = r
	}
	wantA := store.BacklogRow{ChannelID: aID, Pending: 2, Ready: 1}
	if got := byChannel[aID]; got != wantA {
		t.Fatalf("backlog delayed channel = %+v, want %+v", got, wantA)
	}
	wantB := store.BacklogRow{ChannelID: bID, InFlight: 1}
	if got := byChannel[bID]; got != wantB {
		t.Fatalf("backlog leased channel = %+v, want %+v", got, wantB)
	}
	wantC := store.BacklogRow{ChannelID: cID, Dead: 1}
	if got := byChannel[cID]; got != wantC {
		t.Fatalf("backlog poison channel = %+v, want %+v", got, wantC)
	}
	if _, ok := byChannel[dID]; ok {
		t.Fatal("empty channel produced a backlog row; callers zero-fill those")
	}
}

// TestAdminDailyCounters: day rows come back existing-days-only, ascending,
// rolled up per day (topic reads include the channel_id = 0 sentinel), over
// the trailing days-day UTC window ending today with the boundary from the
// DB clock.
func TestAdminDailyCounters(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "admday_" + time.Now().Format("150405.000")
	chA, err := s.EnsureChannel(ctx, topic, "A")
	if err != nil {
		t.Fatal(err)
	}
	chB, err := s.EnsureChannel(ctx, topic, "B")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Seed day buckets directly: today's channel + sentinel rows, two-day-old
	// rows, and a ten-day-old row that every window below must exclude.
	seed := func(dayExpr string, channelID, publish, claim, ack, requeue, dead, purged int64) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO novaque_stats_daily
			  (day_utc, topic_id, channel_id, publish, claim, ack, requeue, dead, purged)
			VALUES (`+dayExpr+`, ?, ?, ?, ?, ?, ?, ?, ?)`,
			topicID, channelID, publish, claim, ack, requeue, dead, purged); err != nil {
			t.Fatalf("seed %s ch=%d: %v", dayExpr, channelID, err)
		}
	}
	seed("date('now')", chA, 2, 1, 0, 0, 0, 0)            // today, channel A
	seed("date('now')", 0, 5, 0, 0, 0, 0, 0)              // today, sentinel
	seed("date('now','-2 days')", chA, 0, 0, 4, 0, 0, 0)  // D-2, channel A
	seed("date('now','-2 days')", chB, 0, 0, 0, 0, 1, 0)  // D-2, channel B
	seed("date('now','-10 days')", chA, 0, 9, 0, 0, 0, 0) // outside every window

	// Topic rollup: sentinel included, per-day grouping, ascending days.
	got, err := s.TopicDailyCounters(ctx, topicID, 7)
	if err != nil {
		t.Fatal(err)
	}
	day2 := dbDay(t, ctx, db, "date('now','-2 days')")
	day0 := dbDay(t, ctx, db, "date('now')")
	want := []store.DailyCounters{
		{Day: day2, Ack: 4, Dead: 1},
		{Day: day0, Publish: 7, Claim: 1}, // 2 on A + 5 on the sentinel
	}
	if len(got) != len(want) {
		t.Fatalf("TopicDailyCounters(7) = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TopicDailyCounters(7)[%d] = %+v, want %+v", i, got[i], want[i])
		}
		if !got[i].Day.Equal(want[i].Day) || got[i].Day.Location() != time.UTC {
			t.Fatalf("day %d = %v, want UTC midnight %v", i, got[i].Day, want[i].Day)
		}
	}

	// days = 1 keeps only today; days boundaries are inclusive of the window.
	if got, err = s.TopicDailyCounters(ctx, topicID, 1); err != nil || len(got) != 1 || got[0] != want[1] {
		t.Fatalf("TopicDailyCounters(1) = %+v err=%v, want only today %+v", got, err, want[1])
	}

	// Channel rows: sentinel and sibling channels excluded.
	gotC, err := s.ChannelDailyCounters(ctx, chA, 7)
	if err != nil {
		t.Fatal(err)
	}
	wantC := []store.DailyCounters{
		{Day: day2, Ack: 4},
		{Day: day0, Publish: 2, Claim: 1},
	}
	if len(gotC) != len(wantC) || gotC[0] != wantC[0] || gotC[1] != wantC[1] {
		t.Fatalf("ChannelDailyCounters(A, 7) = %+v, want %+v", gotC, wantC)
	}
	if gotC, err = s.ChannelDailyCounters(ctx, chA, 2); err != nil || len(gotC) != 1 || gotC[0] != wantC[1] {
		t.Fatalf("ChannelDailyCounters(A, 2) = %+v err=%v, want only today (D-2 outside a 2-day window)", gotC, err)
	}
	if gotC, err = s.ChannelDailyCounters(ctx, chA, 3); err != nil || len(gotC) != 2 {
		t.Fatalf("ChannelDailyCounters(A, 3) = %+v err=%v, want D-2 and today", gotC, err)
	}
	if gotC, err = s.ChannelDailyCounters(ctx, chB, 7); err != nil || len(gotC) != 1 || gotC[0].Dead != 1 {
		t.Fatalf("ChannelDailyCounters(B, 7) = %+v err=%v, want one D-2 dead=1 row", gotC, err)
	}

	// Invalid arguments are rejected before SQL runs.
	if _, err = s.TopicDailyCounters(ctx, topicID, 0); err == nil {
		t.Fatal("TopicDailyCounters days=0 must error")
	}
	if _, err = s.ChannelDailyCounters(ctx, 0, 5); err == nil {
		t.Fatal("ChannelDailyCounters channel=0 must error")
	}
}

// TestAdminDeleteTopicCascade covers AE2: deleting a topic removes channels,
// messages, deliveries, and stats rows — including the channel_id = 0
// sentinel — in one transaction, and re-deleting the same id is an
// idempotent no-op.
func TestAdminDeleteTopicCascade(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "admcast_" + time.Now().Format("150405.000")
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	// Zero-channel publish first: its counter lands on the sentinel row only.
	if _, err := s.Publish(ctx, topicID, []byte("nobody"), store.PublishOpts{TTL: time.Hour}); err != nil {
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
	// Fan-out publish + delayed publish: A gets one ready and one delayed
	// delivery; claim one to also leave an in_flight row behind.
	if _, err := s.Publish(ctx, topicID, []byte("m2"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, topicID, []byte("m3"), store.PublishOpts{TTL: time.Hour, Delay: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Claim(ctx, aID, "w", 10*time.Second, 10); err != nil || len(got) != 1 {
		t.Fatalf("claim on A = %v err=%v, want 1", got, err)
	}
	// Land every buffered counter before the delete so the stats rows the
	// cascade must remove actually exist (buffered deltas flush only on
	// FlushStats; deleting first would let the next flush resurrect them).
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}

	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE topic_id = ?`, topicID); n != 3 {
		t.Fatalf("messages before delete = %d, want 3", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id IN (?, ?)`, aID, bID); n != 4 {
		t.Fatalf("deliveries before delete = %d, want 4", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_stats_daily WHERE topic_id = ?`, topicID); n != 3 {
		t.Fatalf("stats rows before delete = %d, want 3 (sentinel + A + B)", n)
	}

	if err := s.DeleteTopic(ctx, topicID); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_topics WHERE id = ?`, topicID); n != 0 {
		t.Fatalf("topics after delete = %d, want 0", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_channels WHERE topic_id = ?`, topicID); n != 0 {
		t.Fatalf("channels after delete = %d, want 0", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE topic_id = ?`, topicID); n != 0 {
		t.Fatalf("messages after delete = %d, want 0", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id IN (?, ?)`, aID, bID); n != 0 {
		t.Fatalf("deliveries after delete = %d, want 0 (cascaded)", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_stats_daily WHERE topic_id = ?`, topicID); n != 0 {
		t.Fatalf("stats rows after delete = %d, want 0 (incl. sentinel)", n)
	}
	// Idempotent re-delete.
	if err := s.DeleteTopic(ctx, topicID); err != nil {
		t.Fatalf("re-delete must be idempotent, got %v", err)
	}
	// Invalid ids never reach SQL.
	if err := s.DeleteTopic(ctx, 0); err == nil {
		t.Fatal("DeleteTopic id=0 must error")
	}
}

// TestAdminDeleteChannelCascade covers AE3: deleting one channel removes only
// its deliveries and stats rows; the shared message rows and sibling
// channels' deliveries survive untouched, and a later publish still fans out
// to the sibling.
func TestAdminDeleteChannelCascade(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "admchcast_" + time.Now().Format("150405.000")
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
	msgID, err := s.Publish(ctx, topicID, []byte("shared"), store.PublishOpts{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Claim(ctx, bID, "w", 10*time.Second, 10); err != nil || len(got) != 1 {
		t.Fatalf("claim on B = %v err=%v, want 1", got, err)
	}
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteChannel(ctx, aID); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = ?`, aID); n != 0 {
		t.Fatalf("deleted channel deliveries = %d, want 0", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = ? AND status = 'in_flight'`, bID); n != 1 {
		t.Fatalf("sibling in_flight delivery = %d, want 1 (untouched)", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, msgID); n != 1 {
		t.Fatalf("shared message row = %d, want 1 (kept for the sibling)", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_channels WHERE topic_id = ?`, topicID); n != 1 {
		t.Fatalf("channels after delete = %d, want 1 (sibling kept)", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_stats_daily WHERE channel_id = ?`, aID); n != 0 {
		t.Fatalf("deleted channel stats rows = %d, want 0", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_stats_daily WHERE channel_id = ?`, bID); n != 1 {
		t.Fatalf("sibling stats rows = %d, want 1 (kept)", n)
	}
	// Future publishes fan out to the surviving channel only, and the shared
	// message stays claimable through it.
	if _, err := s.Publish(ctx, topicID, []byte("next"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, bID, "w2", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Body) != "next" {
		t.Fatalf("post-delete claim on sibling = %#v, want the new delivery", got)
	}
	// Idempotent re-delete.
	if err := s.DeleteChannel(ctx, aID); err != nil {
		t.Fatalf("re-delete must be idempotent, got %v", err)
	}
	if err := s.DeleteChannel(ctx, 0); err == nil {
		t.Fatal("DeleteChannel id=0 must error")
	}
}

// TestAdminRequeueDeadFreshTTL covers AE4: requeueing a near-expiry dead
// delivery resets attempts, writes a fresh TTL to BOTH expires_at columns,
// lets the row survive a forced purge tick, and returns it claimable with
// attempts starting over; the requeue counter bumps by one.
func TestAdminRequeueDeadFreshTTL(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "admrq_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "W")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	// Short TTL so the row dies (needs two claims: first returns it, second
	// terminalizes) well before expiry; then the clock runs past expiry.
	msgID, err := s.Publish(ctx, topicID, []byte("phoenix"), store.PublishOpts{TTL: 3 * time.Second, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %v err=%v, want 1", first, err)
	}
	deadID := first[0].ID
	if err := s.Requeue(ctx, deadID, first[0].LeaseToken, time.Time{}); err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil || len(second) != 0 {
		t.Fatalf("terminal claim = %v err=%v, want 0 (dead at attempts=2)", second, err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ? AND status = 'dead'`, deadID); n != 1 {
		t.Fatalf("dead row = %d, want 1", n)
	}

	// Argument guards run before any mutation; a wrong channel scope is the
	// 0-rows ErrDeadGone of the scoped guard (review #10) — the delivery
	// stays dead under its owning channel.
	otherChID, err := s.EnsureChannel(ctx, topic, "X")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RequeueDead(ctx, deadID, chID, 0); err == nil {
		t.Fatal("RequeueDead freshTTL=0 must error")
	}
	if err := s.RequeueDead(ctx, 999999999, chID, time.Hour); !errors.Is(err, store.ErrDeadGone) {
		t.Fatalf("RequeueDead unknown id = %v, want ErrDeadGone", err)
	}
	if err := s.RequeueDead(ctx, deadID, otherChID, time.Hour); !errors.Is(err, store.ErrDeadGone) {
		t.Fatalf("RequeueDead wrong channel = %v, want ErrDeadGone (scoped guard)", err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ? AND status = 'dead'`, deadID); n != 1 {
		t.Fatalf("dead row after wrong-channel attempt = %d, want 1 (no mutation)", n)
	}

	// 3.6s for a 3s TTL (DB-second flooring margin, same as the other timing
	// tests): both expires_at columns are now strictly in the past.
	time.Sleep(3600 * time.Millisecond)
	var oldDeliveryExp, oldMessageExp int64
	if err := db.QueryRowContext(ctx, `
		SELECT d.expires_at, m.expires_at
		FROM novaque_deliveries d INNER JOIN novaque_messages m ON m.id = d.message_id
		WHERE d.id = ?`, deadID).Scan(&oldDeliveryExp, &oldMessageExp); err != nil {
		t.Fatal(err)
	}

	if err := s.RequeueDead(ctx, deadID, chID, time.Hour); err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}
	var newDeliveryExp, newMessageExp int64
	if err := db.QueryRowContext(ctx, `
		SELECT d.expires_at, m.expires_at
		FROM novaque_deliveries d INNER JOIN novaque_messages m ON m.id = d.message_id
		WHERE d.id = ?`, deadID).Scan(&newDeliveryExp, &newMessageExp); err != nil {
		t.Fatal(err)
	}
	if newDeliveryExp <= oldDeliveryExp || newDeliveryExp-oldDeliveryExp < 3000 {
		t.Fatalf("delivery expires_at %d -> %d, want a fresh ~1h TTL", oldDeliveryExp, newDeliveryExp)
	}
	if newMessageExp <= oldMessageExp || newMessageExp-oldMessageExp < 3000 {
		t.Fatalf("message expires_at %d -> %d, want a fresh ~1h TTL", oldMessageExp, newMessageExp)
	}

	// The forced purge tick — which would have eaten the expired dead row —
	// now leaves it alive on both passes.
	if _, err := s.PurgeExpired(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, deadID); n != 1 {
		t.Fatalf("delivery survived purge = %d, want 1", n)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_messages WHERE id = ?`, msgID); n != 1 {
		t.Fatalf("message survived orphan purge = %d, want 1", n)
	}

	// Claimable again with attempts starting over: the claim increments the
	// reset counter to 1 — without the reset it would read 2 (or die again).
	third, err := s.Claim(ctx, chID, "w", 10*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 1 || third[0].ID != deadID || third[0].MessageID != msgID {
		t.Fatalf("claim after requeue-dead = %#v, want the requeued delivery", third)
	}
	if third[0].Attempts != 1 {
		t.Fatalf("claimed attempts = %d, want 1 (reset to 0, then incremented)", third[0].Attempts)
	}
	if string(third[0].Body) != "phoenix" {
		t.Fatalf("body = %q, want the original payload", third[0].Body)
	}

	// Requeue counter +1 (alongside the handler requeue); claim counted
	// twice more; dead once.
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	want := store.ChannelCounters{Publish: 1, Claim: 3, Requeue: 2, Dead: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after requeue-dead = %+v err=%v, want %+v", c, err, want)
	}

	// The guard: the row is no longer dead, so a second requeue is
	// ErrDeadGone (idempotent-safe for racing admins).
	if err := s.RequeueDead(ctx, deadID, chID, time.Hour); !errors.Is(err, store.ErrDeadGone) {
		t.Fatalf("second RequeueDead = %v, want ErrDeadGone", err)
	}
}

// TestAdminListDeadPagination: 120 dead rows page newest-first in id DESC
// order; the before cursor walks every row without skips or dupes; bodies
// ride along on every page; a non-positive limit falls back to the driver
// default (50).
func TestAdminListDeadPagination(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "admpage_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "P")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	const total = 120
	bodies := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("deadbody-%03d", i)
		bodies[body] = true
		if _, err := s.Publish(ctx, topicID, []byte(body), store.PublishOpts{TTL: time.Hour, MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
	}
	// Terminalize all rows: claim (attempts 1, returned), requeue, claim
	// again (attempts 2 > max 1 -> dead).
	first, err := s.Claim(ctx, chID, "w", time.Minute, total)
	if err != nil || len(first) != total {
		t.Fatalf("first claim = %d err=%v, want %d", len(first), err, total)
	}
	for _, d := range first {
		if err := s.Requeue(ctx, d.ID, d.LeaseToken, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	again, err := s.Claim(ctx, chID, "w", time.Minute, total)
	if err != nil || len(again) != 0 {
		t.Fatalf("terminal claim = %d err=%v, want 0 (all dead)", len(again), err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE channel_id = ? AND status = 'dead'`, chID); n != total {
		t.Fatalf("dead rows = %d, want %d", n, total)
	}

	// Driver default page size.
	p0, err := s.ListDead(ctx, chID, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(p0) != 50 {
		t.Fatalf("default-limit page = %d rows, want 50", len(p0))
	}

	// Walk with the cursor until exhausted; ids strictly descend overall, no
	// dupes, union is every dead row, bodies always present.
	var seen []int64
	seenSet := make(map[int64]bool)
	before := int64(0)
	for {
		page, err := s.ListDead(ctx, chID, before, 50, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for i, d := range page {
			if i > 0 && d.ID >= page[i-1].ID {
				t.Fatalf("page not in id DESC order: %d after %d", d.ID, page[i-1].ID)
			}
			if seenSet[d.ID] {
				t.Fatalf("duplicate delivery id %d across pages", d.ID)
			}
			seenSet[d.ID] = true
			seen = append(seen, d.ID)
			if d.Topic != topic || d.Channel != "P" || d.Status != store.StatusDead {
				t.Fatalf("dead row meta = topic %q channel %q status %q", d.Topic, d.Channel, d.Status)
			}
			if d.Attempts != 2 || d.MaxAttempts != 1 {
				t.Fatalf("dead row attempts = %d/%d, want 2/1", d.Attempts, d.MaxAttempts)
			}
			if len(d.Body) == 0 || !bodies[string(d.Body)] {
				t.Fatalf("dead row %d body %q missing or unknown", d.ID, d.Body)
			}
			if d.ExpiresAt.IsZero() || d.AvailableAt.IsZero() {
				t.Fatalf("dead row %d missing timestamps: %+v", d.ID, d)
			}
		}
		before = page[len(page)-1].ID
		if len(seen) > total {
			t.Fatalf("pagination produced more than %d rows", total)
		}
	}
	if len(seen) != total {
		t.Fatalf("walked %d dead rows, want %d (no skips)", len(seen), total)
	}
	// A cursor at or below the oldest dead id returns nothing.
	if page, err := s.ListDead(ctx, chID, seen[len(seen)-1], 50, 0); err != nil || len(page) != 0 {
		t.Fatalf("below-oldest cursor = %d rows err=%v, want 0", len(page), err)
	}
	// The walk's newest row is the highest id in the channel.
	var maxID int64
	if err := db.QueryRowContext(ctx, `
		SELECT MAX(id) FROM novaque_deliveries WHERE channel_id = ? AND status = 'dead'`, chID).Scan(&maxID); err != nil {
		t.Fatal(err)
	}
	if seen[0] != maxID {
		t.Fatalf("newest walked id = %d, want MAX(id) = %d", seen[0], maxID)
	}

	// Review #13: a positive bodyPrefix pushes truncation into the read.
	// Every body is 12 bytes ("deadbody-NNN"), so a prefix-4 page returns
	// 4-byte Bodies while BodyLen keeps 12 on every row.
	pref, err := s.ListDead(ctx, chID, 0, 50, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(pref) != 50 {
		t.Fatalf("prefix page = %d rows, want 50", len(pref))
	}
	for _, d := range pref {
		if len(d.Body) != 4 || d.BodyLen != 12 {
			t.Fatalf("prefix row %d: body=%d bytes bodyLen=%d, want 4/12", d.ID, len(d.Body), d.BodyLen)
		}
	}
}

// TestAdminDeleteDead: DeleteDead removes exactly the guarded row of the
// named channel, bumps the purge counter, leaves sibling dead rows, and
// reports ErrDeadGone when no dead row of that channel matches (non-dead
// deliveries, and — review #10 — a dead row under a different channel).
func TestAdminDeleteDead(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	topic := "admdd_" + time.Now().Format("150405.000")
	chID, err := s.EnsureChannel(ctx, topic, "D")
	if err != nil {
		t.Fatal(err)
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	// Three dead rows (publish, claim, requeue, claim per row).
	var deadIDs []int64
	for i := 0; i < 3; i++ {
		if _, err := s.Publish(ctx, topicID, []byte(fmt.Sprintf("d%d", i)), store.PublishOpts{TTL: time.Hour, MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
		got, err := s.Claim(ctx, chID, "w", time.Minute, 10)
		if err != nil || len(got) != 1 {
			t.Fatalf("claim %d = %v err=%v, want 1", i, got, err)
		}
		if err := s.Requeue(ctx, got[0].ID, got[0].LeaseToken, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Claim(ctx, chID, "w", time.Minute, 10); err != nil || len(got) != 0 {
			t.Fatalf("terminal claim %d = %v err=%v, want 0", i, got, err)
		}
		deadIDs = append(deadIDs, gotID(t, ctx, db, chID))
	}
	// A pending, non-dead delivery: the guard must refuse it.
	if _, err := s.Publish(ctx, topicID, []byte("alive"), store.PublishOpts{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	var aliveID int64
	if err := db.QueryRowContext(ctx, `
		SELECT id FROM novaque_deliveries WHERE channel_id = ? AND status = 'pending'`, chID).Scan(&aliveID); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteDead(ctx, deadIDs[1], chID); err != nil {
		t.Fatalf("DeleteDead: %v", err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, deadIDs[1]); n != 0 {
		t.Fatalf("deleted row still present = %d, want 0", n)
	}
	left, err := s.ListDead(ctx, chID, 0, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 2 {
		t.Fatalf("remaining dead rows = %d, want 2 (siblings kept)", len(left))
	}
	// The scoped guard (review #10): a sibling dead row is not removable
	// through another channel's id.
	otherChID, err := s.EnsureChannel(ctx, topic, "X")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteDead(ctx, deadIDs[0], otherChID); !errors.Is(err, store.ErrDeadGone) {
		t.Fatalf("DeleteDead wrong channel = %v, want ErrDeadGone (scoped guard)", err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, deadIDs[0]); n != 1 {
		t.Fatalf("sibling after wrong-channel delete = %d, want 1 (no mutation)", n)
	}
	if err := s.DeleteDead(ctx, deadIDs[1], chID); !errors.Is(err, store.ErrDeadGone) {
		t.Fatalf("re-delete = %v, want ErrDeadGone", err)
	}
	if err := s.DeleteDead(ctx, aliveID, chID); !errors.Is(err, store.ErrDeadGone) {
		t.Fatalf("DeleteDead on pending row = %v, want ErrDeadGone", err)
	}
	if n := countRows(ctx, t, db, `SELECT COUNT(*) FROM novaque_deliveries WHERE id = ?`, aliveID); n != 1 {
		t.Fatalf("pending row survived = %d, want 1", n)
	}

	// purge counter +1 (dead-letter deletions count as purge, KTD8).
	if err := s.FlushStats(ctx); err != nil {
		t.Fatal(err)
	}
	want := store.ChannelCounters{Publish: 4, Claim: 6, Requeue: 3, Dead: 3, Purge: 1}
	if c, err := s.ChannelCounters(ctx, chID); err != nil || c != want {
		t.Fatalf("counters after delete-dead = %+v err=%v, want %+v", c, err, want)
	}
}

// gotID returns the newest dead delivery id on a channel (helper for tests
// that terminalize rows via Claim, which does not return them).
func gotID(t *testing.T, ctx context.Context, db *sql.DB, chID int64) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRowContext(ctx, `
		SELECT id FROM novaque_deliveries WHERE channel_id = ? AND status = 'dead' AND attempts = 2
		ORDER BY id DESC LIMIT 1`, chID).Scan(&id); err != nil {
		t.Fatalf("find newest dead id: %v", err)
	}
	return id
}
