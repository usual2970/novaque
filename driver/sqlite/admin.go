package sqlite

// Admin surface of store.Store (mountable admin UI plan U2): listings,
// batched backlogs, day-bucket counter windows, keyset-paginated dead-letter
// browse with bodies (R12: the only admin surface carrying payloads), guarded
// dead ops (KTD8), and single-transaction cascade deletes (KTD7). Reads are
// ID-addressed and never create rows; backlog and counter reads are batched,
// never per-channel N+1.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/usual2970/novaque/store"
)

// defaultDeadPage bounds a ListDead page when the caller passes a
// non-positive limit (store contract: driver default).
const defaultDeadPage = 50

// ListTopics returns every topic row ascending by name (store contract).
func (s *Store) ListTopics(ctx context.Context) ([]store.TopicInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name FROM novaque_topics ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.TopicInfo
	for rows.Next() {
		var t store.TopicInfo
		if err := rows.Scan(&t.ID, &t.Name); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListChannels returns every channel across all topics — each carrying
// TopicID — ordered by topic name then channel name, so the UI groups without
// re-sorting.
func (s *Store) ListChannels(ctx context.Context) ([]store.ChannelInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.id, c.topic_id, c.name
		FROM novaque_channels c
		INNER JOIN novaque_topics t ON t.id = c.topic_id
		ORDER BY t.name ASC, c.name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.ChannelInfo
	for rows.Next() {
		var c store.ChannelInfo
		if err := rows.Scan(&c.ID, &c.TopicID, &c.Name); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Backlogs returns live per-channel backlog counts for every channel in one
// query (R2), interpolating the shared backlogSelectList so per-channel
// reads (ChannelBacklog) and these batched admin totals can never disagree.
// Rows appear only for channels with at least one delivery; callers
// zero-fill the rest against ListChannels.
func (s *Store) Backlogs(ctx context.Context) ([]store.BacklogRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT channel_id,`+backlogSelectList+`
		FROM novaque_deliveries
		GROUP BY channel_id
		ORDER BY channel_id ASC`,
		store.StatusPending, store.StatusPending, store.StatusInFlight, store.StatusDead)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.BacklogRow
	for rows.Next() {
		var r store.BacklogRow
		if err := rows.Scan(&r.ChannelID, &r.Pending, &r.Ready, &r.InFlight, &r.Dead); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// BacklogsForTopic scopes the batched backlog aggregate to one topic's
// channels (the detail-page counterpart of Backlogs — review finding: topic
// and channel pages must not pay the all-channels GROUP BY), interpolating
// the same shared backlogSelectList so per-channel reads, the all-channels
// batch, and this scoped batch can never disagree. Row semantics as per
// Backlogs: rows only for channels with at least one delivery, callers
// zero-fill the rest against ListChannels.
func (s *Store) BacklogsForTopic(ctx context.Context, topicID int64) ([]store.BacklogRow, error) {
	if topicID <= 0 {
		return nil, fmt.Errorf("sqlite admin: invalid topic id %d", topicID)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT channel_id,`+backlogSelectList+`
		FROM novaque_deliveries
		JOIN novaque_channels ON novaque_channels.id = novaque_deliveries.channel_id
		WHERE novaque_channels.topic_id = ?
		GROUP BY channel_id
		ORDER BY channel_id ASC`,
		store.StatusPending, store.StatusPending, store.StatusInFlight, store.StatusDead, topicID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.BacklogRow
	for rows.Next() {
		var r store.BacklogRow
		if err := rows.Scan(&r.ChannelID, &r.Pending, &r.Ready, &r.InFlight, &r.Dead); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TopicDailyCounters returns the topic's retained day-bucket counter rows —
// per-channel rows plus the zero-channel sentinel (channel_id = 0) rolled up
// per day, so a plain SUM per day is correct — over the trailing days-day UTC
// window ending today. Existing-day rows only, ascending by day; zero-filling
// the window is the caller's job.
func (s *Store) TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	if topicID <= 0 {
		return nil, fmt.Errorf("sqlite admin: invalid topic id %d", topicID)
	}
	if days < 1 {
		return nil, fmt.Errorf("sqlite admin: invalid days %d", days)
	}
	return s.dailyCounters(ctx, whereTopicID, topicID, days)
}

// ChannelDailyCounters returns one channel's retained day-bucket counter rows
// over the trailing days-day UTC window ending today. channel_id is globally
// unique, so no topic filter is needed. Existing-day rows only, ascending by
// day; zero-filling is the caller's job.
func (s *Store) ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	if channelID <= 0 {
		return nil, fmt.Errorf("sqlite admin: invalid channel id %d", channelID)
	}
	if days < 1 {
		return nil, fmt.Errorf("sqlite admin: invalid days %d", days)
	}
	return s.dailyCounters(ctx, whereChannelID, channelID, days)
}

// ClusterDailyCounters returns every retained day-bucket row rolled up per
// UTC day over the trailing days-day window ending today.
func (s *Store) ClusterDailyCounters(ctx context.Context, days int) ([]store.DailyCounters, error) {
	if days < 1 {
		return nil, fmt.Errorf("sqlite admin: invalid days %d", days)
	}
	return s.clusterDailyCounters(ctx, days)
}

// The two where-column literals dailyCounters accepts; never caller input.
const (
	whereTopicID   = "topic_id"
	whereChannelID = "channel_id"
)

// dailyCounters runs the shared day-bucket read over the trailing days-day
// UTC window ending today. The boundary comes from the DB clock — the same
// date('now') bucketing FlushStats writes and PruneStats prunes with — never
// host time, and is rendered from the validated int rather than a bound
// parameter (no modifier binding in SQLite). day_utc already crosses the wire
// as a 'YYYY-MM-DD' TEXT string parsed into UTC midnights in Go, so bucketing
// never depends on host-locale date handling; string comparison against the
// TEXT values is chronological.
func (s *Store) dailyCounters(ctx context.Context, whereCol string, id int64, days int) ([]store.DailyCounters, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT day_utc,
		       SUM(publish), SUM(claim), SUM(ack), SUM(requeue), SUM(dead), SUM(purged)
		FROM novaque_stats_daily
		WHERE `+whereCol+` = ? AND day_utc > date('now', '-%d days')
		GROUP BY day_utc
		ORDER BY day_utc ASC`, days), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.DailyCounters
	for rows.Next() {
		var dayStr string
		var d store.DailyCounters
		if err := rows.Scan(&dayStr, &d.Publish, &d.Claim, &d.Ack, &d.Requeue, &d.Dead, &d.Purge); err != nil {
			return nil, err
		}
		day, err := time.Parse("2006-01-02", dayStr)
		if err != nil {
			return nil, fmt.Errorf("sqlite admin: parse day bucket %q: %w", dayStr, err)
		}
		d.Day = day
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) clusterDailyCounters(ctx context.Context, days int) ([]store.DailyCounters, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT day_utc,
		       SUM(publish), SUM(claim), SUM(ack), SUM(requeue), SUM(dead), SUM(purged)
		FROM novaque_stats_daily
		WHERE day_utc > date('now', '-%d days')
		GROUP BY day_utc
		ORDER BY day_utc ASC`, days))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.DailyCounters
	for rows.Next() {
		var dayStr string
		var d store.DailyCounters
		if err := rows.Scan(&dayStr, &d.Publish, &d.Claim, &d.Ack, &d.Requeue, &d.Dead, &d.Purge); err != nil {
			return nil, err
		}
		day, err := time.Parse("2006-01-02", dayStr)
		if err != nil {
			return nil, fmt.Errorf("sqlite admin: parse day bucket %q: %w", dayStr, err)
		}
		d.Day = day
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListDead returns a channel's dead deliveries newest-first (id DESC),
// keyset-paginated: before > 0 returns only rows with id < before; limit
// bounds the page, non-positive falling back to defaultDeadPage. The body is
// joined per page — this is the one admin surface that carries payloads
// (R12).
func (s *Store) ListDead(ctx context.Context, channelID int64, before int64, limit int, bodyPrefix int) ([]store.DeadDelivery, error) {
	if channelID <= 0 {
		return nil, fmt.Errorf("sqlite admin: invalid channel id %d", channelID)
	}
	if limit <= 0 {
		limit = defaultDeadPage
	}
	if bodyPrefix < 0 {
		bodyPrefix = 0
	}
	// The body column is the read's only heavyweight payload, and the polled
	// list never renders more than a preview (review #13): bodyPrefix > 0
	// pushes the truncation into the scan itself (substr is byte-safe on a
	// BLOB — verified empirically: binary bytes > 128 round-trip unchanged;
	// payloads are opaque bytes, not guaranteed UTF-8), while length keeps
	// the true byte size for the truncation marker and body_bytes fields.
	// bodyPrefix = 0 reads whole bodies (per-delivery endpoints). SQLite has
	// no IF() or left(); CASE + substr is the portable shape (verified: no
	// left() in 3.53.4 either).
	rows, err := s.db.QueryContext(ctx, `
		SELECT d.id, d.message_id, d.channel_id, t.name, c.name,
		       CASE WHEN ? = 0 THEN m.body ELSE substr(m.body, 1, ?) END, length(m.body),
		       d.status, d.attempts, d.max_attempts, d.available_at, d.expires_at
		FROM novaque_deliveries d
		INNER JOIN novaque_messages m ON m.id = d.message_id
		INNER JOIN novaque_channels c ON c.id = d.channel_id
		INNER JOIN novaque_topics t ON t.id = c.topic_id
		WHERE d.channel_id = ?
		  AND d.status = ?
		  AND (? = 0 OR d.id < ?)
		ORDER BY d.id DESC
		LIMIT ?`,
		bodyPrefix, bodyPrefix, channelID, store.StatusDead, before, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.DeadDelivery
	for rows.Next() {
		var d store.DeadDelivery
		var availableSec, expiresSec int64
		if err := rows.Scan(
			&d.ID, &d.MessageID, &d.ChannelID, &d.Topic, &d.Channel, &d.Body, &d.BodyLen,
			&d.Status, &d.Attempts, &d.MaxAttempts, &availableSec, &expiresSec,
		); err != nil {
			return nil, err
		}
		d.AvailableAt = secToTime(availableSec)
		d.ExpiresAt = secToTime(expiresSec)
		out = append(out, d)
	}
	return out, rows.Err()
}

// RequeueDead returns one dead delivery of channelID to pending per KTD8:
// attempts reset to zero, lease cleared, available now, and a fresh TTL
// written to BOTH expires_at columns — the delivery column keeps the
// requeued row past the next purge tick, the message column stops the orphan
// purge and the message-delete cascade from eating it. Extending
// messages.expires_at is safe for sibling deliveries: each sibling's own
// deliveries.expires_at still governs its purge, and the message row lives
// until its last delivery is gone (existing orphan semantics). The guarded
// WHERE — channel-scoped as well as status-dead (review #10: the URL names
// the channel, so a delivery dead under a different channel must never move)
// — makes the operation idempotent-safe: a second call, or one naming
// another channel's delivery, affects 0 rows and returns ErrDeadGone.
func (s *Store) RequeueDead(ctx context.Context, deliveryID, channelID int64, freshTTL time.Duration) error {
	if freshTTL <= 0 {
		return fmt.Errorf("sqlite admin: invalid fresh TTL %s", freshTTL)
	}
	ttlSec := store.DurationSec(freshTTL)

	// SQLite transactions are always serializable; BeginTx with nil options —
	// modernc rejects explicit isolation levels (e.g. ReadCommitted).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// The guarded delivery UPDATE returns its stats coordinates itself
	// (RETURNING): the channel-scoped, status-dead guard is the same fencing
	// as before, and a non-matching row returns no rows -> ErrDeadGone.
	var topic sql.NullInt64
	var gotChannel int64
	if err := tx.QueryRowContext(ctx, `
		UPDATE novaque_deliveries
		SET status = ?, attempts = 0, available_at = `+sqlNow+`,
		    lease_owner = NULL, lease_token = NULL, lease_until = NULL,
		    expires_at = `+sqlNow+` + ?
		WHERE id = ? AND channel_id = ? AND status = ?`+statAttribution,
		store.StatusPending, ttlSec, deliveryID, channelID, store.StatusDead).
		Scan(&topic, &gotChannel); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrDeadGone
		}
		return err
	}
	// The message row's expiry moves with the delivery's (subquery reads a
	// different table than it writes, which SQLite permits — it updates the
	// row inside this tx after the delivery's status changed; no separate
	// message_id lookup is needed). Both writes commit or roll back together.
	if _, err := tx.ExecContext(ctx, `
		UPDATE novaque_messages
		SET expires_at = `+sqlNow+` + ?
		WHERE id = (SELECT message_id FROM novaque_deliveries WHERE id = ?)`,
		ttlSec, deliveryID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if topic.Valid {
		s.recordStat(topic.Int64, gotChannel, statRequeue, 1)
	}
	return nil
}

// DeleteDead removes one dead delivery of channelID; the shared message row
// is reclaimed by the orphan purge once its sibling deliveries are gone.
// Guarded on channel_id = channelID AND status = dead (review #10: the URL
// names the channel, so a delivery dead under a different channel is never
// removed through it): ErrDeadGone when no dead row of that channel matched.
// Manual dead-letter deletions count as purge (KTD8: purge = TTL purges +
// manual deletions).
func (s *Store) DeleteDead(ctx context.Context, deliveryID, channelID int64) error {
	// The guarded DELETE returns its coordinates itself (RETURNING): the same
	// channel-scoped, status-dead guard as before, and a non-matching row
	// returns no rows -> ErrDeadGone.
	var topic sql.NullInt64
	var gotChannel int64
	err := s.db.QueryRowContext(ctx, `
		DELETE FROM novaque_deliveries
		WHERE id = ? AND channel_id = ? AND status = ?`+statAttribution,
		deliveryID, channelID, store.StatusDead).Scan(&topic, &gotChannel)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrDeadGone
	}
	if err != nil {
		return err
	}
	if topic.Valid {
		s.recordStat(topic.Int64, gotChannel, statPurged, 1)
	}
	return nil
}

// DeleteTopic removes the topic and everything under it in one transaction,
// strictly child-first (KTD7): messages first — the deliveries.message_id
// ON DELETE CASCADE clears every delivery (verified empirically through the
// store with foreign_keys enabled) — then channels, stats rows (every
// channel's plus the channel_id = 0 sentinel), and finally the topic row.
// Zero rows affected anywhere is an already-deleted topic: the whole tx still
// commits as an idempotent no-op. A concurrent publisher that re-creates the
// topic mid-tx fails the topic delete on its FK and aborts with an error;
// callers retry into idempotent success. After the cascade commits, buffered
// counter deltas for the topic are discarded too, so the next flush cannot
// upsert them back into the just-deleted stats rows.
func (s *Store) DeleteTopic(ctx context.Context, topicID int64) error {
	if topicID <= 0 {
		return fmt.Errorf("sqlite admin: invalid topic id %d", topicID)
	}
	err := s.runCascade(ctx, []cascadeStep{
		{"messages", `DELETE FROM novaque_messages WHERE topic_id = ?`},
		{"channels", `DELETE FROM novaque_channels WHERE topic_id = ?`},
		{"stats", `DELETE FROM novaque_stats_daily WHERE topic_id = ?`},
		{"topic", `DELETE FROM novaque_topics WHERE id = ?`},
	}, topicID)
	if err != nil {
		return err
	}
	// Discard only after the commit: on error the tx rolled back and every
	// row still exists. A publisher that committed just before the cascade
	// can still leave a buffered delta; FlushStats filters coords against
	// live rows and is the durable backstop.
	s.discardTopicStats(topicID)
	return nil
}

// DeleteChannel removes one channel's deliveries and stats rows in one
// transaction, then the channel row itself (KTD7). Messages are never
// touched: deleting them would cascade away sibling channels' deliveries, so
// orphaned message rows are left for the existing orphan purge. Idempotent on
// already-deleted ids. After the cascade commits, buffered counter deltas for
// the channel are discarded too, so the next flush cannot upsert them back
// into the just-deleted stats rows.
func (s *Store) DeleteChannel(ctx context.Context, channelID int64) error {
	if channelID <= 0 {
		return fmt.Errorf("sqlite admin: invalid channel id %d", channelID)
	}
	err := s.runCascade(ctx, []cascadeStep{
		{"deliveries", `DELETE FROM novaque_deliveries WHERE channel_id = ?`},
		{"stats", `DELETE FROM novaque_stats_daily WHERE channel_id = ?`},
		{"channel", `DELETE FROM novaque_channels WHERE id = ?`},
	}, channelID)
	if err != nil {
		return err
	}
	// Discard only after the commit: on error the tx rolled back and the
	// channel's rows still exist. FlushStats' existence filter covers a
	// delta buffered by a publisher that committed just before the cascade.
	s.discardChannelStats(channelID)
	return nil
}

// cascadeStep is one child-first DELETE of a cascade transaction.
type cascadeStep struct {
	name string
	sql  string
}

// runCascade executes the ordered DELETEs in one transaction, committing the
// idempotent no-op when every step affects zero rows and returning the
// failing step's error (wrapped with its name) when one aborts.
func (s *Store) runCascade(ctx context.Context, steps []cascadeStep, id int64) error {
	// SQLite transactions are always serializable; BeginTx with nil options —
	// modernc rejects explicit isolation levels (e.g. ReadCommitted).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, step := range steps {
		if _, err := tx.ExecContext(ctx, step.sql, id); err != nil {
			return fmt.Errorf("sqlite admin cascade (%s): %w", step.name, err)
		}
	}
	return tx.Commit()
}
