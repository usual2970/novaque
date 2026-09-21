package mysql

import (
	"context"
	"fmt"

	"github.com/usual2970/novaque/store"
)

// ChannelCounters sums the retained day buckets for one channel. channel_id
// is globally unique, so no topic filter is needed.
func (s *Store) ChannelCounters(ctx context.Context, channelID int64) (store.ChannelCounters, error) {
	if channelID <= 0 {
		return store.ChannelCounters{}, fmt.Errorf("mysql stats: invalid channel id %d", channelID)
	}
	var c store.ChannelCounters
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(publish), 0), COALESCE(SUM(claim), 0), COALESCE(SUM(ack), 0),
		       COALESCE(SUM(requeue), 0), COALESCE(SUM(dead), 0), COALESCE(SUM(purged), 0)
		FROM novaque_stats_daily
		WHERE channel_id = ?`, channelID).
		Scan(&c.Publish, &c.Claim, &c.Ack, &c.Requeue, &c.Dead, &c.Purge)
	if err != nil {
		return store.ChannelCounters{}, err
	}
	return c, nil
}

// TopicCounters rolls up every retained day-bucket row for a topic. Fan-out
// publishes land only on real channel rows and zero-channel publishes only on
// the channel_id = 0 sentinel row, so a plain SUM over topic_id is correct.
func (s *Store) TopicCounters(ctx context.Context, topicID int64) (store.ChannelCounters, error) {
	if topicID <= 0 {
		return store.ChannelCounters{}, fmt.Errorf("mysql stats: invalid topic id %d", topicID)
	}
	var c store.ChannelCounters
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(publish), 0), COALESCE(SUM(claim), 0), COALESCE(SUM(ack), 0),
		       COALESCE(SUM(requeue), 0), COALESCE(SUM(dead), 0), COALESCE(SUM(purged), 0)
		FROM novaque_stats_daily
		WHERE topic_id = ?`, topicID).
		Scan(&c.Publish, &c.Claim, &c.Ack, &c.Requeue, &c.Dead, &c.Purge)
	if err != nil {
		return store.ChannelCounters{}, err
	}
	return c, nil
}

// ChannelBacklog counts live delivery rows for one channel in a single pass
// over the idx_novaque_deliveries_claim prefix (channel_id, status,
// available_at). Ready is the claimable slice of Pending: available_at has
// passed the DB clock, so delayed publishes are excluded until due.
func (s *Store) ChannelBacklog(ctx context.Context, channelID int64) (store.ChannelBacklog, error) {
	if channelID <= 0 {
		return store.ChannelBacklog{}, fmt.Errorf("mysql stats: invalid channel id %d", channelID)
	}
	var b store.ChannelBacklog
	err := s.db.QueryRowContext(ctx, `
		SELECT
		  COUNT(CASE WHEN status = ? THEN 1 END),
		  COUNT(CASE WHEN status = ? AND available_at <= `+sqlNow+` THEN 1 END),
		  COUNT(CASE WHEN status = ? THEN 1 END),
		  COUNT(CASE WHEN status = ? THEN 1 END)
		FROM novaque_deliveries
		WHERE channel_id = ?`,
		store.StatusPending, store.StatusPending, store.StatusInFlight, store.StatusDead, channelID).
		Scan(&b.Pending, &b.Ready, &b.InFlight, &b.Dead)
	if err != nil {
		return store.ChannelBacklog{}, err
	}
	return b, nil
}

// PruneStats deletes day-bucket rows older than retentionDays UTC days. The
// day boundary comes from the DB clock (UTC_DATE()), never host local time.
func (s *Store) PruneStats(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, fmt.Errorf("mysql stats: invalid retention days %d", retentionDays)
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_stats_daily
		WHERE day_utc < UTC_DATE() - INTERVAL ? DAY`, retentionDays)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// FlushStats is a no-op until the buffered counter sink lands: counter deltas
// will be applied in batches here instead of inside mutation transactions.
func (s *Store) FlushStats(ctx context.Context) error {
	return nil
}
