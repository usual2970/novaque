package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/usual2970/novaque/store"
)

// statKind enumerates the counter columns of novaque_stats_daily.
type statKind uint8

const (
	statPublish statKind = iota
	statClaim
	statAck
	statRequeue
	statDead
	statPurged
	numStatKinds // must stay last
)

// column returns the novaque_stats_daily column a kind flushes into. The
// switch is fixed; caller-supplied strings never reach SQL.
func (k statKind) column() string {
	switch k {
	case statPublish:
		return "publish"
	case statClaim:
		return "claim"
	case statAck:
		return "ack"
	case statRequeue:
		return "requeue"
	case statDead:
		return "dead"
	case statPurged:
		return "purged"
	default:
		return ""
	}
}

// statKey identifies one counter cell for one (topic, channel) pair — the day
// is deliberately absent: bucketing happens at flush time from the DB clock.
// channelID 0 is the zero-channel publish sentinel (KTD3).
type statKey struct {
	topicID   int64
	channelID int64
	kind      statKind
}

// statsFlushBatch caps rows per flush statement; 500 rows keeps the multi-row
// upsert comfortably under packet and placeholder limits.
const statsFlushBatch = 500

// recordStat buffers a counter delta in the in-process sink. Mutators call it
// only AFTER a mutation committed (or RowsAffected confirmed success), so
// rollbacks and lease mismatches never count (R5); the map coalesces repeated
// deltas for free. Buffering keeps every mutation transaction free of stats
// writes — FlushStats drains the sink in batches.
func (s *Store) recordStat(topicID, channelID int64, kind statKind, n int64) {
	if n == 0 || topicID <= 0 || channelID < 0 {
		return
	}
	s.statMu.Lock()
	if s.statBuf == nil {
		s.statBuf = make(map[statKey]int64)
	}
	s.statBuf[statKey{topicID: topicID, channelID: channelID, kind: kind}] += n
	s.statMu.Unlock()
}

// FlushStats drains the buffered counter sink into novaque_stats_daily in
// batched upserts. The day bucket comes from the DB clock (UTC_DATE(), KTD8 —
// never host time), so a flush spanning UTC midnight attributes its whole
// delta to the flush-time day. On error the not-yet-flushed deltas are merged
// back into the sink so a transient DB failure loses no counts (at-least-once).
func (s *Store) FlushStats(ctx context.Context) error {
	s.statMu.Lock()
	if len(s.statBuf) == 0 {
		s.statMu.Unlock()
		return nil
	}
	pending := s.statBuf
	s.statBuf = nil
	s.statMu.Unlock()

	// Coalesce kinds into one row per (topic, channel); order keeps batching
	// deterministic across flushes.
	type rowKey struct {
		topicID   int64
		channelID int64
	}
	counts := make(map[rowKey][numStatKinds]int64, len(pending))
	var order []rowKey
	for k, n := range pending {
		rk := rowKey{topicID: k.topicID, channelID: k.channelID}
		if _, ok := counts[rk]; !ok {
			order = append(order, rk)
		}
		vals := counts[rk]
		vals[k.kind] += n
		counts[rk] = vals
	}

	for start := 0; start < len(order); start += statsFlushBatch {
		end := min(start+statsFlushBatch, len(order))
		args := make([]any, 0, (end-start)*(3+int(numStatKinds)))
		for _, rk := range order[start:end] {
			vals := counts[rk]
			args = append(args, rk.topicID, rk.channelID)
			for k := statKind(0); k < numStatKinds; k++ {
				args = append(args, vals[k])
			}
		}
		if _, err := s.db.ExecContext(ctx, statsFlushSQL(end-start), args...); err != nil {
			// The failed batch (and every later one) never landed: merge those
			// deltas back so the next FlushStats retries them.
			remaining := make(map[rowKey]struct{}, len(order)-start)
			for _, rk := range order[start:] {
				remaining[rk] = struct{}{}
			}
			s.statMu.Lock()
			if s.statBuf == nil {
				s.statBuf = make(map[statKey]int64)
			}
			for k, n := range pending {
				if _, ok := remaining[rowKey{topicID: k.topicID, channelID: k.channelID}]; ok {
					s.statBuf[k] += n
				}
			}
			s.statMu.Unlock()
			return fmt.Errorf("mysql stats flush: %w", err)
		}
	}
	return nil
}

// statsFlushSQL renders the batched counter upsert for n rows. Column names
// come from statKind.column, so the enum and the SQL cannot drift. VALUES()
// is kept over row-alias ODKU because row aliases need MySQL 8.0.19+ and the
// floor is 8.0.1.
func statsFlushSQL(n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO novaque_stats_daily (day_utc, topic_id, channel_id")
	for k := statKind(0); k < numStatKinds; k++ {
		b.WriteString(", ")
		b.WriteString(k.column())
	}
	b.WriteString(") VALUES ")
	row := "(UTC_DATE(), ?, ?"
	for k := statKind(0); k < numStatKinds; k++ {
		row += ", ?"
	}
	row += ")"
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(row)
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	for k := statKind(0); k < numStatKinds; k++ {
		if k > 0 {
			b.WriteString(", ")
		}
		col := k.column()
		b.WriteString(col)
		b.WriteString(" = ")
		b.WriteString(col)
		b.WriteString(" + VALUES(")
		b.WriteString(col)
		b.WriteString(")")
	}
	return b.String()
}

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
