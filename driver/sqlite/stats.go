package sqlite

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/usual2970/novaque/store"
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

// statsFlushBatch caps rows per flush statement; 500 rows keeps the multi-row
// upsert comfortably under the SQLite variable/parameter limits.
const statsFlushBatch = 500

// statsFlushFullSQL is the full-batch statement rendered once: every full
// batch in a drain sends byte-identical SQL, so only a tail batch re-renders.
var statsFlushFullSQL = statsFlushSQL(statsFlushBatch)

// FlushStats drains the buffered counter sink into novaque_stats_daily in
// batched upserts. The day bucket comes from the DB clock (date('now'), the
// UTC day, never host time), so a flush spanning UTC midnight attributes its
// whole delta to the flush-time day. On error the not-yet-flushed deltas are
// merged back into the sink so a transient DB failure loses no counts
// (at-least-once).
func (s *Store) FlushStats(ctx context.Context) error {
	s.statMu.Lock()
	if len(s.statBuf) == 0 {
		s.statMu.Unlock()
		return nil
	}
	pending := s.statBuf
	s.statBuf = nil
	s.statMu.Unlock()

	// Coalesce kinds into one row per (topic, channel). order fixes this
	// flush's row sequence, so the batch loop and a failure re-merge agree on
	// which rows are remaining (map range order would not). Sorting it by
	// (topicID, channelID) also gives every flush — the loop tick, an explicit
	// Client.FlushStats, Shutdown's final drain, or another process in
	// multi-process mode — one global writer order; SQLite permits only one
	// writer at a time regardless, so overlapping flushes cannot deadlock.
	counts := make(map[statCoord][numStatKinds]int64, len(pending))
	var order []statCoord
	for k, n := range pending {
		rk := k.statCoord
		if _, ok := counts[rk]; !ok {
			order = append(order, rk)
		}
		vals := counts[rk]
		vals[k.kind] += n
		counts[rk] = vals
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].topicID != order[j].topicID {
			return order[i].topicID < order[j].topicID
		}
		return order[i].channelID < order[j].channelID
	})

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
		stmt := statsFlushFullSQL
		if end-start < statsFlushBatch {
			stmt = statsFlushSQL(end - start)
		}
		if _, err := s.db.ExecContext(ctx, stmt, args...); err != nil {
			// The failed batch (and every later one) never landed: merge those
			// deltas back so the next FlushStats retries them.
			remaining := make(map[statCoord]struct{}, len(order)-start)
			for _, rk := range order[start:] {
				remaining[rk] = struct{}{}
			}
			for k, n := range pending {
				if _, ok := remaining[k.statCoord]; ok {
					s.recordStat(k.topicID, k.channelID, k.kind, n)
				}
			}
			return fmt.Errorf("sqlite stats flush: %w", err)
		}
	}
	return nil
}

// statsFlushSQL renders the batched counter upsert for n rows. Column names
// come from statKind.column, so the enum and the SQL cannot drift. The day
// bucket is date('now') — UTC TEXT matching day_utc — and the conflict update
// adds this statement's proposed values (excluded) onto the stored row.
func statsFlushSQL(n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO novaque_stats_daily (day_utc, topic_id, channel_id")
	for k := statKind(0); k < numStatKinds; k++ {
		b.WriteString(", ")
		b.WriteString(k.column())
	}
	b.WriteString(") VALUES ")
	row := "(date('now'), ?, ?"
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
	b.WriteString(" ON CONFLICT(day_utc, topic_id, channel_id) DO UPDATE SET ")
	for k := statKind(0); k < numStatKinds; k++ {
		if k > 0 {
			b.WriteString(", ")
		}
		col := k.column()
		b.WriteString(col)
		b.WriteString(" = novaque_stats_daily.")
		b.WriteString(col)
		b.WriteString(" + excluded.")
		b.WriteString(col)
	}
	return b.String()
}

// sumDailyCounters runs the shared SUM-over-retained-days read. whereCol is
// one of the two package literals below, never caller input.
func (s *Store) sumDailyCounters(ctx context.Context, whereCol string, id int64) (store.ChannelCounters, error) {
	var c store.ChannelCounters
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(publish), 0), COALESCE(SUM(claim), 0), COALESCE(SUM(ack), 0),
		       COALESCE(SUM(requeue), 0), COALESCE(SUM(dead), 0), COALESCE(SUM(purged), 0)
		FROM novaque_stats_daily
		WHERE `+whereCol+` = ?`, id).
		Scan(&c.Publish, &c.Claim, &c.Ack, &c.Requeue, &c.Dead, &c.Purge)
	if err != nil {
		return store.ChannelCounters{}, err
	}
	return c, nil
}

// ChannelCounters sums the retained day buckets for one channel. channel_id
// is globally unique, so no topic filter is needed.
func (s *Store) ChannelCounters(ctx context.Context, channelID int64) (store.ChannelCounters, error) {
	if channelID <= 0 {
		return store.ChannelCounters{}, fmt.Errorf("sqlite stats: invalid channel id %d", channelID)
	}
	return s.sumDailyCounters(ctx, "channel_id", channelID)
}

// TopicCounters rolls up every retained day-bucket row for a topic. Fan-out
// publishes land only on real channel rows and zero-channel publishes only on
// the channel_id = 0 sentinel row, so a plain SUM over topic_id is correct.
func (s *Store) TopicCounters(ctx context.Context, topicID int64) (store.ChannelCounters, error) {
	if topicID <= 0 {
		return store.ChannelCounters{}, fmt.Errorf("sqlite stats: invalid topic id %d", topicID)
	}
	return s.sumDailyCounters(ctx, "topic_id", topicID)
}

// backlogSelectList is the one conditional-aggregation select list for live
// backlog counts, shared by ChannelBacklog (one channel) and the admin
// Backlogs batch (every channel): Pending counts every waiting row (delayed
// and expired-but-unpurged ones included) while Ready is its claimable slice
// — due now AND not expired, mirroring Claim's eligibility exactly. Both
// queries interpolate this string so the two can never drift apart.
const backlogSelectList = `
	  COUNT(CASE WHEN status = ? THEN 1 END),
	  COUNT(CASE WHEN status = ? AND available_at <= ` + sqlNow + ` AND expires_at > ` + sqlNow + ` THEN 1 END),
	  COUNT(CASE WHEN status = ? THEN 1 END),
	  COUNT(CASE WHEN status = ? THEN 1 END)`

// ChannelBacklog counts live delivery rows for one channel in a single pass
// over the idx_novaque_deliveries_claim prefix (channel_id, status,
// available_at). Ready is the claimable slice of Pending and mirrors Claim's
// eligibility exactly (claim.go): available_at has passed the DB clock AND the
// delivery's TTL has not. Delayed publishes are excluded until due, and
// expired-but-not-yet-purged pending rows stay out of Ready (they can never be
// claimed; PurgeExpired reaps them on its tick) while Pending still counts them.
func (s *Store) ChannelBacklog(ctx context.Context, channelID int64) (store.ChannelBacklog, error) {
	if channelID <= 0 {
		return store.ChannelBacklog{}, fmt.Errorf("sqlite stats: invalid channel id %d", channelID)
	}
	var b store.ChannelBacklog
	err := s.db.QueryRowContext(ctx, `
		SELECT`+backlogSelectList+`
		FROM novaque_deliveries
		WHERE channel_id = ?`,
		store.StatusPending, store.StatusPending, store.StatusInFlight, store.StatusDead, channelID).
		Scan(&b.Pending, &b.Ready, &b.InFlight, &b.Dead)
	if err != nil {
		return store.ChannelBacklog{}, err
	}
	return b, nil
}

// statsPruneBatch caps rows per prune statement, keeping the write transaction
// short on the first prune after a retention lowering (same rationale as the
// other maintenance batches).
const statsPruneBatch = 500

// PruneStats deletes day-bucket rows older than retentionDays UTC days. The
// day boundary comes from the DB clock (date('now'), never host local time).
// Deletes run in bounded batches; the return value is the same total a single
// unbounded delete would report.
//
// Unlike MySQL, SQLite has no DELETE ... ORDER BY ... LIMIT, and this table has
// no id column: the bounded batch is selected through a subquery over the
// composite primary key (day_utc, topic_id, channel_id). The cutoff modifier is
// rendered from the validated int rather than a bound parameter.
func (s *Store) PruneStats(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, fmt.Errorf("sqlite stats: invalid retention days %d", retentionDays)
	}
	var total int64
	for {
		res, err := s.db.ExecContext(ctx, fmt.Sprintf(`
			DELETE FROM novaque_stats_daily
			WHERE (day_utc, topic_id, channel_id) IN (
				SELECT day_utc, topic_id, channel_id
				FROM novaque_stats_daily
				WHERE day_utc < date('now', '-%d days')
				ORDER BY day_utc, topic_id, channel_id
				LIMIT %d
			)`, retentionDays, statsPruneBatch))
		if err != nil {
			return total, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < statsPruneBatch {
			return total, nil
		}
	}
}
