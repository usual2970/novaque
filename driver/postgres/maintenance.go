package postgres

import (
	"context"
	"fmt"

	"github.com/usual2970/novaque/store"
)

// statCoord is the stats coordinates of one (topic, channel) pair — the
// per-channel attribution of a doomed batch. The day is deliberately absent:
// bucketing happens at flush time from the DB clock.
type statCoord struct {
	topicID   int64
	channelID int64
}

// ReapExpiredLeases resets expired in_flight deliveries to pending (DB time).
// It records no stats at all: a lease reap is not a requeue — the requeue
// counter belongs to handler-driven Requeue only, and reclaiming reaped work
// counts claim again.
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	// Postgres has no UPDATE ... ORDER BY/LIMIT: an IN subquery selects the
	// doomed ids (evaluated first, so updating the same table is legal),
	// ordered by lease_until and capped like the MySQL statement. The
	// eligibility predicates are repeated in the outer WHERE so READ
	// COMMITTED rechecks them against the newest row version (EvalPlanQual):
	// a row requeued and reclaimed with a fresh lease after the subquery
	// materialized is matched by id but fails the outer predicates and
	// survives, matching InnoDB's semantically-checked UPDATE.
	res, err := s.db.ExecContext(ctx, `
		UPDATE novaque_deliveries
		SET status = $1, available_at = `+sqlNow+`,
		    lease_owner = NULL, lease_token = NULL, lease_until = NULL
		WHERE id IN (
			SELECT id
			FROM novaque_deliveries
			WHERE status = $2 AND lease_until IS NOT NULL AND lease_until < `+sqlNow+`
			ORDER BY lease_until ASC
			LIMIT $3
		)
		  AND status = $2
		  AND lease_until IS NOT NULL
		  AND lease_until < `+sqlNow+`
		`,
		store.StatusPending, store.StatusInFlight, limit)
	if err != nil {
		return 0, fmt.Errorf("postgres: reap expired leases: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("postgres: reap rows affected: %w", err)
	}
	return n, nil
}

// PurgeExpired deletes expired deliveries (and orphan messages) to keep the
// claim index small. Counters need no transaction (they are buffered, not
// written in-tx): a read-only SELECT captures the doomed batch with its
// per-channel attribution, the DELETE rechecks eligibility so a mid-batch ack
// cannot die, and purge deltas are recorded only after the delete succeeds.
// Attribution comes from the SELECT, so a mid-batch race can over-count purge
// by a hair — acceptable on a maintenance path.
func (s *Store) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	// Prefer purging by delivery.expires_at (hot-path column); skip valid leases.
	rows, err := s.db.QueryContext(ctx, `
		SELECT d.id, d.channel_id, c.topic_id
		FROM novaque_deliveries d
		INNER JOIN novaque_channels c ON c.id = d.channel_id
		WHERE d.expires_at < `+sqlNow+`
		  AND (
		    d.status IN ($1, $2)
		    OR (d.status = $3 AND (d.lease_until IS NULL OR d.lease_until < `+sqlNow+`))
		  )
		ORDER BY d.expires_at ASC
		LIMIT $4`,
		store.StatusPending, store.StatusDead, store.StatusInFlight, limit)
	if err != nil {
		return 0, fmt.Errorf("postgres: select expired deliveries: %w", err)
	}
	counts := make(map[statCoord]int64)
	var ids []int64
	for rows.Next() {
		var id, channelID, topicID int64
		if err := rows.Scan(&id, &channelID, &topicID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("postgres: scan expired delivery: %w", err)
		}
		ids = append(ids, id)
		counts[statCoord{topicID: topicID, channelID: channelID}]++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: expired deliveries rows: %w", err)
	}

	var n1 int64
	if len(ids) > 0 {
		marks, args := idPlaceholders(ids)
		// Keep the eligibility conditions in the DELETE so rows that changed
		// state between SELECT and DELETE survive (mid-batch acks etc.).
		// The id marks occupy $1..$len(ids); the three statuses follow.
		q := fmt.Sprintf(`
			DELETE FROM novaque_deliveries
			WHERE id IN (%s)
			  AND expires_at < `+sqlNow+`
			  AND (
			    status IN ($%d, $%d)
			    OR (status = $%d AND (lease_until IS NULL OR lease_until < `+sqlNow+`))
			  )`,
			marks, len(ids)+1, len(ids)+2, len(ids)+3)
		args = append(args, store.StatusPending, store.StatusDead, store.StatusInFlight)
		res, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			return 0, fmt.Errorf("postgres: delete expired deliveries: %w", err)
		}
		n1, _ = res.RowsAffected()
		for k, n := range counts {
			s.recordStat(k.topicID, k.channelID, statPurged, n)
		}
	}
	// Orphan message pass: never records any stats event. Postgres permits a
	// DELETE whose subquery reads the same table directly, so the MySQL double
	// wrapper (SELECT id FROM (SELECT ... ) doomed) collapses to one level.
	res2, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_messages
		WHERE id IN (
			SELECT m.id
			FROM novaque_messages m
			WHERE m.expires_at < `+sqlNow+`
			  AND NOT EXISTS (
			    SELECT 1 FROM novaque_deliveries d WHERE d.message_id = m.id
			  )
			ORDER BY m.expires_at ASC
			LIMIT $1
		)`, limit)
	if err != nil {
		return n1, fmt.Errorf("postgres: delete orphan messages: %w", err)
	}
	n2, _ := res2.RowsAffected()
	return n1 + n2, nil
}
