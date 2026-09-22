package sqlite

import (
	"context"
	"fmt"

	"github.com/usual2970/novaque/store"
)

// ReapExpiredLeases resets expired in_flight deliveries to pending (DB time).
// It records no stats at all: a lease reap is not a requeue — the requeue
// counter belongs to handler-driven Requeue only, and reclaiming reaped work
// counts claim again.
//
// Unlike MySQL, SQLite has no UPDATE ... ORDER BY ... LIMIT: the bounded batch
// is selected through an id subquery.
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE novaque_deliveries
		SET status = ?, available_at = `+sqlNow+`,
		    lease_owner = NULL, lease_token = NULL, lease_until = NULL
		WHERE id IN (
			SELECT id
			FROM novaque_deliveries
			WHERE status = ? AND lease_until IS NOT NULL AND lease_until < `+sqlNow+`
			ORDER BY lease_until ASC
			LIMIT ?
		)`,
		store.StatusPending, store.StatusInFlight, limit)
	if err != nil {
		return 0, fmt.Errorf("sqlite: reap expired leases: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: reap expired leases rows affected: %w", err)
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
		    d.status IN (?, ?)
		    OR (d.status = ? AND (d.lease_until IS NULL OR d.lease_until < `+sqlNow+`))
		  )
		ORDER BY d.expires_at ASC
		LIMIT ?`,
		store.StatusPending, store.StatusDead, store.StatusInFlight, limit)
	if err != nil {
		return 0, fmt.Errorf("sqlite: select expired deliveries: %w", err)
	}
	counts := make(map[statCoord]int64)
	var ids []int64
	for rows.Next() {
		var id, channelID, topicID int64
		if err := rows.Scan(&id, &channelID, &topicID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("sqlite: scan expired delivery: %w", err)
		}
		ids = append(ids, id)
		counts[statCoord{topicID: topicID, channelID: channelID}]++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sqlite: iterate expired deliveries: %w", err)
	}

	var n1 int64
	if len(ids) > 0 {
		marks, args := idPlaceholders(ids)
		// Keep the eligibility conditions in the DELETE so rows that changed
		// state between SELECT and DELETE survive (mid-batch acks etc.).
		q := fmt.Sprintf(`
			DELETE FROM novaque_deliveries
			WHERE id IN (%s)
			  AND expires_at < `+sqlNow+`
			  AND (
			    status IN (?, ?)
			    OR (status = ? AND (lease_until IS NULL OR lease_until < `+sqlNow+`))
			  )`, marks)
		args = append(args, store.StatusPending, store.StatusDead, store.StatusInFlight)
		res, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			return 0, fmt.Errorf("sqlite: delete expired deliveries: %w", err)
		}
		n1, _ = res.RowsAffected()
		for k, n := range counts {
			s.recordStat(k.topicID, k.channelID, statPurged, n)
		}
	}
	// Orphan message pass: never records any stats event.

	// SQLite forbids deleting novaque_messages while a subquery reads the same
	// table (the MySQL single-statement nested-derived-table shape is not
	// accepted here), so select the doomed ids into Go with the exact
	// WHERE/ORDER/LIMIT, then delete by bound id placeholders.
	msgRows, err := s.db.QueryContext(ctx, `
		SELECT m.id
		FROM novaque_messages m
		WHERE m.expires_at < `+sqlNow+`
		  AND NOT EXISTS (
		    SELECT 1 FROM novaque_deliveries d WHERE d.message_id = m.id
		  )
		ORDER BY m.expires_at ASC
		LIMIT ?`, limit)
	if err != nil {
		return n1, fmt.Errorf("sqlite: select orphan messages: %w", err)
	}
	var msgIDs []int64
	for msgRows.Next() {
		var id int64
		if err := msgRows.Scan(&id); err != nil {
			msgRows.Close()
			return n1, fmt.Errorf("sqlite: scan orphan message: %w", err)
		}
		msgIDs = append(msgIDs, id)
	}
	msgRows.Close()
	if err := msgRows.Err(); err != nil {
		return n1, fmt.Errorf("sqlite: iterate orphan messages: %w", err)
	}

	var n2 int64
	if len(msgIDs) > 0 {
		marks, args := idPlaceholders(msgIDs)
		q := fmt.Sprintf(`DELETE FROM novaque_messages WHERE id IN (%s)`, marks)
		res, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			return n1, fmt.Errorf("sqlite: delete orphan messages: %w", err)
		}
		n2, _ = res.RowsAffected()
	}
	return n1 + n2, nil
}
