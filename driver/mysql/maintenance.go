package mysql

import (
	"context"

	"novaque/store"
)

// ReapExpiredLeases resets expired in_flight deliveries to pending (DB time).
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE novaque_deliveries
		SET status = ?, available_at = NOW(3),
		    lease_owner = NULL, lease_token = NULL, lease_until = NULL
		WHERE status = ? AND lease_until IS NOT NULL AND lease_until < NOW(3)
		ORDER BY lease_until ASC
		LIMIT ?`,
		store.StatusPending, store.StatusInFlight, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PurgeExpired deletes expired messages and deliveries that are not under a valid lease.
func (s *Store) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_deliveries
		WHERE id IN (
		  SELECT id FROM (
		    SELECT d.id
		    FROM novaque_deliveries d
		    INNER JOIN novaque_messages m ON m.id = d.message_id
		    WHERE m.expires_at < NOW(3)
		      AND (
		        d.status IN (?, ?)
		        OR (d.status = ? AND (d.lease_until IS NULL OR d.lease_until < NOW(3)))
		      )
		    ORDER BY m.expires_at ASC
		    LIMIT ?
		  ) doomed
		)`,
		store.StatusPending, store.StatusDead, store.StatusInFlight, limit)
	if err != nil {
		return 0, err
	}
	n1, _ := res.RowsAffected()

	res2, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_messages
		WHERE id IN (
		  SELECT id FROM (
		    SELECT m.id
		    FROM novaque_messages m
		    WHERE m.expires_at < NOW(3)
		      AND NOT EXISTS (
		        SELECT 1 FROM novaque_deliveries d WHERE d.message_id = m.id
		      )
		    ORDER BY m.expires_at ASC
		    LIMIT ?
		  ) doomed
		)`, limit)
	if err != nil {
		return n1, err
	}
	n2, _ := res2.RowsAffected()
	return n1 + n2, nil
}
