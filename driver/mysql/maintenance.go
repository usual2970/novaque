package mysql

import (
	"context"

	"github.com/usual2970/novaque/store"
)

// ReapExpiredLeases resets expired in_flight deliveries to pending (DB time).
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE novaque_deliveries
		SET status = ?, available_at = `+sqlNow+`,
		    lease_owner = NULL, lease_token = NULL, lease_until = NULL
		WHERE status = ? AND lease_until IS NOT NULL AND lease_until < `+sqlNow+`
		ORDER BY lease_until ASC
		LIMIT ?`,
		store.StatusPending, store.StatusInFlight, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PurgeExpired deletes expired deliveries (and orphan messages) to keep the claim index small.
func (s *Store) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	// Prefer purging by delivery.expires_at (hot-path column); skip valid leases.
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_deliveries
		WHERE id IN (
		  SELECT id FROM (
		    SELECT d.id
		    FROM novaque_deliveries d
		    WHERE d.expires_at < `+sqlNow+`
		      AND (
		        d.status IN (?, ?)
		        OR (d.status = ? AND (d.lease_until IS NULL OR d.lease_until < `+sqlNow+`))
		      )
		    ORDER BY d.expires_at ASC
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
		    WHERE m.expires_at < `+sqlNow+`
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
