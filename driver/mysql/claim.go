package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"novaque/store"
)

// Claim leases eligible deliveries using FOR UPDATE SKIP LOCKED (DB time for leases).
func (s *Store) Claim(ctx context.Context, topic, channel, owner string, leaseFor time.Duration, limit int) ([]store.Delivery, error) {
	if limit <= 0 {
		limit = 1
	}
	if leaseFor <= 0 {
		leaseFor = 30 * time.Second
	}
	channelID, err := s.EnsureChannel(ctx, topic, channel)
	if err != nil {
		return nil, err
	}

	leaseSeconds := int64(leaseFor / time.Second)
	if leaseSeconds < 1 {
		leaseSeconds = 1
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT d.id
		FROM novaque_deliveries d
		INNER JOIN novaque_messages m ON m.id = d.message_id
		WHERE d.channel_id = ?
		  AND d.status = ?
		  AND d.available_at <= NOW(3)
		  AND m.expires_at > NOW(3)
		ORDER BY d.available_at ASC, d.id ASC
		LIMIT ?
		FOR UPDATE OF d SKIP LOCKED`,
		channelID, store.StatusPending, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	var out []store.Delivery
	for _, id := range ids {
		token := newLeaseToken()
		res, err := tx.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?,
			    attempts = attempts + 1,
			    lease_owner = ?,
			    lease_token = ?,
			    lease_until = DATE_ADD(NOW(3), INTERVAL ? SECOND)
			WHERE id = ? AND status = ?`,
			store.StatusInFlight, owner, token, leaseSeconds, id, store.StatusPending)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			continue
		}

		var d store.Delivery
		err = tx.QueryRowContext(ctx, `
			SELECT d.id, d.message_id, d.channel_id, t.name, c.name, m.body,
			       d.status, d.attempts, d.max_attempts, d.lease_token, d.available_at, d.lease_until
			FROM novaque_deliveries d
			INNER JOIN novaque_messages m ON m.id = d.message_id
			INNER JOIN novaque_channels c ON c.id = d.channel_id
			INNER JOIN novaque_topics t ON t.id = c.topic_id
			WHERE d.id = ?`, id).Scan(
			&d.ID, &d.MessageID, &d.ChannelID, &d.Topic, &d.Channel, &d.Body,
			&d.Status, &d.Attempts, &d.MaxAttempts, &d.LeaseToken, &d.AvailableAt, &d.LeaseUntil,
		)
		if err != nil {
			return nil, err
		}

		if d.Attempts > d.MaxAttempts {
			_, err = tx.ExecContext(ctx, `
				UPDATE novaque_deliveries
				SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL
				WHERE id = ?`, store.StatusDead, id)
			if err != nil {
				return nil, err
			}
			continue
		}
		out = append(out, d)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// Ack deletes the delivery when the lease token still matches.
func (s *Store) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_deliveries
		WHERE id = ? AND lease_token = ? AND status = ?`,
		deliveryID, leaseToken, store.StatusInFlight)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("ack rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	return nil
}

// Requeue returns a delivery to pending when the lease token matches.
func (s *Store) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	var res sql.Result
	var err error
	if availableAt.IsZero() {
		res, err = s.db.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, available_at = NOW(3),
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ? AND lease_token = ? AND status = ?`,
			store.StatusPending, deliveryID, leaseToken, store.StatusInFlight)
	} else {
		res, err = s.db.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, available_at = ?,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ? AND lease_token = ? AND status = ?`,
			store.StatusPending, availableAt.UTC(), deliveryID, leaseToken, store.StatusInFlight)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("requeue rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	return nil
}
