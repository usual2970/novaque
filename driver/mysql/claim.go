package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/usual2970/novaque/store"
)

// Claim leases eligible deliveries by channel id (no name lookup on the hot path).
// Poll query touches only novaque_deliveries (expires_at denormalized); body loaded after lease.
func (s *Store) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]store.Delivery, error) {
	if channelID <= 0 {
		return nil, fmt.Errorf("mysql claim: invalid channel id")
	}
	if limit <= 0 {
		limit = 1
	}
	if leaseFor <= 0 {
		leaseFor = 30 * time.Second
	}
	leaseSec := durationSec(leaseFor)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Covering-style poll on the ready slice only (Solid Queue ready_executions analogue).
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM novaque_deliveries
		WHERE channel_id = ?
		  AND status = ?
		  AND available_at <= `+sqlNow+`
		  AND expires_at > `+sqlNow+`
		ORDER BY available_at ASC, id ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED`,
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

	claimedIDs := make([]int64, 0, len(ids))
	tokens := make(map[int64]string, len(ids))
	for _, id := range ids {
		token := newLeaseToken()
		res, err := tx.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?,
			    attempts = attempts + 1,
			    lease_owner = ?,
			    lease_token = ?,
			    lease_until = `+sqlNow+` + ?
			WHERE id = ? AND status = ?`,
			store.StatusInFlight, owner, token, leaseSec, id, store.StatusPending)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			continue
		}
		claimedIDs = append(claimedIDs, id)
		tokens[id] = token
	}
	if len(claimedIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	placeholders := make([]string, len(claimedIDs))
	args := make([]any, 0, len(claimedIDs))
	for i, id := range claimedIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	q := fmt.Sprintf(`
		SELECT d.id, d.message_id, d.channel_id, c.topic_id, t.name, c.name, m.body,
		       d.status, d.attempts, d.max_attempts, d.lease_token, d.available_at, d.lease_until
		FROM novaque_deliveries d
		INNER JOIN novaque_messages m ON m.id = d.message_id
		INNER JOIN novaque_channels c ON c.id = d.channel_id
		INNER JOIN novaque_topics t ON t.id = c.topic_id
		WHERE d.id IN (%s)`, strings.Join(placeholders, ","))

	bodyRows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer bodyRows.Close()

	var out []store.Delivery
	var deadIDs []int64
	var statTopicID int64 // every row in a Claim shares the channel, hence the topic
	byID := make(map[int64]store.Delivery, len(claimedIDs))
	for bodyRows.Next() {
		var d store.Delivery
		var availableSec int64
		var leaseUntilSec sql.NullInt64
		if err := bodyRows.Scan(
			&d.ID, &d.MessageID, &d.ChannelID, &statTopicID, &d.Topic, &d.Channel, &d.Body,
			&d.Status, &d.Attempts, &d.MaxAttempts, &d.LeaseToken, &availableSec, &leaseUntilSec,
		); err != nil {
			return nil, err
		}
		d.AvailableAt = secToTime(availableSec)
		if leaseUntilSec.Valid {
			d.LeaseUntil = secToTime(leaseUntilSec.Int64)
		}
		if tok, ok := tokens[d.ID]; ok {
			d.LeaseToken = tok
		}
		if d.Attempts > d.MaxAttempts {
			deadIDs = append(deadIDs, d.ID)
			continue
		}
		byID[d.ID] = d
	}
	if err := bodyRows.Err(); err != nil {
		return nil, err
	}

	for _, id := range deadIDs {
		if _, err := tx.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ?`, store.StatusDead, id); err != nil {
			return nil, err
		}
	}

	// Preserve claim order.
	for _, id := range claimedIDs {
		if d, ok := byID[id]; ok {
			out = append(out, d)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// Counters bump only after the commit succeeded (R5). claim counts every
	// row leased to in_flight here — poison rows included — and dead counts
	// rows terminalized inside this call (KTD4).
	s.recordStat(statTopicID, channelID, statClaim, int64(len(claimedIDs)))
	if len(deadIDs) > 0 {
		s.recordStat(statTopicID, channelID, statDead, int64(len(deadIDs)))
	}
	return out, nil
}

// ackAttribution resolves the stats coordinates of a still-leased delivery in
// one indexed round trip. It returns sql.ErrNoRows on mismatch; the caller
// falls through to its mutation, which then affects 0 rows and produces the
// legacy error without recording anything (R5).
func (s *Store) ackAttribution(ctx context.Context, deliveryID int64, leaseToken string) (topicID, channelID int64, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT c.topic_id, d.channel_id
		FROM novaque_deliveries d
		INNER JOIN novaque_channels c ON c.id = d.channel_id
		WHERE d.id = ? AND d.lease_token = ? AND d.status = ?`,
		deliveryID, leaseToken, store.StatusInFlight).Scan(&topicID, &channelID)
	return topicID, channelID, err
}

// Ack deletes the delivery when the lease token still matches. The read-only
// attribution lookup must find the row before the delete (it disappears on
// ack), but only a DELETE affecting exactly 1 row records the event.
func (s *Store) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	topicID, channelID, scanErr := s.ackAttribution(ctx, deliveryID, leaseToken)
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return scanErr
	}
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
	if scanErr == nil {
		s.recordStat(topicID, channelID, statAck, 1)
	}
	return nil
}

// Requeue returns a delivery to pending when the lease token matches. Same
// attribution shape as Ack; only an UPDATE affecting exactly 1 row counts.
func (s *Store) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	topicID, channelID, scanErr := s.ackAttribution(ctx, deliveryID, leaseToken)
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return scanErr
	}
	var res sql.Result
	var err error
	if availableAt.IsZero() {
		res, err = s.db.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, available_at = `+sqlNow+`,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ? AND lease_token = ? AND status = ?`,
			store.StatusPending, deliveryID, leaseToken, store.StatusInFlight)
	} else {
		res, err = s.db.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, available_at = ?,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ? AND lease_token = ? AND status = ?`,
			store.StatusPending, timeToSec(availableAt), deliveryID, leaseToken, store.StatusInFlight)
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
	if scanErr == nil {
		s.recordStat(topicID, channelID, statRequeue, 1)
	}
	return nil
}
