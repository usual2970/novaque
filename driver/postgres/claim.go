package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/usual2970/novaque/store"
)

// newLeaseToken returns a 32-char lowercase hex token from 16 random bytes.
// Behavior is byte-identical to driver/mysql's newLeaseToken.
func newLeaseToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Claim leases eligible deliveries by channel id (no name lookup on the hot path).
// Poll query touches only novaque_deliveries (expires_at denormalized); body loaded after lease.
func (s *Store) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]store.Delivery, error) {
	if channelID <= 0 {
		return nil, fmt.Errorf("postgres: claim: invalid channel id")
	}
	if limit <= 0 {
		limit = 1
	}
	if leaseFor <= 0 {
		leaseFor = 30 * time.Second
	}
	leaseSec := store.DurationSec(leaseFor)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Covering-style poll on the ready slice only (Solid Queue ready_executions analogue).
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM novaque_deliveries
		WHERE channel_id = $1
		  AND status = $2
		  AND available_at <= `+sqlNow+`
		  AND expires_at > `+sqlNow+`
		ORDER BY available_at ASC, id ASC
		LIMIT $3
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

	// One statement leases every polled row. The poll's FOR UPDATE already
	// holds each row's lock in this transaction, so UPDATE ... FROM over a
	// per-id VALUES table — each entry carrying its pre-generated lease
	// token — is exactly equivalent to one guarded UPDATE per id, in a
	// single round trip. RETURNING identifies the rows that moved (the
	// pending-status predicate stays in WHERE); claim order is reconstructed
	// from ids below.
	tokensByID := make(map[int64]string, len(ids))
	var b strings.Builder
	b.WriteString(`
		UPDATE novaque_deliveries d
		SET status = $1,
		    attempts = d.attempts + 1,
		    lease_owner = $2,
		    lease_token = v.token,
		    lease_until = ` + sqlNow + ` + $3::bigint
		FROM (VALUES `)
	args := []any{store.StatusInFlight, owner, leaseSec}
	for i, id := range ids {
		token := newLeaseToken()
		tokensByID[id] = token
		if i > 0 {
			b.WriteString(", ")
		}
		base := len(args) + 1
		fmt.Fprintf(&b, "($%d::bigint, $%d::varchar)", base, base+1)
		args = append(args, id, token)
	}
	statusParam := len(args) + 1
	fmt.Fprintf(&b, `) AS v(claim_id, token)
		WHERE d.id = v.claim_id AND d.status = $%d
		RETURNING d.id`, statusParam)
	args = append(args, store.StatusPending)

	leaseRows, err := tx.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	claimed := make(map[int64]bool, len(ids))
	for leaseRows.Next() {
		var id int64
		if err := leaseRows.Scan(&id); err != nil {
			leaseRows.Close()
			return nil, err
		}
		claimed[id] = true
	}
	leaseRows.Close()
	if err := leaseRows.Err(); err != nil {
		return nil, err
	}

	claimedIDs := make([]int64, 0, len(ids))
	tokens := make(map[int64]string, len(ids))
	for _, id := range ids {
		if claimed[id] {
			claimedIDs = append(claimedIDs, id)
			tokens[id] = tokensByID[id]
		}
	}
	if len(claimedIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	marks, args := idPlaceholders(claimedIDs)
	q := fmt.Sprintf(`
		SELECT d.id, d.message_id, d.channel_id, c.topic_id, t.name, c.name, m.body,
		       d.status, d.attempts, d.max_attempts, d.lease_token, d.available_at, d.lease_until
		FROM novaque_deliveries d
		INNER JOIN novaque_messages m ON m.id = d.message_id
		INNER JOIN novaque_channels c ON c.id = d.channel_id
		INNER JOIN novaque_topics t ON t.id = c.topic_id
		WHERE d.id IN (%s)`, marks)

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
			SET status = $1, lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = $2`, store.StatusDead, id); err != nil {
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
	// Counters bump only after the commit succeeded. claim counts every row
	// leased to in_flight here — poison rows included — and a poison claim
	// also counts dead: the lease attempt and the terminal outcome both.
	s.recordStat(statTopicID, channelID, statClaim, int64(len(claimedIDs)))
	if len(deadIDs) > 0 {
		s.recordStat(statTopicID, channelID, statDead, int64(len(deadIDs)))
	}
	return out, nil
}

// leaseAttribution resolves the stats coordinates of a still-leased delivery
// in one indexed round trip. It returns sql.ErrNoRows on mismatch; the caller
// falls through to its mutation, which then affects 0 rows and produces the
// legacy error without recording anything.
func (s *Store) leaseAttribution(ctx context.Context, deliveryID int64, leaseToken string) (topicID, channelID int64, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT c.topic_id, d.channel_id
		FROM novaque_deliveries d
		INNER JOIN novaque_channels c ON c.id = d.channel_id
		WHERE d.id = $1 AND d.lease_token = $2 AND d.status = $3`,
		deliveryID, leaseToken, store.StatusInFlight).Scan(&topicID, &channelID)
	return topicID, channelID, err
}

// Ack deletes the delivery when the lease token still matches. The read-only
// attribution lookup must find the row before the delete (it disappears on
// ack), but only a DELETE affecting exactly 1 row records the event.
func (s *Store) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	topicID, channelID, scanErr := s.leaseAttribution(ctx, deliveryID, leaseToken)
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return scanErr
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM novaque_deliveries
		WHERE id = $1 AND lease_token = $2 AND status = $3`,
		deliveryID, leaseToken, store.StatusInFlight)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("postgres: ack rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	if scanErr == nil {
		s.recordStat(topicID, channelID, statAck, 1)
	}
	return nil
}

// Requeue returns a delivery to pending when the lease token matches. Only an
// UPDATE affecting exactly 1 row counts.
func (s *Store) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	topicID, channelID, scanErr := s.leaseAttribution(ctx, deliveryID, leaseToken)
	if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return scanErr
	}
	var res sql.Result
	var err error
	if availableAt.IsZero() {
		res, err = s.db.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = $1, available_at = `+sqlNow+`,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = $2 AND lease_token = $3 AND status = $4`,
			store.StatusPending, deliveryID, leaseToken, store.StatusInFlight)
	} else {
		res, err = s.db.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = $1, available_at = $2,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = $3 AND lease_token = $4 AND status = $5`,
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
		return fmt.Errorf("postgres: requeue rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	if scanErr == nil {
		s.recordStat(topicID, channelID, statRequeue, 1)
	}
	return nil
}
