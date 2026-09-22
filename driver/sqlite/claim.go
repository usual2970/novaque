package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	sqlite3 "modernc.org/sqlite"

	"github.com/usual2970/novaque/store"
)

// Claim retry tuning for SQLite snapshot contention. SQLite has no row locks:
// concurrent claim transactions each take a snapshot on their poll SELECT,
// and the first one to commit the pending→in_flight updates invalidates the
// others' snapshots; their first write then fails SQLITE_BUSY_SNAPSHOT (517).
// busy_timeout does not retry 517, so Claim re-runs the whole transaction.
const (
	claimMaxAttempts = 10
	claimBackoffMin  = time.Millisecond
	claimBackoffMax  = 50 * time.Millisecond
)

// sqliteErrBusy is SQLITE_BUSY (primary code 5); sqliteErrBusySnapshot is the
// extended SQLITE_BUSY_SNAPSHOT (5 | 2<<8 = 517): a read transaction's
// snapshot went stale before a write could start on the same connection.
const (
	sqliteErrBusy         = 5
	sqliteErrBusySnapshot = 517
)

// isBusyError reports whether err is a SQLite busy code (5 or 517). busy_timeout
// already retried the ordinary lock cases before such an error surfaces, so a
// 5 here is treated the same as 517: discard the stale transaction, retry.
func isBusyError(err error) bool {
	var e *sqlite3.Error
	if errors.As(err, &e) {
		switch e.Code() {
		case sqliteErrBusy, sqliteErrBusySnapshot:
			return true
		}
	}
	return false
}

// claimBackoff is the jittered sleep before claim attempt `attempt` (0-based):
// 1ms, 2ms, 4ms, … capped at 50ms, plus up to half again in jitter so
// competing transactions do not retry in lockstep.
func claimBackoff(attempt int) time.Duration {
	d := claimBackoffMin << attempt
	if d <= 0 || d > claimBackoffMax {
		d = claimBackoffMax
	}
	return d + time.Duration(rand.Int64N(int64(d)/2+1))
}

// Claim leases eligible deliveries by channel id (no name lookup on the hot path).
// Poll query touches only novaque_deliveries (expires_at denormalized); body loaded after lease.
// Unlike MySQL, the transaction may fail with a stale-snapshot busy error under
// concurrency; Claim retries the whole transaction up to claimMaxAttempts.
func (s *Store) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]store.Delivery, error) {
	if channelID <= 0 {
		return nil, fmt.Errorf("sqlite claim: invalid channel id")
	}
	if limit <= 0 {
		limit = 1
	}
	if leaseFor <= 0 {
		leaseFor = 30 * time.Second
	}
	leaseSec := durationSec(leaseFor)

	var (
		out          []store.Delivery
		statTopicID  int64
		claimedCount int
		deadCount    int
		lastBusyErr  error
		err          error
	)
	for attempt := 0; attempt < claimMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(claimBackoff(attempt - 1)):
			}
		}
		out, statTopicID, claimedCount, deadCount, err = s.runClaimTx(ctx, channelID, owner, leaseSec, limit)
		switch {
		case err == nil:
			// Counters bump only after the commit succeeded. claim counts every row
			// leased to in_flight here — poison rows included — and a poison claim
			// also counts dead: the lease attempt and the terminal outcome both.
			s.recordStat(statTopicID, channelID, statClaim, int64(claimedCount))
			if deadCount > 0 {
				s.recordStat(statTopicID, channelID, statDead, int64(deadCount))
			}
			return out, nil
		case isBusyError(err):
			lastBusyErr = err
			continue
		default:
			return nil, err
		}
	}
	return nil, fmt.Errorf("sqlite claim: busy after %d attempts: %w", claimMaxAttempts, lastBusyErr)
}

// runClaimTx executes one claim attempt: poll ready ids, guard-lease each id
// pending→in_flight, load bodies, and mark poison rows dead. A stale snapshot
// surfaces from the first UPDATE (or the commit); the caller retries.
func (s *Store) runClaimTx(ctx context.Context, channelID int64, owner string, leaseSec int64, limit int) (out []store.Delivery, statTopicID int64, claimedCount, deadCount int, err error) {
	// SQLite transactions are always serializable; BeginTx with nil options —
	// modernc rejects explicit isolation levels (e.g. ReadCommitted).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()

	// Covering-style poll on the ready slice only (Solid Queue ready_executions analogue).
	// No FOR UPDATE SKIP LOCKED here: SQLite has no row locks and never
	// implemented that clause in any version. The pending→in_flight UPDATE
	// below re-checks status, so a concurrent claim with a stale snapshot can
	// never re-lease; its first write fails SQLITE_BUSY_SNAPSHOT (517) and the
	// caller retries with a fresh snapshot.
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM novaque_deliveries
		WHERE channel_id = ?
		  AND status = ?
		  AND available_at <= `+sqlNow+`
		  AND expires_at > `+sqlNow+`
		ORDER BY available_at ASC, id ASC
		LIMIT ?`,
		channelID, store.StatusPending, limit)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, 0, 0, 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, 0, 0, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, 0, 0, 0, err
		}
		return nil, 0, 0, 0, nil
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
			return nil, 0, 0, 0, err
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
			return nil, 0, 0, 0, err
		}
		return nil, 0, 0, 0, nil
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
		return nil, 0, 0, 0, err
	}
	defer bodyRows.Close()

	var deadIDs []int64
	byID := make(map[int64]store.Delivery, len(claimedIDs))
	for bodyRows.Next() {
		var d store.Delivery
		var availableSec int64
		var leaseUntilSec sql.NullInt64
		if err := bodyRows.Scan(
			&d.ID, &d.MessageID, &d.ChannelID, &statTopicID, &d.Topic, &d.Channel, &d.Body,
			&d.Status, &d.Attempts, &d.MaxAttempts, &d.LeaseToken, &availableSec, &leaseUntilSec,
		); err != nil {
			return nil, 0, 0, 0, err
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
		return nil, 0, 0, 0, err
	}

	for _, id := range deadIDs {
		if _, err := tx.ExecContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ?`, store.StatusDead, id); err != nil {
			return nil, 0, 0, 0, err
		}
	}

	// Preserve claim order.
	for _, id := range claimedIDs {
		if d, ok := byID[id]; ok {
			out = append(out, d)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, 0, 0, err
	}
	return out, statTopicID, len(claimedIDs), len(deadIDs), nil
}

// ensureSQLiteVersion gates Migrate on SQLite 3.39.0+ (KTD2, OQ2). The
// threshold and failure wording were set when SKIP LOCKED was believed to
// exist on recent SQLite; empirically no SQLite release parses that clause,
// and on this driver the no-double-lease guarantee rests on snapshot
// isolation plus the status-guarded lease UPDATE. The check is kept as the
// mandated runtime floor; it is cached per Store (sync.Once) so a repeated
// Migrate costs nothing.
func (s *Store) ensureSQLiteVersion(ctx context.Context) error {
	s.versionOnce.Do(func() {
		s.versionErr = s.checkSQLiteVersion(ctx)
	})
	return s.versionErr
}

// checkSQLiteVersion reads sqlite_version() and requires 3.39.0 or newer,
// failing fast with a versioned error instead of running on an old build.
func (s *Store) checkSQLiteVersion(ctx context.Context) error {
	var version string
	if err := s.db.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&version); err != nil {
		return fmt.Errorf("sqlite: read sqlite_version: %w", err)
	}
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return fmt.Errorf("novaque sqlite: unsupported SQLite version %q: novaque requires >= 3.39.0", version)
	}
	nums := make([]int, 3)
	for i := 0; i < 3; i++ {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return fmt.Errorf("novaque sqlite: unsupported SQLite version %q: novaque requires >= 3.39.0", version)
		}
		nums[i] = n
	}
	major, minor := nums[0], nums[1]
	if major < 3 || (major == 3 && minor < 39) {
		return fmt.Errorf("novaque sqlite: unsupported SQLite version %s: novaque requires >= 3.39.0", version)
	}
	return nil
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
		WHERE d.id = ? AND d.lease_token = ? AND d.status = ?`,
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
		return fmt.Errorf("sqlite: ack rejected: delivery %d lease mismatch or not in_flight", deliveryID)
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
		return fmt.Errorf("sqlite: requeue rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	if scanErr == nil {
		s.recordStat(topicID, channelID, statRequeue, 1)
	}
	return nil
}
