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
	leaseSec := store.DurationSec(leaseFor)

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
	// below re-checks status and both time-window predicates, so a concurrent
	// claim with a stale snapshot can never re-lease; its first write fails
	// SQLITE_BUSY_SNAPSHOT (517) and the caller retries with a fresh snapshot.
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

	// One set-based lease for the whole poll slice instead of one UPDATE per
	// row. Each token is generated in the statement itself:
	// lower(hex(randomblob(16))) is the same 32-hex-char, 128-bit-per-row
	// format newLeaseToken produced in Go (randomblob uses SQLite's RNG;
	// tokens are distinct per row). The WHERE clause re-checks the full poll
	// eligibility, not just status. unixepoch() is evaluated afresh by this
	// statement, and wall time can cross a polled row's expires_at between
	// the two statements — while this UPDATE waits on the writer lock under
	// busy_timeout, or during a scheduler stall — so the time predicates must
	// run again to keep an expired delivery from being leased. expires_at is
	// NOT NULL and always populated at publish, so the same plain predicate
	// as the poll suffices. A concurrent commit that changed any of these
	// rows also invalidates this snapshot: the write then fails
	// SQLITE_BUSY_SNAPSHOT (517) and the caller retries. RETURNING id yields
	// exactly the rows actually leased; only those are loaded below.
	leaseMarks, leaseMarkArgs := idPlaceholders(ids)
	leaseArgs := make([]any, 0, len(ids)+4)
	leaseArgs = append(leaseArgs, store.StatusInFlight, owner, leaseSec)
	leaseArgs = append(leaseArgs, leaseMarkArgs...)
	leaseArgs = append(leaseArgs, store.StatusPending)
	leaseRows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		UPDATE novaque_deliveries
		SET status = ?,
		    attempts = attempts + 1,
		    lease_owner = ?,
		    lease_token = lower(hex(randomblob(16))),
		    lease_until = `+sqlNow+` + ?
		WHERE id IN (%s)
		  AND status = ?
		  AND available_at <= `+sqlNow+`
		  AND expires_at > `+sqlNow+`
		RETURNING id`, leaseMarks), leaseArgs...)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	leased := make(map[int64]struct{}, len(ids))
	for leaseRows.Next() {
		var id int64
		if err := leaseRows.Scan(&id); err != nil {
			leaseRows.Close()
			return nil, 0, 0, 0, err
		}
		leased[id] = struct{}{}
	}
	leaseRows.Close()
	if err := leaseRows.Err(); err != nil {
		return nil, 0, 0, 0, err
	}
	// Keep poll order (available_at, id): RETURNING does not promise it.
	claimedIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := leased[id]; ok {
			claimedIDs = append(claimedIDs, id)
		}
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
		if d.Attempts > d.MaxAttempts {
			deadIDs = append(deadIDs, d.ID)
			continue
		}
		byID[d.ID] = d
	}
	if err := bodyRows.Err(); err != nil {
		return nil, 0, 0, 0, err
	}

	if len(deadIDs) > 0 {
		deadMarks, deadMarkArgs := idPlaceholders(deadIDs)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE novaque_deliveries
			SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id IN (%s)`, deadMarks),
			append([]any{store.StatusDead}, deadMarkArgs...)...); err != nil {
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

// checkSQLiteVersion gates Migrate on the supported SQLite floor, 3.39.0+,
// by reading sqlite_version(). The threshold and failure wording were set
// when SKIP LOCKED was believed to exist on recent SQLite; empirically no
// SQLite release parses that clause, and on this driver the no-double-lease
// guarantee rests on snapshot isolation plus the status-guarded lease
// UPDATE. The check is kept as the mandated runtime floor; it runs on every
// Migrate (it is one cheap SELECT), so a context-canceled first attempt
// does not freeze an error for the Store's lifetime.
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

// statAttribution is the RETURNING fragment that resolves the affected row's
// stats coordinates straight from the mutation: the channel id from the
// mutated row and the topic id via a subquery on novaque_channels (NULL if
// the channel row is somehow missing — the caller treats a missing topic as
// "record no stat", matching the old INNER JOIN attribution).
const statAttribution = `
		RETURNING
			(SELECT c.topic_id FROM novaque_channels c
			 WHERE c.id = novaque_deliveries.channel_id),
			channel_id`

// Ack deletes the delivery when the lease token still matches. The guarded
// DELETE returns its stats coordinates itself (RETURNING), so ack costs one
// round trip; only a DELETE matching exactly one in_flight row records the
// event.
func (s *Store) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	var topic sql.NullInt64
	var channelID int64
	err := s.db.QueryRowContext(ctx, `
		DELETE FROM novaque_deliveries
		WHERE id = ? AND lease_token = ? AND status = ?`+statAttribution,
		deliveryID, leaseToken, store.StatusInFlight).Scan(&topic, &channelID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite: ack rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	if err != nil {
		return err
	}
	if topic.Valid {
		s.recordStat(topic.Int64, channelID, statAck, 1)
	}
	return nil
}

// Requeue returns a delivery to pending when the lease token matches. The
// guarded UPDATE returns its coordinates itself (RETURNING); only an UPDATE
// matching exactly one in_flight row records the event.
func (s *Store) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	var topic sql.NullInt64
	var channelID int64
	var err error
	if availableAt.IsZero() {
		err = s.db.QueryRowContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, available_at = `+sqlNow+`,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ? AND lease_token = ? AND status = ?`+statAttribution,
			store.StatusPending, deliveryID, leaseToken, store.StatusInFlight).Scan(&topic, &channelID)
	} else {
		err = s.db.QueryRowContext(ctx, `
			UPDATE novaque_deliveries
			SET status = ?, available_at = ?,
			    lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE id = ? AND lease_token = ? AND status = ?`+statAttribution,
			store.StatusPending, timeToSec(availableAt), deliveryID, leaseToken, store.StatusInFlight).Scan(&topic, &channelID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite: requeue rejected: delivery %d lease mismatch or not in_flight", deliveryID)
	}
	if err != nil {
		return err
	}
	if topic.Valid {
		s.recordStat(topic.Int64, channelID, statRequeue, 1)
	}
	return nil
}
