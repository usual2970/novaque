package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sqlite3 "modernc.org/sqlite"

	"github.com/usual2970/novaque/store"
)

// sqliteErrConstraintForeignKey is SQLITE_CONSTRAINT_FOREIGNKEY (extended
// code 787): an INSERT violated a foreign key — for Publish, a topic id
// another process deleted after this one resolved it. modernc enables
// extended result codes per connection, so (*sqlite.Error).Code() reports
// 787 rather than the primary SQLITE_CONSTRAINT (19). The numeric code is
// declared here because it is not exported from the top-level
// modernc.org/sqlite package (only from its lib subpackage).
const sqliteErrConstraintForeignKey = 787

// mapTopicGone translates Publish's topic-FK violation into the
// dialect-agnostic store.ErrTopicGone sentinel, matching the SQLite extended
// error code only (never message strings). Every other error passes through
// untouched.
func mapTopicGone(err error) error {
	var e *sqlite3.Error
	if errors.As(err, &e) && e.Code() == sqliteErrConstraintForeignKey {
		return store.ErrTopicGone
	}
	return err
}

// Publish inserts message + per-channel deliveries atomically for a known
// topic id. A nil body is stored as empty. When the topic row vanished after
// the caller resolved topicID (an admin delete in another process), the topic
// FK fails with SQLITE_CONSTRAINT_FOREIGNKEY and Publish returns
// store.ErrTopicGone; callers evict the memoized id, re-Ensure the name, and
// retry once (KTD9).
func (s *Store) Publish(ctx context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	messageID, err := s.publish(ctx, topicID, body, opts)
	if err != nil {
		return 0, mapTopicGone(err)
	}
	return messageID, nil
}

func (s *Store) publish(ctx context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	if body == nil {
		body = []byte{}
	}
	if topicID <= 0 {
		return 0, fmt.Errorf("sqlite: invalid topic id %d", topicID)
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	// SQLite transactions are always serializable; BeginTx with nil options
	// — modernc rejects unsupported isolation levels (e.g. ReadCommitted).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var nowUnix int64
	if err := tx.QueryRowContext(ctx, `SELECT `+sqlNow).Scan(&nowUnix); err != nil {
		return 0, err
	}
	if err := store.ValidatePublishDelay(opts, nowUnix); err != nil {
		return 0, err
	}

	// TTL decision made once: an explicit TTL overrides the default; an
	// absolute ExpiresAt (with no TTL) takes the absolute INSERT below.
	ttlSec := store.DurationSec(store.DefaultPublishTTL)
	if opts.TTL > 0 {
		ttlSec = store.DurationSec(opts.TTL)
	}

	var messageID int64
	var res sql.Result
	if opts.TTL <= 0 && !opts.ExpiresAt.IsZero() {
		res, err = tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at) VALUES (?, ?, ?)`,
			topicID, body, timeToSec(opts.ExpiresAt))
	} else {
		res, err = tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES (?, ?, ? + ?)`, topicID, body, nowUnix, ttlSec)
	}
	if err != nil {
		return 0, err
	}
	messageID, err = res.LastInsertId()
	if err != nil {
		return 0, err
	}

	delaySec := store.DelaySec(opts.Delay)
	availableAt := nowUnix + delaySec

	// Single-statement fan-out; copy message expires_at onto each delivery
	// (claim hot path). RETURNING gives the inserted channels straight from
	// the INSERT ... SELECT, so stats attribution needs no separate read-back
	// round trip: one returned row per channel.
	fanRows, err := tx.QueryContext(ctx, `
		INSERT INTO novaque_deliveries
		  (message_id, channel_id, status, available_at, attempts, max_attempts, expires_at)
		SELECT ?, c.id, ?, ?, 0, ?, m.expires_at
		FROM novaque_channels c
		INNER JOIN novaque_messages m ON m.id = ?
		WHERE c.topic_id = ?
		RETURNING channel_id`,
		messageID, store.StatusPending, availableAt, maxAttempts, messageID, topicID)
	if err != nil {
		return 0, err
	}
	var fanout []int64
	for fanRows.Next() {
		var chID int64
		if err := fanRows.Scan(&chID); err != nil {
			fanRows.Close()
			return 0, err
		}
		fanout = append(fanout, chID)
	}
	fanRows.Close()
	if err := fanRows.Err(); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	// Counters bump only after the commit succeeded, so a rolled-back
	// mutation records nothing. The publish unit is one delivery per channel;
	// zero-channel publishes land on the channel_id=0 sentinel row (a
	// topic-only counter with no channel backlog change).
	if len(fanout) == 0 {
		s.recordStat(topicID, 0, statPublish, 1)
	} else {
		for _, chID := range fanout {
			s.recordStat(topicID, chID, statPublish, 1)
		}
	}
	return messageID, nil
}
