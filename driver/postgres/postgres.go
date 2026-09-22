// Package postgres implements store.Store on PostgreSQL 14+. Claiming uses
// FOR UPDATE SKIP LOCKED so multiple processes compete safely within a
// channel. The schema is embedded and applied idempotently by Migrate.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/usual2970/novaque/store"
)

//go:embed schema.sql
var schemaFS embed.FS

// DefaultMaxAttempts is the default used when publish opts leave MaxAttempts unset.
const DefaultMaxAttempts = 5

// Store is the PostgreSQL implementation of store.Store.
type Store struct {
	db *sql.DB

	// statMu guards statBuf, the in-process counter sink: mutators buffer
	// deltas here only after a commit, and FlushStats drains them in batches,
	// so no mutation transaction ever carries a stats write. The sink is
	// wired in a later unit; the fields live here from the start.
	statMu  sync.Mutex
	statBuf map[statKey]int64
}

// statKey identifies one counter cell: a topic/channel pair plus the counter
// kind. The buffering and flush are added by the stats unit.
type statKey struct {
	topicID   int64
	channelID int64
	kind      string
}

// New wraps a caller-owned *sql.DB. Requires PostgreSQL >= 14 (SKIP LOCKED).
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

var _ store.Store = (*Store)(nil)

// Migrate applies schema DDL idempotently. The first run verifies the
// server version floor (PostgreSQL 14) and pins the session to UTC before
// applying the embedded schema statement by statement.
func (s *Store) Migrate(ctx context.Context) error {
	var versionText string
	if err := s.db.QueryRowContext(ctx, `SHOW server_version_num`).Scan(&versionText); err != nil {
		return fmt.Errorf("postgres: query server version: %w", err)
	}
	num, err := strconv.Atoi(versionText)
	if err != nil {
		return fmt.Errorf("postgres: parse server_version_num %q: %w", versionText, err)
	}
	if err := checkVersion(num); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `SET TIME ZONE 'UTC'`); err != nil {
		return fmt.Errorf("postgres: set time zone UTC: %w", err)
	}
	raw, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	stmts := splitSQL(string(raw))
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres migrate: %w\nstmt: %s", err, stmt)
		}
	}
	return nil
}

// checkVersion enforces the PostgreSQL 14 floor (KTD2). num is a
// server_version_num value (major*10000 + minor*100 + patch).
func checkVersion(num int) error {
	const minVersion = 140000
	if num < minVersion {
		return fmt.Errorf("postgres: server version %d is below the required minimum %d (PostgreSQL 14+)", num, minVersion)
	}
	return nil
}

func splitSQL(s string) []string {
	var out []string
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
		if strings.HasSuffix(trim, ";") {
			stmt := strings.TrimSpace(b.String())
			stmt = strings.TrimSuffix(stmt, ";")
			if stmt != "" {
				out = append(out, stmt)
			}
			b.Reset()
		}
	}
	if rest := strings.TrimSpace(b.String()); rest != "" {
		out = append(out, rest)
	}
	return out
}

func validateName(kind, name string) error {
	if n := len(name); n < 1 || n > 64 {
		return fmt.Errorf("invalid %s name length %d (want 1..64)", kind, n)
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if !ok {
			return fmt.Errorf("invalid %s name %q", kind, name)
		}
	}
	return nil
}

// idPlaceholders renders an IN (...) placeholder list plus its bound id args.
func idPlaceholders(ids []int64) (marks string, args []any) {
	placeholders := make([]string, len(ids))
	args = make([]any, 0, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args = append(args, id)
	}
	return strings.Join(placeholders, ","), args
}

// errNotImplemented marks store.Store methods whose bodies arrive in later
// units. U1 only implements Migrate; every other method fails loudly rather
// than faking behavior.
func errNotImplemented(method string) error {
	return fmt.Errorf("postgres: %s not implemented yet", method)
}

// EnsureTopic creates the topic if missing. One round trip: ON CONFLICT with a
// tautological SET lets RETURNING yield the existing id when the name already
// exists (Postgres equivalent of MySQL's INSERT IGNORE then SELECT).
func (s *Store) EnsureTopic(ctx context.Context, name string) (int64, error) {
	if err := validateName("topic", name); err != nil {
		return 0, err
	}
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO novaque_topics (name) VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id`, name).Scan(&id)
	return id, err
}

// EnsureChannel creates topic+channel if missing. Same upsert-returning idiom
// as EnsureTopic, keyed on (topic_id, name).
func (s *Store) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	if err := validateName("channel", channel); err != nil {
		return 0, err
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO novaque_channels (topic_id, name) VALUES ($1, $2)
		ON CONFLICT (topic_id, name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id`, topicID, channel).Scan(&id)
	return id, err
}

// pgErrForeignKeyViolation is PostgreSQL SQLSTATE 23503
// (foreign_key_violation): an INSERT referenced a parent row that does not
// exist — for Publish, a topic id another process deleted after this one
// resolved it.
const pgErrForeignKeyViolation = "23503"

// mapTopicGone translates Publish's topic-FK violation into the
// dialect-agnostic store.ErrTopicGone sentinel, matching the PostgreSQL
// SQLSTATE code only (never message strings). Every other error passes
// through untouched.
func mapTopicGone(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgErrForeignKeyViolation {
		return store.ErrTopicGone
	}
	return err
}

// Publish inserts message + per-channel deliveries atomically for a known
// topic id. A nil body is stored as empty. When the topic row vanished after
// the caller resolved topicID (an admin delete in another process), the topic
// FK fails with PostgreSQL SQLSTATE 23503 and Publish returns
// store.ErrTopicGone; callers evict the memoized id, re-Ensure the name, and
// retry once.
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
		return 0, fmt.Errorf("postgres: invalid topic id %d", topicID)
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
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

	ttlSec := store.DurationSec(store.DefaultPublishTTL)
	switch {
	case opts.TTL > 0:
		ttlSec = store.DurationSec(opts.TTL)
	case !opts.ExpiresAt.IsZero():
		ttlSec = 0 // absolute path below
	}

	// pgx stdlib does not support sql.Result.LastInsertId, so every branch
	// inserts ... RETURNING id and scans it.
	var messageID int64
	switch {
	case opts.TTL > 0:
		err = tx.QueryRowContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES ($1, $2, $3::bigint + $4::bigint) RETURNING id`,
			topicID, body, nowUnix, ttlSec).Scan(&messageID)
		if err != nil {
			return 0, err
		}
	case !opts.ExpiresAt.IsZero():
		err = tx.QueryRowContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES ($1, $2, $3) RETURNING id`,
			topicID, body, timeToSec(opts.ExpiresAt)).Scan(&messageID)
		if err != nil {
			return 0, err
		}
	default:
		err = tx.QueryRowContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES ($1, $2, $3::bigint + $4::bigint) RETURNING id`,
			topicID, body, nowUnix, ttlSec).Scan(&messageID)
		if err != nil {
			return 0, err
		}
	}

	delaySec := store.DelaySec(opts.Delay)
	availableAt := nowUnix + delaySec

	// Single-statement fan-out; copy message expires_at onto each delivery (claim hot path).
	_, err = tx.ExecContext(ctx, `
		INSERT INTO novaque_deliveries
		  (message_id, channel_id, status, available_at, attempts, max_attempts, expires_at)
		SELECT $1, c.id, $2, $3, 0, $4, m.expires_at
		FROM novaque_channels c
		INNER JOIN novaque_messages m ON m.id = $5
		WHERE c.topic_id = $6`,
		messageID, store.StatusPending, availableAt, maxAttempts, messageID, topicID)
	if err != nil {
		return 0, err
	}

	// Read-only attribution for stats (no extra writes in this tx): the
	// delivery rows were just inserted above, so the fan-out is exact here.
	attrRows, err := tx.QueryContext(ctx, `
		SELECT channel_id FROM novaque_deliveries WHERE message_id = $1`, messageID)
	if err != nil {
		return 0, err
	}
	fanout := make(map[int64]int64) // channelID -> deliveries (one per channel)
	for attrRows.Next() {
		var chID int64
		if err := attrRows.Scan(&chID); err != nil {
			attrRows.Close()
			return 0, err
		}
		fanout[chID]++
	}
	attrRows.Close()
	if err := attrRows.Err(); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	// stats (U5): publish counters buffered after commit, mirroring driver/mysql.
	// The fanout attribution above feeds those buffered counters; zero-channel
	// publishes land on the channel_id=0 sentinel row.
	return messageID, nil
}

// ChannelCounters sums the retained day-bucket event counters for one channel.
func (s *Store) ChannelCounters(ctx context.Context, channelID int64) (store.ChannelCounters, error) {
	return store.ChannelCounters{}, errNotImplemented("ChannelCounters")
}

// TopicCounters rolls up the retained day-bucket event counters for a topic.
func (s *Store) TopicCounters(ctx context.Context, topicID int64) (store.ChannelCounters, error) {
	return store.ChannelCounters{}, errNotImplemented("TopicCounters")
}

// ChannelBacklog returns live pending / ready / in_flight / dead counts for one channel.
func (s *Store) ChannelBacklog(ctx context.Context, channelID int64) (store.ChannelBacklog, error) {
	return store.ChannelBacklog{}, errNotImplemented("ChannelBacklog")
}

// PruneStats deletes day-bucket counter rows older than retentionDays.
func (s *Store) PruneStats(ctx context.Context, retentionDays int) (int64, error) {
	return 0, errNotImplemented("PruneStats")
}

// FlushStats drains any buffered counter deltas into the stats table.
func (s *Store) FlushStats(ctx context.Context) error {
	return errNotImplemented("FlushStats")
}

// ListTopics returns every topic, ascending by name.
func (s *Store) ListTopics(ctx context.Context) ([]store.TopicInfo, error) {
	return nil, errNotImplemented("ListTopics")
}

// ListChannels returns every channel across all topics, topic name then channel name.
func (s *Store) ListChannels(ctx context.Context) ([]store.ChannelInfo, error) {
	return nil, errNotImplemented("ListChannels")
}

// Backlogs returns live per-channel backlog counts for every channel in one query.
func (s *Store) Backlogs(ctx context.Context) ([]store.BacklogRow, error) {
	return nil, errNotImplemented("Backlogs")
}

// BacklogsForTopic scopes the batched backlog aggregate to one topic's channels.
func (s *Store) BacklogsForTopic(ctx context.Context, topicID int64) ([]store.BacklogRow, error) {
	return nil, errNotImplemented("BacklogsForTopic")
}

// TopicDailyCounters returns the topic's per-day counter rows over the trailing window.
func (s *Store) TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	return nil, errNotImplemented("TopicDailyCounters")
}

// ChannelDailyCounters returns one channel's per-day counter rows over the trailing window.
func (s *Store) ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	return nil, errNotImplemented("ChannelDailyCounters")
}

// ListDead returns a channel's dead deliveries newest-first, keyset-paginated.
func (s *Store) ListDead(ctx context.Context, channelID int64, before int64, limit int, bodyPrefix int) ([]store.DeadDelivery, error) {
	return nil, errNotImplemented("ListDead")
}

// RequeueDead returns one dead delivery to pending with a fresh expiry.
func (s *Store) RequeueDead(ctx context.Context, deliveryID, channelID int64, freshTTL time.Duration) error {
	return errNotImplemented("RequeueDead")
}

// DeleteDead removes one dead delivery.
func (s *Store) DeleteDead(ctx context.Context, deliveryID, channelID int64) error {
	return errNotImplemented("DeleteDead")
}

// DeleteTopic removes the topic and everything under it in one transaction.
func (s *Store) DeleteTopic(ctx context.Context, topicID int64) error {
	return errNotImplemented("DeleteTopic")
}

// DeleteChannel removes one channel's deliveries and stats rows.
func (s *Store) DeleteChannel(ctx context.Context, channelID int64) error {
	return errNotImplemented("DeleteChannel")
}
