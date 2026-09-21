package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	"github.com/usual2970/novaque/store"
)

//go:embed schema.sql
var schemaFS embed.FS

// DefaultMaxAttempts used when publish opts leave MaxAttempts unset.
const DefaultMaxAttempts = 5

// Store is the MySQL implementation of store.Store.
type Store struct {
	db *sql.DB
}

// New wraps a caller-owned *sql.DB. Requires MySQL >= 8.0.1 (InnoDB, SKIP LOCKED).
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

var _ store.Store = (*Store)(nil)

// Migrate applies schema DDL idempotently and upgrades older delivery tables.
func (s *Store) Migrate(ctx context.Context) error {
	raw, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	stmts := splitSQL(string(raw))
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql migrate: %w\nstmt: %s", err, stmt)
		}
	}
	return s.ensureDeliveryExpiresAt(ctx)
}

func (s *Store) ensureDeliveryExpiresAt(ctx context.Context) error {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'novaque_deliveries'
		  AND COLUMN_NAME = 'expires_at'`).Scan(&n)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
		ALTER TABLE novaque_deliveries
		ADD COLUMN expires_at BIGINT NOT NULL DEFAULT 0 AFTER available_at`); err != nil {
		return fmt.Errorf("add deliveries.expires_at: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE novaque_deliveries d
		INNER JOIN novaque_messages m ON m.id = d.message_id
		SET d.expires_at = m.expires_at`); err != nil {
		return fmt.Errorf("backfill deliveries.expires_at: %w", err)
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

// EnsureTopic creates the topic if missing.
func (s *Store) EnsureTopic(ctx context.Context, name string) (int64, error) {
	if err := validateName("topic", name); err != nil {
		return 0, err
	}
	_, err := s.db.ExecContext(ctx, `INSERT IGNORE INTO novaque_topics (name) VALUES (?)`, name)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `SELECT id FROM novaque_topics WHERE name = ?`, name).Scan(&id)
	return id, err
}

// EnsureChannel creates topic+channel if missing.
func (s *Store) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	if err := validateName("channel", channel); err != nil {
		return 0, err
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		return 0, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT IGNORE INTO novaque_channels (topic_id, name) VALUES (?, ?)`, topicID, channel)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `
		SELECT id FROM novaque_channels WHERE topic_id = ? AND name = ?`, topicID, channel).Scan(&id)
	return id, err
}

// Publish inserts message + per-channel deliveries atomically for a known topic id.
func (s *Store) Publish(ctx context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	if body == nil {
		body = []byte{}
	}
	if topicID <= 0 {
		return 0, fmt.Errorf("mysql: invalid topic id %d", topicID)
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

	var messageID int64
	switch {
	case opts.TTL > 0:
		res, err := tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES (?, ?, ? + ?)`, topicID, body, nowUnix, ttlSec)
		if err != nil {
			return 0, err
		}
		messageID, err = res.LastInsertId()
		if err != nil {
			return 0, err
		}
	case !opts.ExpiresAt.IsZero():
		res, err := tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at) VALUES (?, ?, ?)`,
			topicID, body, timeToSec(opts.ExpiresAt))
		if err != nil {
			return 0, err
		}
		messageID, err = res.LastInsertId()
		if err != nil {
			return 0, err
		}
	default:
		res, err := tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES (?, ?, ? + ?)`, topicID, body, nowUnix, ttlSec)
		if err != nil {
			return 0, err
		}
		messageID, err = res.LastInsertId()
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
		SELECT ?, c.id, ?, ?, 0, ?, m.expires_at
		FROM novaque_channels c
		INNER JOIN novaque_messages m ON m.id = ?
		WHERE c.topic_id = ?`,
		messageID, store.StatusPending, availableAt, maxAttempts, messageID, topicID)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return messageID, nil
}

func newLeaseToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 32)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return string(out)
}
