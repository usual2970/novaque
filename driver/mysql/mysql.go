package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	"novaque/store"
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

// Migrate applies schema DDL idempotently.
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

// Publish inserts message + per-channel deliveries atomically.
func (s *Store) Publish(ctx context.Context, topic string, body []byte, opts store.PublishOpts) (int64, error) {
	if body == nil {
		body = []byte{}
	}
	topicID, err := s.EnsureTopic(ctx, topic)
	if err != nil {
		return 0, err
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

	var messageID int64
	if !opts.ExpiresAt.IsZero() {
		res, err := tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at) VALUES (?, ?, ?)`,
			topicID, body, opts.ExpiresAt.UTC())
		if err != nil {
			return 0, err
		}
		messageID, err = res.LastInsertId()
		if err != nil {
			return 0, err
		}
	} else {
		res, err := tx.ExecContext(ctx, `
			INSERT INTO novaque_messages (topic_id, body, expires_at)
			VALUES (?, ?, DATE_ADD(NOW(3), INTERVAL 7 DAY))`, topicID, body)
		if err != nil {
			return 0, err
		}
		messageID, err = res.LastInsertId()
		if err != nil {
			return 0, err
		}
	}

	rows, err := tx.QueryContext(ctx, `SELECT id FROM novaque_channels WHERE topic_id = ?`, topicID)
	if err != nil {
		return 0, err
	}
	var channelIDs []int64
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			rows.Close()
			return 0, err
		}
		channelIDs = append(channelIDs, channelID)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, channelID := range channelIDs {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO novaque_deliveries
			  (message_id, channel_id, status, available_at, attempts, max_attempts)
			VALUES (?, ?, ?, NOW(3), 0, ?)`,
			messageID, channelID, store.StatusPending, maxAttempts)
		if err != nil {
			return 0, err
		}
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
