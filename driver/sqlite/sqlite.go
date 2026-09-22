// Package sqlite implements store.Store on SQLite. The schema is embedded and
// applied idempotently by Migrate. The hot path uses Unix-second clocks
// (unixepoch()); a file database should be opened with WAL, foreign_keys, and
// a busy timeout (see internal/testsqlite for the recommended DSN).
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"sync"

	"github.com/usual2970/novaque/store"
)

//go:embed schema.sql
var schemaFS embed.FS

// DefaultMaxAttempts is the default used when publish opts leave MaxAttempts unset.
const DefaultMaxAttempts = 5

// Store is the SQLite implementation of store.Store.
type Store struct {
	db *sql.DB

	// statMu guards statBuf, the in-process counter sink: mutators buffer
	// deltas here only after a commit, and FlushStats drains them in batches,
	// so no mutation transaction ever carries a stats write.
	statMu  sync.Mutex
	statBuf map[statKey]int64
}

// New wraps a caller-owned *sql.DB.
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
			return fmt.Errorf("sqlite migrate: %w\nstmt: %s", err, stmt)
		}
	}
	// Fail fast if the runtime SQLite is older than the supported floor,
	// 3.39.0 (KTD2, OQ2).
	return s.checkSQLiteVersion(ctx)
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
		placeholders[i] = "?"
		args = append(args, id)
	}
	return strings.Join(placeholders, ","), args
}

// EnsureTopic creates the topic if missing.
func (s *Store) EnsureTopic(ctx context.Context, name string) (int64, error) {
	if err := validateName("topic", name); err != nil {
		return 0, err
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO novaque_topics (name) VALUES (?)`, name)
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
		INSERT OR IGNORE INTO novaque_channels (topic_id, name) VALUES (?, ?)`, topicID, channel)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `
		SELECT id FROM novaque_channels WHERE topic_id = ? AND name = ?`, topicID, channel).Scan(&id)
	return id, err
}
