// Package postgres implements store.Store on PostgreSQL 14+. Claiming uses
// FOR UPDATE SKIP LOCKED so multiple processes compete safely within a
// channel. The schema is embedded and applied idempotently by Migrate.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

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

// EnsureTopic creates the topic if missing.
func (s *Store) EnsureTopic(ctx context.Context, name string) (int64, error) {
	return 0, errNotImplemented("EnsureTopic")
}

// EnsureChannel creates topic+channel if missing.
func (s *Store) EnsureChannel(ctx context.Context, topic, channel string) (int64, error) {
	return 0, errNotImplemented("EnsureChannel")
}

// Publish inserts message + per-channel deliveries atomically for a known topic id.
func (s *Store) Publish(ctx context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	return 0, errNotImplemented("Publish")
}

// Claim leases up to limit eligible deliveries for a known channel id.
func (s *Store) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]store.Delivery, error) {
	return nil, errNotImplemented("Claim")
}

// Ack completes a delivery when lease_token still matches.
func (s *Store) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	return errNotImplemented("Ack")
}

// Requeue returns an in-flight delivery to pending when lease_token matches.
func (s *Store) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	return errNotImplemented("Requeue")
}

// ReapExpiredLeases resets in_flight rows whose lease_until is in the past.
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	return 0, errNotImplemented("ReapExpiredLeases")
}

// PurgeExpired deletes expired messages and their non-live deliveries.
func (s *Store) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	return 0, errNotImplemented("PurgeExpired")
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
