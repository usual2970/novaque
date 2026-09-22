package mysql

// Compile-only stubs for the admin surface of store.Store (mountable admin UI
// plan U1). They exist so the interface assertion in mysql.go keeps the
// driver compiling the moment store.Store grows; unit U2 replaces every one
// of them with real SQL. Behavior until then: listings are empty, the guarded
// dead ops report no matching dead row, and deletes succeed as the idempotent
// no-ops an already-deleted id would produce.

import (
	"context"
	"time"

	"github.com/usual2970/novaque/store"
)

// ListTopics is a stub until U2; returns no topics.
func (s *Store) ListTopics(ctx context.Context) ([]store.TopicInfo, error) {
	return nil, nil
}

// ListChannels is a stub until U2; returns no channels.
func (s *Store) ListChannels(ctx context.Context) ([]store.ChannelInfo, error) {
	return nil, nil
}

// Backlogs is a stub until U2; returns no backlog rows.
func (s *Store) Backlogs(ctx context.Context) ([]store.BacklogRow, error) {
	return nil, nil
}

// TopicDailyCounters is a stub until U2; returns no day rows.
func (s *Store) TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	return nil, nil
}

// ChannelDailyCounters is a stub until U2; returns no day rows.
func (s *Store) ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	return nil, nil
}

// ListDead is a stub until U2; returns no dead deliveries.
func (s *Store) ListDead(ctx context.Context, channelID int64, before int64, limit int) ([]store.DeadDelivery, error) {
	return nil, nil
}

// RequeueDead is a stub until U2; reports the guarded row as gone.
func (s *Store) RequeueDead(ctx context.Context, deliveryID int64, freshTTL time.Duration) error {
	return store.ErrDeadGone
}

// DeleteDead is a stub until U2; reports the guarded row as gone.
func (s *Store) DeleteDead(ctx context.Context, deliveryID int64) error {
	return store.ErrDeadGone
}

// DeleteTopic is a stub until U2; succeeds as an idempotent no-op.
func (s *Store) DeleteTopic(ctx context.Context, topicID int64) error {
	return nil
}

// DeleteChannel is a stub until U2; succeeds as an idempotent no-op.
func (s *Store) DeleteChannel(ctx context.Context, channelID int64) error {
	return nil
}
