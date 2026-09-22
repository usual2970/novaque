package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/usual2970/novaque/store"
)

// The methods below are temporary U1 stubs; later implementation units
// replace them one file at a time (publish/claim, maintenance, stats, admin).
// Every stub returns an error rather than panicking or faking success.

func (s *Store) Publish(ctx context.Context, topicID int64, body []byte, opts store.PublishOpts) (int64, error) {
	return 0, fmt.Errorf("sqlite: Publish not implemented yet")
}

func (s *Store) Claim(ctx context.Context, channelID int64, owner string, leaseFor time.Duration, limit int) ([]store.Delivery, error) {
	return nil, fmt.Errorf("sqlite: Claim not implemented yet")
}

func (s *Store) Ack(ctx context.Context, deliveryID int64, leaseToken string) error {
	return fmt.Errorf("sqlite: Ack not implemented yet")
}

func (s *Store) Requeue(ctx context.Context, deliveryID int64, leaseToken string, availableAt time.Time) error {
	return fmt.Errorf("sqlite: Requeue not implemented yet")
}

func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int64, error) {
	return 0, fmt.Errorf("sqlite: ReapExpiredLeases not implemented yet")
}

func (s *Store) PurgeExpired(ctx context.Context, limit int) (int64, error) {
	return 0, fmt.Errorf("sqlite: PurgeExpired not implemented yet")
}

func (s *Store) ChannelCounters(ctx context.Context, channelID int64) (store.ChannelCounters, error) {
	return store.ChannelCounters{}, fmt.Errorf("sqlite: ChannelCounters not implemented yet")
}

func (s *Store) TopicCounters(ctx context.Context, topicID int64) (store.ChannelCounters, error) {
	return store.ChannelCounters{}, fmt.Errorf("sqlite: TopicCounters not implemented yet")
}

func (s *Store) ChannelBacklog(ctx context.Context, channelID int64) (store.ChannelBacklog, error) {
	return store.ChannelBacklog{}, fmt.Errorf("sqlite: ChannelBacklog not implemented yet")
}

func (s *Store) PruneStats(ctx context.Context, retentionDays int) (int64, error) {
	return 0, fmt.Errorf("sqlite: PruneStats not implemented yet")
}

func (s *Store) FlushStats(ctx context.Context) error {
	return fmt.Errorf("sqlite: FlushStats not implemented yet")
}

func (s *Store) ListTopics(ctx context.Context) ([]store.TopicInfo, error) {
	return nil, fmt.Errorf("sqlite: ListTopics not implemented yet")
}

func (s *Store) ListChannels(ctx context.Context) ([]store.ChannelInfo, error) {
	return nil, fmt.Errorf("sqlite: ListChannels not implemented yet")
}

func (s *Store) Backlogs(ctx context.Context) ([]store.BacklogRow, error) {
	return nil, fmt.Errorf("sqlite: Backlogs not implemented yet")
}

func (s *Store) BacklogsForTopic(ctx context.Context, topicID int64) ([]store.BacklogRow, error) {
	return nil, fmt.Errorf("sqlite: BacklogsForTopic not implemented yet")
}

func (s *Store) TopicDailyCounters(ctx context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	return nil, fmt.Errorf("sqlite: TopicDailyCounters not implemented yet")
}

func (s *Store) ChannelDailyCounters(ctx context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	return nil, fmt.Errorf("sqlite: ChannelDailyCounters not implemented yet")
}

func (s *Store) ListDead(ctx context.Context, channelID int64, before int64, limit int, bodyPrefix int) ([]store.DeadDelivery, error) {
	return nil, fmt.Errorf("sqlite: ListDead not implemented yet")
}

func (s *Store) RequeueDead(ctx context.Context, deliveryID, channelID int64, freshTTL time.Duration) error {
	return fmt.Errorf("sqlite: RequeueDead not implemented yet")
}

func (s *Store) DeleteDead(ctx context.Context, deliveryID, channelID int64) error {
	return fmt.Errorf("sqlite: DeleteDead not implemented yet")
}

func (s *Store) DeleteTopic(ctx context.Context, topicID int64) error {
	return fmt.Errorf("sqlite: DeleteTopic not implemented yet")
}

func (s *Store) DeleteChannel(ctx context.Context, channelID int64) error {
	return fmt.Errorf("sqlite: DeleteChannel not implemented yet")
}
