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
