package sqlite

// statKind enumerates the counter columns of novaque_stats_daily.
type statKind uint8

const (
	statPublish statKind = iota
	statClaim
	statAck
	statRequeue
	statDead
	statPurged
	numStatKinds // must stay last
)

// statCoord is the stats coordinates of one (topic, channel) pair — the day is
// deliberately absent: bucketing happens at flush time from the DB clock.
// channelID 0 is the zero-channel publish sentinel (topic-only counter, no
// channel backlog change).
type statCoord struct {
	topicID   int64
	channelID int64
}

// statKey identifies one counter cell: a statCoord plus the counter kind.
type statKey struct {
	statCoord
	kind statKind
}

// recordStat buffers a counter delta in the in-process sink. Mutators call it
// only AFTER a mutation committed (or RowsAffected confirmed success), so
// rollbacks and lease mismatches never count; the map coalesces repeated
// deltas for free. Buffering keeps every mutation transaction free of stats
// writes — FlushStats drains the sink in batches.
func (s *Store) recordStat(topicID, channelID int64, kind statKind, n int64) {
	if n == 0 || topicID <= 0 || channelID < 0 {
		return
	}
	s.statMu.Lock()
	if s.statBuf == nil {
		s.statBuf = make(map[statKey]int64)
	}
	s.statBuf[statKey{statCoord: statCoord{topicID: topicID, channelID: channelID}, kind: kind}] += n
	s.statMu.Unlock()
}
