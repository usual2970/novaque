package mysql

import (
	"time"

	"github.com/usual2970/novaque/store"
)

// sqlNow is the database clock as Unix seconds (SIGNED BIGINT).
const sqlNow = "UNIX_TIMESTAMP()"

func durationSec(d time.Duration) int64 {
	return store.DurationSec(d)
}

func timeToSec(t time.Time) int64 {
	return t.UTC().Unix()
}

func secToTime(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}
