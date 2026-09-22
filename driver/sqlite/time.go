package sqlite

import (
	"time"
)

// sqlNow is the database clock as Unix seconds.
const sqlNow = "unixepoch()"

func timeToSec(t time.Time) int64 {
	return t.UTC().Unix()
}

func secToTime(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}
