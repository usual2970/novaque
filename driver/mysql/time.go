package mysql

import "time"

// sqlNow is the database clock as Unix seconds (SIGNED BIGINT).
const sqlNow = "UNIX_TIMESTAMP()"

func durationSec(d time.Duration) int64 {
	sec := int64(d / time.Second)
	if sec < 1 {
		return 1
	}
	return sec
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
