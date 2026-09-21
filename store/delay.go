package store

import (
	"errors"
	"fmt"
	"time"
)

// MaxDelay is the maximum relative publish delay (NSQ-style deferred publish cap).
const MaxDelay = 90 * 24 * time.Hour

// DefaultPublishTTL matches the MySQL driver's fallback when TTL and ExpiresAt are unset.
const DefaultPublishTTL = 7 * 24 * time.Hour

var (
	// ErrDelayNegative is returned when Publish Delay is negative.
	ErrDelayNegative = errors.New("novaque: delay is negative")
	// ErrDelayTooLong is returned when Publish Delay exceeds MaxDelay.
	ErrDelayTooLong = errors.New("novaque: delay exceeds maximum")
	// ErrDelayExceedsTTL is returned when Delay would leave no claimable window before expiry.
	ErrDelayExceedsTTL = errors.New("novaque: delay must be less than TTL")
)

// DurationSec converts a positive duration to whole Unix seconds for TTL math.
// Sub-second positive values floor to 1 (matches historical MySQL driver behavior).
func DurationSec(d time.Duration) int64 {
	sec := int64(d / time.Second)
	if sec < 1 {
		return 1
	}
	return sec
}

// DelaySec converts Delay to whole Unix seconds. Zero or negative → 0 (immediate).
// Unlike DurationSec, sub-second positive Delay truncates to 0 (immediate ready).
func DelaySec(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(d / time.Second)
}

// ValidatePublishDelay checks Delay against MaxDelay and effective retention.
// nowUnix is used only for the absolute ExpiresAt path; TTL comparisons use second offsets only.
func ValidatePublishDelay(opts PublishOpts, nowUnix int64) error {
	if opts.Delay < 0 {
		return ErrDelayNegative
	}
	if opts.Delay > MaxDelay {
		return fmt.Errorf("%w (%v > %v)", ErrDelayTooLong, opts.Delay, MaxDelay)
	}
	delaySec := DelaySec(opts.Delay)
	if delaySec == 0 {
		return nil
	}

	var expiresAtUnix int64
	switch {
	case opts.TTL > 0:
		expiresAtUnix = nowUnix + DurationSec(opts.TTL)
	case !opts.ExpiresAt.IsZero():
		expiresAtUnix = opts.ExpiresAt.UTC().Unix()
	default:
		expiresAtUnix = nowUnix + DurationSec(DefaultPublishTTL)
	}

	if nowUnix+delaySec >= expiresAtUnix {
		return ErrDelayExceedsTTL
	}
	return nil
}
