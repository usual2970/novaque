package store

import (
	"errors"
	"testing"
	"time"
)

func TestValidatePublishDelay(t *testing.T) {
	now := int64(1_700_000_000)

	t.Run("zero and omitted ok", func(t *testing.T) {
		if err := ValidatePublishDelay(PublishOpts{}, now); err != nil {
			t.Fatal(err)
		}
		if err := ValidatePublishDelay(PublishOpts{Delay: 0, TTL: time.Hour}, now); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("negative", func(t *testing.T) {
		err := ValidatePublishDelay(PublishOpts{Delay: -time.Second, TTL: time.Hour}, now)
		if !errors.Is(err, ErrDelayNegative) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("over max", func(t *testing.T) {
		err := ValidatePublishDelay(PublishOpts{Delay: MaxDelay + time.Second, TTL: 91 * 24 * time.Hour}, now)
		if !errors.Is(err, ErrDelayTooLong) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("max delay with longer ttl ok", func(t *testing.T) {
		err := ValidatePublishDelay(PublishOpts{Delay: MaxDelay, TTL: 91 * 24 * time.Hour}, now)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delay exceeds ttl seconds", func(t *testing.T) {
		err := ValidatePublishDelay(PublishOpts{Delay: 8 * 24 * time.Hour, TTL: 7 * 24 * time.Hour}, now)
		if !errors.Is(err, ErrDelayExceedsTTL) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("same second collapse", func(t *testing.T) {
		// Delay=2s truncates to 2; TTL=2500ms DurationSec → 2; 2 < 2 false.
		err := ValidatePublishDelay(PublishOpts{Delay: 2 * time.Second, TTL: 2500 * time.Millisecond}, now)
		if !errors.Is(err, ErrDelayExceedsTTL) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("delay ok with ttl", func(t *testing.T) {
		err := ValidatePublishDelay(PublishOpts{Delay: time.Hour, TTL: 2 * time.Hour}, now)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("default ttl when unset", func(t *testing.T) {
		err := ValidatePublishDelay(PublishOpts{Delay: 8 * 24 * time.Hour}, now)
		if !errors.Is(err, ErrDelayExceedsTTL) {
			t.Fatalf("got %v want ErrDelayExceedsTTL", err)
		}
	})
}

func TestDelaySec(t *testing.T) {
	if got := DelaySec(0); got != 0 {
		t.Fatalf("got %d", got)
	}
	if got := DelaySec(500 * time.Millisecond); got != 0 {
		t.Fatalf("sub-second got %d", got)
	}
	if got := DelaySec(2 * time.Second); got != 2 {
		t.Fatalf("got %d", got)
	}
}
