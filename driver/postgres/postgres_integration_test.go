//go:build integration

package postgres_test

import (
	"context"
	"sort"
	"testing"

	"github.com/usual2970/novaque/driver/postgres"
	"github.com/usual2970/novaque/internal/testpostgres"
)

// TestMigrateIdempotent runs Migrate twice in a row on the same database;
// every DDL statement is IF NOT EXISTS so the second run must be a no-op.
func TestMigrateIdempotent(t *testing.T) {
	db := testpostgres.Open(t)
	ctx := context.Background()
	s := postgres.New(db)

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

// TestMigrateCreatesTables verifies the five parity tables exist after Migrate.
func TestMigrateCreatesTables(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	rows, err := db.Query(`
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close tables query: %v", err)
	}
	sort.Strings(got)

	want := []string{
		"novaque_channels",
		"novaque_deliveries",
		"novaque_messages",
		"novaque_stats_daily",
		"novaque_topics",
	}
	if len(got) != len(want) {
		t.Fatalf("tables = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables = %v, want %v", got, want)
		}
	}
}

// TestMigrateCreatesIndexes verifies the claim/expires/stats indexes exist.
func TestMigrateCreatesIndexes(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	rows, err := db.Query(`
		SELECT indexname FROM pg_catalog.pg_indexes
		WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	got := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		got[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close indexes query: %v", err)
	}

	want := []string{
		"idx_novaque_deliveries_claim",
		"idx_novaque_messages_expires",
		"idx_novaque_stats_daily_channel",
		"idx_novaque_stats_daily_topic",
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("index %q missing; have %v", name, got)
		}
	}
}

// TestMigrateSessionUTC verifies sessions on the helper DSN run in UTC so the
// UTC day buckets match the MySQL driver.
func TestMigrateSessionUTC(t *testing.T) {
	db := testpostgres.Open(t)
	s := postgres.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var tz string
	if err := db.QueryRow(`SHOW TIME ZONE`).Scan(&tz); err != nil {
		t.Fatalf("show time zone: %v", err)
	}
	if tz != "UTC" {
		t.Fatalf("session time zone = %q, want UTC", tz)
	}
}
