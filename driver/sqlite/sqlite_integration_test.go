//go:build integration

package sqlite_test

import (
	"context"
	"testing"

	"github.com/usual2970/novaque/driver/sqlite"
	"github.com/usual2970/novaque/internal/testsqlite"
)

func TestMigrateIdempotentAndUniqueChannel(t *testing.T) {
	db := testsqlite.Open(t)
	s := sqlite.New(db)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureChannel(ctx, "t1", "c1"); err != nil {
		t.Fatal(err)
	}
	id2, err := s.EnsureChannel(ctx, "t1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	id1, err := s.EnsureChannel(ctx, "t1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("expected same channel id, got %d vs %d", id1, id2)
	}
}
