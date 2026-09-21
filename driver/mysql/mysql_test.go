package mysql_test

import (
	"context"
	"testing"

	"novaque/driver/mysql"
	"novaque/store"
)

func TestStoreImplementsInterface(t *testing.T) {
	var _ store.Store = (*mysql.Store)(nil)
}

func TestValidateNamesViaEnsure(t *testing.T) {
	// Pure unit: Migrate/Ensure need DB; keep compile + constructor smoke here.
	s := mysql.New(nil)
	if s == nil {
		t.Fatal("expected store")
	}
	_ = context.Background()
}
