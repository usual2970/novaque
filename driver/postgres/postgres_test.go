package postgres

import (
	"context"
	"testing"

	"github.com/usual2970/novaque/store"
)

func TestStoreImplementsInterface(t *testing.T) {
	var _ store.Store = (*Store)(nil)
}

func TestNewSmoke(t *testing.T) {
	// Pure unit: Migrate needs DB; keep compile + constructor smoke here.
	s := New(nil)
	if s == nil {
		t.Fatal("expected store")
	}
	_ = context.Background()
}

// TestCheckVersion covers the PostgreSQL 14 floor at and around the
// boundary; numbers are server_version_num values (major*10000 + minor).
func TestCheckVersion(t *testing.T) {
	cases := []struct {
		name    string
		num     int
		wantErr bool
	}{
		{name: "pg 13 latest patch", num: 139999, wantErr: true},
		{name: "pg 14 floor", num: 140000, wantErr: false},
		{name: "pg 16", num: 160000, wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkVersion(tc.num)
			if tc.wantErr && err == nil {
				t.Fatalf("checkVersion(%d) = nil, want error", tc.num)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("checkVersion(%d) = %v, want nil", tc.num, err)
			}
		})
	}
}
