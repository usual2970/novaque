//go:build integration

package testpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	once    sync.Once
	dsn     string
	errOnce error
)

// DSN starts a shared PostgreSQL 16 container (or returns a prior error).
func DSN(t *testing.T) string {
	t.Helper()
	once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		req := testcontainers.ContainerRequest{
			Image: "postgres:16",
			Env: map[string]string{
				"POSTGRES_USER":     "novaque",
				"POSTGRES_PASSWORD": "novaque",
				"POSTGRES_DB":       "novaque",
			},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithStartupTimeout(2 * time.Minute),
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			errOnce = err
			return
		}
		host, err := c.Host(ctx)
		if err != nil {
			errOnce = err
			return
		}
		port, err := c.MappedPort(ctx, "5432/tcp")
		if err != nil {
			errOnce = err
			return
		}
		dsn = fmt.Sprintf("postgres://novaque:novaque@%s:%s/novaque?sslmode=disable&TimeZone=UTC", host, port.Port())
		// Wait until it accepts connections.
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			db, err := sql.Open("pgx", dsn)
			if err == nil {
				if pingErr := db.Ping(); pingErr == nil {
					_ = db.Close()
					return
				}
				_ = db.Close()
			}
			time.Sleep(500 * time.Millisecond)
		}
		errOnce = fmt.Errorf("postgres container not ready")
	})
	if errOnce != nil {
		t.Skipf("postgres testcontainer unavailable: %v", errOnce)
	}
	return dsn
}

// Open returns a fresh *sql.DB connected to the shared container.
func Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
