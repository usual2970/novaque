//go:build integration

package testmysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	once sync.Once
	dsn  string
	errOnce error
)

// DSN starts a shared MySQL 8 container (or returns a prior error).
func DSN(t *testing.T) string {
	t.Helper()
	once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		req := testcontainers.ContainerRequest{
			Image:        "mysql:8.0.36",
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "novaque"},
			ExposedPorts: []string{"3306/tcp"},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
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
		port, err := c.MappedPort(ctx, "3306")
		if err != nil {
			errOnce = err
			return
		}
		dsn = fmt.Sprintf("root:root@tcp(%s:%s)/novaque?parseTime=true&loc=UTC&multiStatements=true", host, port.Port())
		// Wait until accept connections
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			db, err := sql.Open("mysql", dsn)
			if err == nil {
				if pingErr := db.Ping(); pingErr == nil {
					_ = db.Close()
					return
				}
				_ = db.Close()
			}
			time.Sleep(500 * time.Millisecond)
		}
		errOnce = fmt.Errorf("mysql container not ready")
	})
	if errOnce != nil {
		t.Skipf("mysql testcontainer unavailable: %v", errOnce)
	}
	return dsn
}

// Open returns a fresh *sql.DB connected to the shared container.
func Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
