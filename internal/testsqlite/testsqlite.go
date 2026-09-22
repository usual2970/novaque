//go:build integration

package testsqlite

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// Open returns a *sql.DB backed by a fresh temp-file SQLite database. The DSN
// enables FK enforcement, WAL journaling, and a 10s busy timeout; the DB and
// its files are removed with t.TempDir()'s cleanup.
func Open(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "novaque.db") +
		"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
