//go:build integration

package testsqlite

import (
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"

	_ "modernc.org/sqlite"
)

// Path returns a SQLite file URI for a database inside a fresh t.TempDir()
// (the file is not created). Pass the same path to repeated OpenPath calls to
// open one database file through several *sql.DB handles, each with its own
// connection pool but identical DSN pragmas.
func Path(t *testing.T) string {
	t.Helper()
	return "file:" + filepath.Join(t.TempDir(), "novaque.db")
}

// OpenPath opens (creating on first use) the database at the file URI from
// Path. The DSN enables FK enforcement and WAL journaling; busyMS sets the
// busy timeout in milliseconds — small values let failure-injection tests
// observe SQLITE_BUSY without waiting on the default 10s.
func OpenPath(t *testing.T, path string, busyMS int) *sql.DB {
	t.Helper()
	dsn := path +
		"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(" +
		strconv.Itoa(busyMS) + ")"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Open returns a *sql.DB backed by a fresh temp-file SQLite database. The DSN
// enables FK enforcement, WAL journaling, and a 10s busy timeout; the DB and
// its files are removed with t.TempDir()'s cleanup.
func Open(t *testing.T) *sql.DB {
	t.Helper()
	return OpenPath(t, Path(t), 10000)
}
