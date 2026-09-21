# novaque

Go library for **NSQ-style pub/sub** on a relational database. Callers supply a DB connection; novaque embeds into your process.

- **Topology:** topic → channels (fan-out); multiple consumers on one channel compete
- **Delivery:** at-least-once with explicit ack; handlers must be idempotent
- **MVP driver:** MySQL ≥ 8.0.1 (InnoDB, `SKIP LOCKED`)
- **Extensibility:** domain code depends on `store.Store` interfaces; Postgres/SQLite drivers can be added later under `driver/`

## Install

```bash
go get novaque@latest   # when published; for local submodule use a replace directive
```

## Quick start (MySQL)

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"novaque"
	mysqldriver "novaque/driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}
	store := mysqldriver.New(db)
	client, err := novaque.Open(store, novaque.Options{})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	if err := client.Start(ctx); err != nil { // reaper + TTL
		log.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	cons, err := client.Subscribe("events", "indexer", func(ctx context.Context, msg *novaque.Message) error {
		log.Printf("got %s attempt=%d", msg.Body, msg.Attempts)
		return nil // ack; return error to requeue
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = cons.Start(ctx) // launches Options.MaxInFlight concurrent workers
	defer cons.Shutdown(context.Background())

	if _, err := client.Publish(ctx, "events", []byte(`{"ok":true}`), novaque.PublishOpts{}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}
```

## Local load test

```bash
# starts MySQL 8 via Docker/testcontainers
go run ./cmd/loadtest

# or point at an existing instance
NOVAQUE_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/novaque?parseTime=true&loc=UTC' \
  go run ./cmd/loadtest -n 10000 -publishers 8 -max-inflight 32
```

Flags: `-n`, `-publishers`, `-max-inflight`, `-body`, `-pool`, `-dsn`.

## Guarantees (MVP)

| Behavior | Contract |
|----------|----------|
| Fan-out | Every **existing** channel gets a copy at publish time |
| Late subscribe | Channels created later do **not** receive historical messages |
| Delivery | At-least-once; lease expiry redelivers; ack requires matching lease token |
| Poison | After `max_attempts` claims, delivery is marked `dead` |
| Backend | Only MySQL driver ships; use `store.Store` for fakes/tests |

## Concurrency

- **In-process:** set `Options.MaxInFlight` (default 1). `Start` runs **one batch poller** (claims up to free slots) plus that many handler workers — Solid Queue–style, not N independent empty polls.
- **Multi-node:** more processes on the same channel compete via `SKIP LOCKED`.
- **Hot path:** `Subscribe` caches `channel_id`; claim scans only `novaque_deliveries` (`expires_at` denormalized); idle polls use jitter.
- Size `*sql.DB` pool ≥ `MaxInFlight` (plus publish/reaper headroom).
- **API:** `Subscribe` then `Start` when wiring many consumers; `SubscribeAndStart` for the simple path.
## Layout

```
novaque/
  store/           # Store interface (dialect-agnostic)
  driver/mysql/    # MySQL implementation
  client.go        # Client / Consumer / Publish
```

## Requirements

- Go 1.22+
- MySQL ≥ 8.0.1 for the MySQL driver
- Claim/ack/publish must hit a primary-writable connection (no read replicas)
