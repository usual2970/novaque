# novaque

Embeddable Go library for **NSQ-style pub/sub** on a relational database.

You bring a `*sql.DB`; novaque runs inside your process — no broker daemon. Topics fan out to channels; consumers on the same channel compete. Delivery is **at-least-once** with lease + ack.

| | |
|---|---|
| Topology | topic → channels (multicast); compete within a channel |
| Durability | rows in MySQL (MVP); claim with `SKIP LOCKED` |
| Extensibility | `store.Store` seam — Postgres/SQLite drivers can plug in later |
| Form | library module, not a long-running service |

## Install

Module path is currently `novaque` (GitHub: [usual2970/novaque](https://github.com/usual2970/novaque)).

```bash
# clone / submodule, then in your app:
go get novaque@v0.0.1

# or local replace
# replace novaque => ../novaque
```

Requires **Go 1.22+** and, for the shipped driver, **MySQL ≥ 8.0.1** (InnoDB).

## Quick start

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"novaque"
	mysqldriver "github.com/usual2970/novaque/driver/mysql"
)

func main() {
	db, err := sql.Open("mysql",
		"user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(32)

	client, err := novaque.Open(mysqldriver.New(db), novaque.Options{
		MaxInFlight: 8, // concurrent handlers per consumer
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	if err := client.Start(ctx); err != nil { // lease reaper + TTL purge + stats flush/prune
		log.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	cons, err := client.SubscribeAndStart(ctx, "events", "indexer",
		func(ctx context.Context, msg *novaque.Message) error {
			log.Printf("got %s attempt=%d", msg.Body, msg.Attempts)
			return nil // nil → ack; error → requeue
		})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Shutdown(context.Background())

	if _, err := client.Publish(ctx, "events", []byte(`{"ok":true}`),
		novaque.PublishOpts{}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}
```

`Subscribe` + `Start` is available when you need to wire several consumers before polling.

## Concepts

```
Publisher ──Publish──▶ topic ──fan-out──▶ channel A ──compete──▶ consumers
                                   └──▶ channel B ──compete──▶ consumers
```

- **Topic** — named stream; created lazily on first publish/subscribe.
- **Channel** — named subscription on a topic; each existing channel gets its own delivery row at publish time.
- **Late subscribe** — a channel created after messages were published does **not** receive history.
- **Handler** — must be **idempotent** (at-least-once; crash before ack → redelivery after lease expiry).

## Options

| Field | Default | Role |
|-------|---------|------|
| `DefaultTTL` | 7d | message retention when publish omits TTL |
| `DefaultLease` | 30s | claim lease duration |
| `DefaultMaxAttempts` | 5 | poison threshold (then `dead`) |
| `PollInterval` | 200ms | consumer idle poll base (+ jitter) |
| `MaxInFlight` | 1 | handler workers + batch claim size per consumer |
| `ReapInterval` | 1s | expired-lease reaper tick |
| `PurgeInterval` | 5s | TTL cleanup tick |
| `MaintenanceBatch` | 100 | rows per reaper/purge pass |
| `StatsRetentionDays` | 30 | UTC days a counter day bucket is kept before prune |
| `StatsFlushInterval` | 2s | stats counter flush tick (buffered deltas → DB) |
| `StatsPruneInterval` | 1h | stats retention prune tick |
| `Logger` | zap Nop (silent) | structured operational logs; inject e.g. `Zap(yourZapLogger)` |

### PublishOpts

| Field | Role |
|-------|------|
| `TTL` | retention from publish time (overrides `DefaultTTL` when set) |
| `Delay` | relative defer until first claim (NSQ `DPUB`-style); max **90 days** (`MaxDelay`) |
| `MaxAttempts` | poison threshold for this message |

`Delay` must be **strictly less than** effective TTL (after `DefaultTTL` fill), measured in whole Unix seconds — otherwise Publish returns `ErrDelayExceedsTTL`. Over-max returns `ErrDelayTooLong`; negative returns `ErrDelayNegative`. Handler failure still requeues **immediately** (publish delay only).

Example: a 2-day delay needs an explicit TTL longer than 2 days (default TTL is 7d, so omit is fine; an 8-day delay needs `TTL` > 8d).

Size `*sql.DB` `MaxOpenConns` ≥ `MaxInFlight` plus publish/maintenance headroom. Use a **primary-writable** DSN (no read replicas) for claim/ack/publish.

### Logging

`Options.Logger` takes the public `Logger` interface — `Debug`/`Info`/`Warn`/`Error(msg, ...zap.Field)` plus `With` for child loggers. When unset, novaque binds a **silent `zap.NewNop()` default**: the library produces no console noise until you inject one.

```go
client, err := novaque.Open(mysqldriver.New(db), novaque.Options{
	Logger: novaque.Zap(zapLogger), // adapt your configured *zap.Logger
})
```

- **Lifecycle `Info`** — Client and Consumer Start/Shutdown, once per actual transition (duplicate Start / Shutdown-when-not-started stay silent).
- **Hot path `Debug`** — successful publish (`topic`, `message_id`) and non-empty claim (`topic`, `channel`, `count`). Expect high volume if you enable Debug in production.
- **`Error` only for swallowed failures** — claim backoff, reap, purge, stats flush/prune, and final ack/requeue after retries. Errors returned to your caller (e.g. Publish) are not duplicate-logged; shutdown cancels are silent.
- **Payload privacy** — message bodies are **never logged**; only ids, topic, channel, counts, and errors.

The quick start's stdlib `log.Printf` is caller-side printing, separate from this library logging.

## Stats

novaque keeps two kinds of numbers, and the split matters:

- **Counters** — `publish` / `claim` / `ack` / `requeue` / `dead` / `purge` event totals stored as **UTC day buckets** keyed by topic/channel. They survive ack deletes and message TTL purges; they only fall when the retention prune removes old day buckets.
- **Backlog** — a **live count** of `novaque_deliveries` rows right now. It drops as work is acked, purged, or dead-lettered, and is never day-bucketed.

| Method | Returns |
|--------|---------|
| `ChannelCounters(ctx, topic, channel)` | summed counters over retained days for one channel |
| `TopicCounters(ctx, topic)` | counters rolled up over the topic (per-channel rows plus the topic-level row) |
| `ChannelBacklog(ctx, topic, channel)` | live `Pending` / `Ready` / `InFlight` / `Dead` counts |
| `FlushStats(ctx)` | drains buffered counter deltas into the DB now |
| `PruneStats(ctx)` | deletes day buckets older than `StatsRetentionDays`; returns rows deleted |

```go
cc, err := client.ChannelCounters(ctx, "events", "indexer")
bl, err := client.ChannelBacklog(ctx, "events", "indexer")
log.Printf("publish=%d ack=%d pending=%d ready=%d",
    cc.Publish, cc.Ack, bl.Pending, bl.Ready)
```

Semantics worth knowing:

- **Stats reads create on read.** The stats read APIs resolve names the same way `Publish`/`Subscribe` do, so reading a topic or channel that does not exist yet creates it — a typo'd name shows zeros and permanently joins future publish fan-out. Double-check names in monitoring code.
- **Async counters.** Mutations buffer counter deltas in-process; the maintenance loop flushes them every `StatsFlushInterval` (default 2s). Reads are eventually consistent within that window; a hard crash loses at most the unflushed window. A flush that landed server-side but *looked* failed is retried and can double-count — at-least-once, never loses counts. Graceful `Shutdown` performs one final flush.
- **Reap is not requeue.** Only a handler-driven `Requeue` counts. A lease that expires and is re-claimed counts `claim` again — the same at-least-once rule as delivery.
- **Ready vs Pending.** Delayed publishes (`PublishOpts.Delay`) count as `Pending` but not `Ready` until `available_at` passes; claim only takes `Ready`. `Ready` mirrors claim eligibility exactly, so pending rows whose TTL has expired (not yet purged) stay in `Pending` but drop out of `Ready`.
- **Zero-channel publishes** count on a topic-level row (visible in `TopicCounters`; no channel backlog changes).
- **Retention.** Day buckets older than `StatsRetentionDays` (default 30) are pruned every `StatsPruneInterval` (default 1h). Prune needs `Start` — or call `PruneStats` / `FlushStats` explicitly when you host novaque without maintenance loops.
- **Privacy.** Stats store ids and counts only — never message payloads.
- **Shutdown order.** Stop Consumers before the Client so their final acks land before the Client's last counter flush.

## Guarantees

| Behavior | Contract |
|----------|----------|
| Fan-out | One pending delivery per **existing** channel, same transaction as the message |
| Late channel | No retroactive history |
| Delivery | At-least-once; ack requires matching `lease_token` |
| Compete | Multi-process safe via `FOR UPDATE SKIP LOCKED` |
| Poison | After `max_attempts` claims → `dead`, not returned |
| TTL | `Client.Start` purges expired messages/deliveries |
| Delay | Relative publish defer via `available_at`; max 90d; requires TTL > Delay |
| Stats | counters in UTC day buckets (flushed async, pruned after `StatsRetentionDays`); backlog is a live COUNT — see [Stats](#stats) |

## Architecture

```
novaque/
  client.go           # Client, Consumer, Publish / Subscribe
  logger.go           # Logger interface, zap adapter, silent Nop default
  store/
    store.go          # Store interface (dialect-agnostic)
    cached.go         # WithCache — memoize EnsureTopic / EnsureChannel
  driver/mysql/       # MySQL Store + schema.sql
  cmd/loadtest/       # local publish/consume stress tool
  internal/testmysql/ # testcontainers helper (integration tests)
```

- Domain code talks only to `store.Store`; MySQL SQL/locking stays in `driver/mysql`.
- `Open` wraps the driver with `store.WithCache` so steady-state publish/subscribe skips name→id round-trips.
- Claim path uses **channel id** and denormalized `expires_at` on `novaque_deliveries` (no hot-path JOIN).
- Each consumer runs **one batch poller** + `MaxInFlight` workers (Solid Queue–style), not N independent empty polls.

## Testing

```bash
go test ./...

# needs Docker
go test -tags=integration ./...
```

### Load test

```bash
# boots MySQL 8 via testcontainers
go run ./cmd/loadtest

NOVAQUE_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/novaque?parseTime=true&loc=UTC' \
  go run ./cmd/loadtest -n 10000 -publishers 8 -max-inflight 32
```

Flags: `-n`, `-publishers`, `-max-inflight`, `-body`, `-pool`, `-dsn`.

## Status / non-goals

Shipped: MySQL driver, publish fan-out, subscribe/claim/ack/requeue, publish-time Delay (max 90d), reaper, TTL, in-process name cache, injectable logging (zap Nop default), DB-backed queue stats (day-bucket counters + live backlog), loadtest.

Not in MVP: Postgres/SQLite drivers, NSQ wire protocol, standalone broker, admin UI, deferred requeue/backoff.
