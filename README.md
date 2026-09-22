# novaque

Embeddable Go library for **NSQ-style pub/sub** on a relational database.

You bring a `*sql.DB`; novaque runs inside your process — no broker daemon. Topics fan out to channels; consumers on the same channel compete. Delivery is **at-least-once** with lease + ack.

| | |
|---|---|
| Topology | topic → channels (multicast); compete within a channel |
| Durability | rows in MySQL or PostgreSQL; claim with `SKIP LOCKED` |
| Extensibility | `store.Store` seam — other drivers (e.g. SQLite) can plug in |
| Form | library module, not a long-running service |

## Install

```bash
go get github.com/usual2970/novaque
```

Requires **Go 1.26.5+** and one of the shipped drivers: **MySQL ≥ 8.0.1** (InnoDB) or **PostgreSQL ≥ 14**.

## Upgrading to v0.0.7

v0.0.7 ships the mountable admin UI and grows the exported surface. Existing `*Client` callers keep compiling; out-of-tree implementors of the `store.Store` seam must add the new methods.

- **`store.Store`**: `BacklogsForTopic(ctx, topicID int64) ([]BacklogRow, error)` — the batched pending/ready/in-flight/dead read per channel, replacing N separate `ChannelBacklog` calls — and `ListDead(ctx, channelID, before int64, limit, bodyPrefix int) ([]DeadDelivery, error)`.
- **`store.DeadDelivery`**: new `BodyLen int64` is the `OCTET_LENGTH` of the full stored body. `Body` is the complete body when `bodyPrefix == 0` and at most `bodyPrefix` bytes otherwise, so a prefixed row can still report its true size.
- **Dead-letter writes are channel-scoped**: `Client.RequeueDead(ctx, deliveryID, channelID)` and `Client.DeleteDead(ctx, deliveryID, channelID)` — pass the owning channel id alongside the delivery id; an id under a different channel is a no-op.

Run `Client.Migrate` on deploy as usual; the admin UI adds no separate migration.

## Quick start

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/usual2970/novaque"
	"github.com/usual2970/novaque/driver/mysql"
)

func main() {
	db, err := sql.Open("mysql",
		"user:pass@tcp(127.0.0.1:3306)/app?parseTime=true&loc=UTC")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(32)

	client, err := novaque.Open(mysql.New(db), novaque.Options{
		MaxInFlight: 8, // concurrent handlers per consumer
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Migrate(ctx); err != nil {
		log.Fatal(err) // schema setup; idempotent, safe every startup
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
	defer cons.Shutdown(context.Background()) // stop the consumer before the client

	if _, err := client.Publish(ctx, "events", []byte(`{"ok":true}`),
		novaque.PublishOpts{}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}
```

`Subscribe` + `Start` is available when you need to wire several consumers before polling.

## PostgreSQL driver

`driver/postgres` implements the same `store.Store` semantics on **PostgreSQL 14+**: topic→channel fan-out, `FOR UPDATE SKIP LOCKED` claiming, lease/ack, TTL purge, day-bucket stats, and the admin/dead-letter surface behave identically to the MySQL driver. `Migrate` checks `server_version_num` and refuses any server below 14, then applies the embedded schema.

```bash
go get github.com/jackc/pgx/v5
```

```go
import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/usual2970/novaque"
	"github.com/usual2970/novaque/driver/postgres"
)

db, err := sql.Open("pgx",
	"postgres://user:pass@127.0.0.1:5432/app?sslmode=disable&TimeZone=UTC")
if err != nil {
	log.Fatal(err)
}
db.SetMaxOpenConns(32)

client, err := novaque.Open(postgres.New(db), novaque.Options{
	MaxInFlight: 8, // concurrent handlers per consumer
})
```

- **DSN.** pgx v5 connection string over `database/sql`. `sslmode=disable` is for local dev only — require TLS (`sslmode=require` or stricter) for any off-host database.
- **UTC.** Every clock on the hot path is derived server-side (`clock_timestamp()` and `NOW() AT TIME ZONE 'UTC'`), so correctness does not depend on the session time zone; setting `TimeZone=UTC` in the DSN keeps manual inspection and logs aligned.
- **Case sensitivity.** Unlike MySQL's default utf8mb4 collation, PostgreSQL names are case-sensitive: `Orders` and `orders` are distinct topics.
- **Connection pool.** Same sizing rule as MySQL: `MaxOpenConns` ≥ `MaxInFlight` plus publish/maintenance headroom, and a primary-writable connection (no read replicas) for claim/ack/publish.

## Documentation

Every exported symbol in `novaque`, `novaque/store`, `novaque/admin`, `novaque/driver/mysql`, and `novaque/driver/postgres` carries identifier-first godoc. The root package ships compile-verified `Example` functions (`example_test.go`) covering the open → migrate → start lifecycle, publish options, the consume loop, and backlog reads — they need a live MySQL, so they run as ordinary programs rather than under `go test` output comparison.

```bash
go doc github.com/usual2970/novaque.Client
go doc github.com/usual2970/novaque.PublishOpts
```

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
| `PollInterval` | 200ms | consumer idle poll base (±50% jitter) |
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
client, err := novaque.Open(mysql.New(db), novaque.Options{
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

## Admin UI

`novaque/admin` ships a zero-build admin UI and JSON API as one stdlib `http.Handler`: a dashboard with live backlog, topic/channel detail pages with day-bucket trends, create and cascade-delete flows, and dead-letter browse (truncated list, full body on demand) with requeue/delete. Templates, CSS, and vanilla JS are embedded — no Node toolchain, no new runtime dependencies.

```go
h, err := admin.New(client, admin.Options{
	Prefix:        "/admin", // default; "/" mounts at the root
	BasicAuthUser: "ops",    // optional single pair — both or neither
	BasicAuthPass: "hunter2",
})
```

The handler strips its own prefix and re-applies it to every generated URL and redirect, so the host never wraps `http.StripPrefix`. Mount recipes:

```go
r.Any("/admin/*any", gin.WrapH(h))       // gin
e.Any("/admin/*", echo.WrapHandler(h))   // echo — needs the bare route too:
e.Any("/admin", echo.WrapHandler(h))
r.Mount("/admin", h)                     // chi
mux.Handle("/admin/", h)                 // net/http
```

### Auth and CSRF

The only auth the package provides is the optional single basic-auth pair — no sessions, no RBAC. Real authentication belongs in host middleware mounted before the admin handler. **Basic auth without TLS on the host sends decodable credentials on every request**: serve the admin path over HTTPS whenever the pair is set.

Every mutation is a POST (no mutating GET). Cross-origin browser mutations are rejected by the stdlib `http.CrossOriginProtection`: a POST with a mismatched `Origin` header answers 403, while same-origin forms and curl (no `Origin`) pass.

### Delete semantics

- **Topic delete cascades** in one transaction: the topic's channels, messages, deliveries, and retained day-bucket stats rows (including the zero-channel sentinel rows) all go. It is idempotent on an already-deleted id, and publishers self-heal — the next publish to the topic name re-creates it.
- **Channel delete removes only that channel's deliveries and stats rows.** Shared message rows survive, so sibling channels keep their deliveries.
- **Remove the channel from your consumer configuration before restarting consumers.** A consumer restarted while still subscribed re-creates the channel (create-on-subscribe), and a running consumer left subscribed idles forever until restarted. The delete confirmation page warns about this.

### Dead letters

Dead deliveries are browsable per channel — body preview truncated in the list, full body on demand — with attempts, remaining TTL, and per-delivery requeue/delete. Two semantics worth knowing:

- **Requeue restarts the clock.** The original per-publish TTL is not stored: a requeued dead delivery gets a fresh TTL from the Client's `DefaultTTL`, with attempts reset and available now.
- **Requeue is a retry, not immortality.** The fresh TTL is bounded by the message's remaining life / `DefaultTTL` — the TTL purge still reclaims any row whose `expires_at` passes. Dead-letter requeue buys another processing window; it does not exempt the message from expiry.

### Caveats

- **Collation.** MySQL's default utf8mb4 collation is case-insensitive: `Orders` and `orders` are the same topic. Duplicate creates resolve idempotently to the existing entity either way.
- **Counter lag.** Counters buffer in-process and flush every `StatsFlushInterval` (default 2s); each process flushes only its own deltas, so with multiple publisher processes the UI's numbers may lag by a few seconds (the page footer notes this). Backlog is a live COUNT and does not lag.
- **`purge` counter.** The `purge` day-bucket counter totals TTL purges **plus manual dead-letter deletions** from the admin UI — deleting a dead delivery bumps `purge`.

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
  example_test.go     # godoc Example functions (compile-verified)
  logger.go           # Logger interface, zap adapter, silent Nop default
  store/
    store.go          # Store interface (dialect-agnostic)
    cached.go         # WithCache — memoize EnsureTopic / EnsureChannel
  driver/mysql/       # MySQL Store + schema.sql
  driver/postgres/    # PostgreSQL 14+ Store + schema.sql
  admin/              # mountable admin UI + JSON API (stdlib http.Handler)
  cmd/example/        # local HTTP demo: admin UI + publish/subscribe hooks
  cmd/loadtest/       # local publish/consume stress tool
  internal/testmysql/    # testcontainers helper (MySQL integration tests)
  internal/testpostgres/ # testcontainers helper (PostgreSQL integration tests)
```

- Domain code talks only to `store.Store`; dialect SQL/locking stays inside each `driver/` package.
- `Open` wraps the driver with `store.WithCache` so steady-state publish/subscribe skips name→id round-trips.
- Claim path uses **channel id** and denormalized `expires_at` on `novaque_deliveries` (no hot-path JOIN).
- Each consumer runs **one batch poller** + `MaxInFlight` workers (Solid Queue–style), not N independent empty polls.

## Testing

```bash
go test ./...

# needs Docker
go test -tags=integration ./...

# one dialect at a time
go test -tags=integration ./driver/postgres/...
go test -tags=integration ./driver/mysql/...
```

### Example server (admin + publish/subscribe)

```bash
# boots MySQL 8 via testcontainers, serves admin at /admin/
go run ./cmd/example

# publish and inspect the demo consumer
curl -sS -X POST http://127.0.0.1:8080/demo/publish -d 'hello'
curl -sS http://127.0.0.1:8080/demo/stats
```

Flags: `-addr`, `-dsn`, `-topic`, `-channel`, `-admin-user`, `-admin-pass`, `-pool`. Open `http://127.0.0.1:8080/admin/` for the dashboard.

### Load test

```bash
# boots MySQL 8 via testcontainers
go run ./cmd/loadtest

NOVAQUE_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/novaque?parseTime=true&loc=UTC' \
  go run ./cmd/loadtest -n 10000 -publishers 8 -max-inflight 32
```

Flags: `-n`, `-publishers`, `-max-inflight`, `-body`, `-pool`, `-dsn`.

## Status / non-goals

Shipped: MySQL and PostgreSQL 14+ drivers, publish fan-out, subscribe/claim/ack/requeue, publish-time Delay (max 90d), reaper, TTL, in-process name cache, injectable logging (zap Nop default), DB-backed queue stats (day-bucket counters + live backlog), mountable admin UI (`novaque/admin`), loadtest, complete identifier-first godoc with compile-verified examples.

Not in MVP: SQLite driver, NSQ wire protocol, standalone broker, deferred requeue/backoff.
