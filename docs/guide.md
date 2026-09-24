# novaque guide

Reference for topics covered briefly in the root [README](../README.md).

## SQLite (`driver/sqlite`)

Pure-Go **modernc.org/sqlite** — no cgo. Same `store.Store` surface as MySQL.

```go
db, _ := sql.Open("sqlite",
	"file:data/novaque.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
client, _ := novaque.Open(sqlite.New(db), novaque.Options{MaxInFlight: 4})
```

- **Pragmas:** `foreign_keys(1)`, `journal_mode(WAL)`, `busy_timeout(10000)` on file DSNs.
- **Floor:** SQLite ≥ 3.39.0 (`Migrate` checks `sqlite_version()`).
- **Claiming:** serializable transactions + status-guarded updates (no `SKIP LOCKED`); busy retries.
- **Fit:** single-node, tests, edge — not many concurrent writers (one writer at a time DB-wide).

## PostgreSQL (`driver/postgres`)

PostgreSQL **14+**; `FOR UPDATE SKIP LOCKED` like MySQL. Requires `github.com/jackc/pgx/v5` stdlib driver.

```go
db, _ := sql.Open("pgx", "postgres://user:pass@127.0.0.1:5432/app?sslmode=disable&TimeZone=UTC")
client, _ := novaque.Open(postgres.New(db), novaque.Options{MaxInFlight: 8})
```

- Use TLS (`sslmode=require` or stricter) off localhost.
- Topic/channel names are **case-sensitive** (unlike default MySQL utf8mb4_ci).
- Driver sets local `lock_timeout` / `statement_timeout` on transactional paths; tune pool DSN or role defaults for autocommit work.

## Options (full)

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
| `StatsRetentionDays` | 30 | UTC days before stats day-bucket prune |
| `StatsFlushInterval` | 2s | flush buffered counter deltas |
| `StatsPruneInterval` | 1h | stats prune tick |
| `Logger` | silent zap Nop | inject via `novaque.Zap(*zap.Logger)` |

**PublishOpts:** `TTL`, `Delay` (max 90d, must be less than effective TTL), `MaxAttempts`. Handler errors requeue immediately (delay is publish-time only).

Size `MaxOpenConns` ≥ `MaxInFlight` plus headroom; use a **primary-writable** DSN for claim/ack/publish.

### Logging

Lifecycle `Info` on Start/Shutdown; hot-path `Debug` for publish/claim; `Error` for swallowed maintenance/ack failures only. **Bodies are never logged.**

## Stats

- **Counters** — UTC day buckets (`publish`, `claim`, `ack`, `requeue`, `dead`, `purge`); async flush; survive ack/TTL deletes until prune.
- **Backlog** — live delivery row counts (`Pending`, `Ready`, `InFlight`, `Dead`).

| Method | Returns |
|--------|---------|
| `ChannelCounters` / `TopicCounters` | summed counters |
| `ChannelBacklog` | live backlog |
| `FlushStats` / `PruneStats` | explicit maintenance |

Stats reads **Ensure** names (typos create topics/channels). Stop consumers before client shutdown so final acks flush.

## Admin UI (`novaque/admin`)

Mountable stdlib `http.Handler` — dashboard, trends, create/delete, dead-letter browse. Embedded assets; optional basic auth (use HTTPS if enabled).

```go
h, _ := admin.New(client, admin.Options{Prefix: "/admin"})
// gin: r.Any("/admin/*any", gin.WrapH(h))
// chi: r.Mount("/admin", h)
```

POST-only mutations; stdlib CSRF protection on cross-origin posts. Topic delete cascades; channel delete drops that channel’s deliveries/stats only — remove consumers from config before delete or they may re-create the channel on subscribe.

Dead-letter requeue applies fresh `DefaultTTL` and resets attempts; TTL purge still applies.

## Upgrading to v0.0.7

- `store.Store`: `BacklogsForTopic`, `ListDead`; `DeadDelivery.BodyLen`.
- `RequeueDead` / `DeleteDead` require `channelID`.
- Out-of-tree `Store` implementors must add new methods. `Migrate` on deploy as usual.

## Repository layout

```
client.go, store/, driver/{mysql,postgres,sqlite}/, admin/, cmd/{example,loadtest}/
```

Domain code uses `store.Store` only; `Open` wraps `store.WithCache`. Claim path uses channel id + denormalized `expires_at` on deliveries.

## Testing & tools

```bash
go test ./...
go test -tags=integration ./...
go run ./cmd/example    # admin + demo HTTP (testcontainers MySQL)
go run ./cmd/loadtest   # or NOVAQUE_MYSQL_DSN=... go run ./cmd/loadtest -n 10000
```

Extended architecture (Mermaid): [novaque-workspace](https://github.com/usual2970/novaque-workspace/blob/main/docs/architecture/novaque-architecture-and-dataflow.md) when using the dual repo checkout.
