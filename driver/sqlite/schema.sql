-- novaque SQLite schema
-- All clocks are Unix seconds (INTEGER), compared against DB time via unixepoch().
CREATE TABLE IF NOT EXISTS novaque_topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS novaque_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic_id INTEGER NOT NULL REFERENCES novaque_topics(id),
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  UNIQUE (topic_id, name)
);

CREATE TABLE IF NOT EXISTS novaque_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic_id INTEGER NOT NULL REFERENCES novaque_topics(id),
  body BLOB NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_novaque_messages_expires
ON novaque_messages(expires_at);

-- Hot path: pending rows only; expires_at denormalized to avoid JOIN on claim.
CREATE TABLE IF NOT EXISTS novaque_deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES novaque_messages(id) ON DELETE CASCADE,
  channel_id INTEGER NOT NULL REFERENCES novaque_channels(id),
  status TEXT NOT NULL,
  available_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  lease_until INTEGER,
  lease_owner TEXT,
  lease_token TEXT
);

CREATE INDEX IF NOT EXISTS idx_novaque_deliveries_claim
ON novaque_deliveries(channel_id, status, available_at, id);

-- Maintenance ticks must not full-scan the hot table: the partial reap index
-- holds only leased in_flight rows (the only rows ReapExpiredLeases touches;
-- status transitions keep it small), and the expires index lets PurgeExpired
-- seek by expires_at.
CREATE INDEX IF NOT EXISTS idx_novaque_deliveries_reap
ON novaque_deliveries(lease_until) WHERE status = 'in_flight';

CREATE INDEX IF NOT EXISTS idx_novaque_deliveries_expires
ON novaque_deliveries(expires_at);

-- Day-bucket event counters (UTC day; the one non-Unix-second clock on purpose).
-- channel_id = 0 is the sentinel for topic-only publish rows (zero-channel
-- publishes); no FK to channels so the sentinel and pruning stay cheap.
CREATE TABLE IF NOT EXISTS novaque_stats_daily (
  day_utc TEXT NOT NULL,
  topic_id INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  publish INTEGER NOT NULL DEFAULT 0,
  claim INTEGER NOT NULL DEFAULT 0,
  ack INTEGER NOT NULL DEFAULT 0,
  requeue INTEGER NOT NULL DEFAULT 0,
  dead INTEGER NOT NULL DEFAULT 0,
  purged INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (day_utc, topic_id, channel_id)
);

-- Counter reads filter by channel or topic alone; without these the SUM
-- full-scans a table that grows to retention x topics x channels.
CREATE INDEX IF NOT EXISTS idx_novaque_stats_daily_channel
ON novaque_stats_daily(channel_id);

CREATE INDEX IF NOT EXISTS idx_novaque_stats_daily_topic
ON novaque_stats_daily(topic_id);
