-- novaque PostgreSQL schema (PostgreSQL >= 14)
-- All clocks are Unix seconds (BIGINT), compared against DB time via
-- FLOOR(EXTRACT(EPOCH FROM clock_timestamp())).
CREATE TABLE IF NOT EXISTS novaque_topics (
  id BIGSERIAL PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))),
  CONSTRAINT uk_novaque_topics_name UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS novaque_channels (
  id BIGSERIAL PRIMARY KEY,
  topic_id BIGINT NOT NULL,
  name VARCHAR(64) NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))),
  CONSTRAINT uk_novaque_channels_topic_name UNIQUE (topic_id, name),
  CONSTRAINT fk_novaque_channels_topic FOREIGN KEY (topic_id) REFERENCES novaque_topics (id)
);

CREATE TABLE IF NOT EXISTS novaque_messages (
  id BIGSERIAL PRIMARY KEY,
  topic_id BIGINT NOT NULL,
  body BYTEA NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))),
  expires_at BIGINT NOT NULL,
  CONSTRAINT fk_novaque_messages_topic FOREIGN KEY (topic_id) REFERENCES novaque_topics (id)
);
CREATE INDEX IF NOT EXISTS idx_novaque_messages_expires ON novaque_messages (expires_at);

-- Hot path: pending rows only; expires_at denormalized to avoid JOIN on claim.
CREATE TABLE IF NOT EXISTS novaque_deliveries (
  id BIGSERIAL PRIMARY KEY,
  message_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL,
  available_at BIGINT NOT NULL,
  expires_at BIGINT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  lease_until BIGINT NULL,
  lease_owner VARCHAR(128) NULL,
  lease_token VARCHAR(64) NULL,
  CONSTRAINT fk_novaque_deliveries_message FOREIGN KEY (message_id) REFERENCES novaque_messages (id) ON DELETE CASCADE,
  CONSTRAINT fk_novaque_deliveries_channel FOREIGN KEY (channel_id) REFERENCES novaque_channels (id)
);
CREATE INDEX IF NOT EXISTS idx_novaque_deliveries_claim ON novaque_deliveries (channel_id, status, available_at, id);

-- Day-bucket event counters (UTC day; the one non-Unix-second clock on purpose).
-- channel_id = 0 is the sentinel for topic-only publish rows (zero-channel
-- publishes); no FK to channels so the sentinel and pruning stay cheap.
-- The purge counter is the `purged` column.
CREATE TABLE IF NOT EXISTS novaque_stats_daily (
  day_utc DATE NOT NULL,
  topic_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  publish BIGINT NOT NULL DEFAULT 0,
  claim BIGINT NOT NULL DEFAULT 0,
  ack BIGINT NOT NULL DEFAULT 0,
  requeue BIGINT NOT NULL DEFAULT 0,
  dead BIGINT NOT NULL DEFAULT 0,
  purged BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT pk_novaque_stats_daily PRIMARY KEY (day_utc, topic_id, channel_id)
);
-- Counter reads filter by channel or topic alone; without these the SUM
-- full-scans a table that grows to retention x topics x channels.
CREATE INDEX IF NOT EXISTS idx_novaque_stats_daily_channel ON novaque_stats_daily (channel_id);
CREATE INDEX IF NOT EXISTS idx_novaque_stats_daily_topic ON novaque_stats_daily (topic_id);
