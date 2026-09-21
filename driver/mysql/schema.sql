-- novaque MySQL schema (MySQL >= 8.0.1 / InnoDB)
-- All clocks are Unix seconds (BIGINT), compared against DB time via UNIX_TIMESTAMP().
CREATE TABLE IF NOT EXISTS novaque_topics (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  UNIQUE KEY uk_novaque_topics_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS novaque_channels (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  topic_id BIGINT NOT NULL,
  name VARCHAR(64) NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  UNIQUE KEY uk_novaque_channels_topic_name (topic_id, name),
  CONSTRAINT fk_novaque_channels_topic FOREIGN KEY (topic_id) REFERENCES novaque_topics (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS novaque_messages (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  topic_id BIGINT NOT NULL,
  body LONGBLOB NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  expires_at BIGINT NOT NULL,
  CONSTRAINT fk_novaque_messages_topic FOREIGN KEY (topic_id) REFERENCES novaque_topics (id),
  KEY idx_novaque_messages_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Hot path: pending rows only; expires_at denormalized to avoid JOIN on claim.
CREATE TABLE IF NOT EXISTS novaque_deliveries (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  message_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL,
  available_at BIGINT NOT NULL,
  expires_at BIGINT NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL,
  lease_until BIGINT NULL,
  lease_owner VARCHAR(128) NULL,
  lease_token VARCHAR(64) NULL,
  CONSTRAINT fk_novaque_deliveries_message FOREIGN KEY (message_id) REFERENCES novaque_messages (id) ON DELETE CASCADE,
  CONSTRAINT fk_novaque_deliveries_channel FOREIGN KEY (channel_id) REFERENCES novaque_channels (id),
  KEY idx_novaque_deliveries_claim (channel_id, status, available_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Day-bucket event counters (UTC day; the one non-Unix-second clock on purpose).
-- channel_id = 0 is the sentinel for topic-only publish rows (zero-channel
-- publishes); no FK to channels so the sentinel and pruning stay cheap.
-- The purge counter is the `purged` column (PURGE is reserved in MySQL 8).
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
  PRIMARY KEY (day_utc, topic_id, channel_id),
  -- Counter reads filter by channel or topic alone; without these the SUM
  -- full-scans a table that grows to retention x topics x channels.
  KEY idx_novaque_stats_daily_channel (channel_id),
  KEY idx_novaque_stats_daily_topic (topic_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
