-- novaque MySQL schema (MySQL >= 8.0.1 / InnoDB)
CREATE TABLE IF NOT EXISTS novaque_topics (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_novaque_topics_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS novaque_channels (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  topic_id BIGINT NOT NULL,
  name VARCHAR(64) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_novaque_channels_topic_name (topic_id, name),
  CONSTRAINT fk_novaque_channels_topic FOREIGN KEY (topic_id) REFERENCES novaque_topics (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS novaque_messages (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  topic_id BIGINT NOT NULL,
  body LONGBLOB NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  CONSTRAINT fk_novaque_messages_topic FOREIGN KEY (topic_id) REFERENCES novaque_topics (id),
  KEY idx_novaque_messages_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS novaque_deliveries (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  message_id BIGINT NOT NULL,
  channel_id BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL,
  available_at DATETIME(3) NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL,
  lease_until DATETIME(3) NULL,
  lease_owner VARCHAR(128) NULL,
  lease_token VARCHAR(64) NULL,
  CONSTRAINT fk_novaque_deliveries_message FOREIGN KEY (message_id) REFERENCES novaque_messages (id) ON DELETE CASCADE,
  CONSTRAINT fk_novaque_deliveries_channel FOREIGN KEY (channel_id) REFERENCES novaque_channels (id),
  KEY idx_novaque_deliveries_claim (channel_id, status, available_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
