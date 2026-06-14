CREATE TABLE IF NOT EXISTS transactions (
  id           BIGSERIAL PRIMARY KEY,
  tx_id        TEXT UNIQUE NOT NULL,
  user_id      TEXT NOT NULL,
  amount       NUMERIC(12,2) NOT NULL,
  merchant     TEXT,
  country      TEXT,
  device       TEXT,
  ts           TIMESTAMPTZ NOT NULL,
  risk_score   REAL,
  is_fraud     BOOLEAN,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_tx_ts ON transactions(ts DESC);
CREATE INDEX IF NOT EXISTS ix_tx_user ON transactions(user_id);

CREATE TABLE IF NOT EXISTS alerts (
  id         BIGSERIAL PRIMARY KEY,
  tx_id      TEXT REFERENCES transactions(tx_id),
  reason     TEXT,
  risk_score REAL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_features (
  user_id          TEXT PRIMARY KEY,
  avg_amount       NUMERIC(12,2),
  tx_count_24h     INT,
  distinct_country INT,
  last_seen        TIMESTAMPTZ
);
