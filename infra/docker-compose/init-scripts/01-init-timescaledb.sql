-- Real-Time Signal Intelligence Platform — TimescaleDB bootstrap
--
-- Creates the cold-storage schema: raw events, 1-minute rollups, anomaly
-- alerts, and reconstructed sessions (written by the Spark
-- session-analytics-job). Runs automatically on first container start via
-- the docker-entrypoint-initdb.d mechanism.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- Raw events (written by the aggregation job's cold-path sink consumer)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events_raw (
    ts TIMESTAMPTZ NOT NULL,
    event_id UUID NOT NULL,
    source TEXT NOT NULL,
    metric DOUBLE PRECISION,
    status TEXT,
    user_id TEXT,
    attributes JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('events_raw', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_events_source_ts ON events_raw (source, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events_raw (user_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON events_raw (status);
CREATE INDEX IF NOT EXISTS idx_events_attributes ON events_raw USING GIN (attributes);

-- ---------------------------------------------------------------------
-- 1-minute rollups (written by the Flink aggregation job's cold sink)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics_1min (
    ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    count_events BIGINT,
    avg_metric DOUBLE PRECISION,
    p95_metric DOUBLE PRECISION,
    p99_metric DOUBLE PRECISION,
    error_rate DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('metrics_1min', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_metrics_source_ts ON metrics_1min (source, ts DESC);

-- ---------------------------------------------------------------------
-- Anomalies (written by the notifier-service on confirmed alerts)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    z_score DOUBLE PRECISION,
    description TEXT,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_ts ON anomalies (ts DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_source ON anomalies (source);
CREATE INDEX IF NOT EXISTS idx_anomalies_resolved ON anomalies (resolved);

-- ---------------------------------------------------------------------
-- Sessions (new — written by the Spark session-analytics-job)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    furthest_step TEXT NOT NULL DEFAULT 'landing',
    converted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_ended_at ON sessions (ended_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions (source);
CREATE INDEX IF NOT EXISTS idx_sessions_converted ON sessions (converted);

-- ---------------------------------------------------------------------
-- Retention & compression (see docs/COST.md for the sizing rationale)
-- ---------------------------------------------------------------------
SELECT add_retention_policy('events_raw', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('metrics_1min', INTERVAL '90 days', if_not_exists => TRUE);

ALTER TABLE events_raw SET (timescaledb.compress, timescaledb.compress_segmentby = 'source');
SELECT add_compression_policy('events_raw', INTERVAL '7 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- Convenience views
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW events_last_hour AS
SELECT * FROM events_raw WHERE ts >= NOW() - INTERVAL '1 hour' ORDER BY ts DESC;

CREATE OR REPLACE VIEW metrics_last_24h AS
SELECT * FROM metrics_1min WHERE ts >= NOW() - INTERVAL '24 hours' ORDER BY ts DESC;

CREATE OR REPLACE VIEW active_anomalies AS
SELECT * FROM anomalies WHERE resolved = FALSE ORDER BY ts DESC;

CREATE OR REPLACE VIEW converted_sessions_last_24h AS
SELECT * FROM sessions WHERE converted = TRUE AND ended_at >= NOW() - INTERVAL '24 hours' ORDER BY ended_at DESC;

-- ---------------------------------------------------------------------
-- Permissions (adjust for a real production role/least-privilege setup —
-- see docs/SECURITY.md)
-- ---------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO signalintel_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO signalintel_admin;
