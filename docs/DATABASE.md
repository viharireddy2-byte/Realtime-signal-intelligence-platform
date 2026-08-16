# Database & Data Model

## Event schema (in-flight, Kafka)

Every event on `signal.events.v1` follows this envelope (see `ingestion/event-producer/event_producer.py`):

```json
{
  "event_id": "uuid",
  "schema_version": "2.0",
  "source": "web | mobile | api | iot-device | service-checkout | service-search",
  "timestamp": "ISO-8601 UTC",
  "attributes": {
    "user_id": "sha256-hashed, first 16 hex chars",
    "metric": 0.0,
    "status": "ok | warning | error",
    "session_id": "uuid",
    "funnel_step": "landing | product_view | add_to_cart | checkout | purchase",
    "region": "us-east | us-west | eu-west | eu-central | ap-south | ap-northeast",
    "version": "semver string",
    "...source-specific fields (browser, platform, device_type, downstream_latency_ms, ...)"
  }
}
```

`user_id` is hashed at the producer before it ever reaches Kafka — see [`SECURITY.md`](SECURITY.md#data-anonymization).

## Kafka topics

| Topic | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `signal.events.v1` | event-producer | aggregation-job, anomaly-detection-job, session-analytics-job | Raw event stream |
| `signal.aggregates.hot` | aggregation-job | (external consumer writes to Redis) | 1-minute KPI aggregates formatted for Redis |
| `signal.aggregates.cold` | aggregation-job | (external consumer writes to TimescaleDB) | Same aggregates formatted for `metrics_1min` |
| `signal.alerts.v1` | anomaly-detection-job | notifier-service | Confirmed anomalies for notification dispatch |
| `signal.anomalies.cold` | anomaly-detection-job | (external consumer writes to TimescaleDB) | Confirmed anomalies formatted for `anomalies` |

> The `aggregates.*`/`anomalies.cold` sink topics are intentionally decoupled from the actual database writers (see [`ARCHITECTURE.md`](ARCHITECTURE.md#why-kafka-topics-instead-of-direct-writes)). In the reference docker-compose stack, the notifier-service performs the `anomalies` write directly when it consumes from `signal.alerts.v1`; a lightweight sink consumer for `aggregates.hot`/`aggregates.cold` is the natural next microservice to add if you outgrow having query-api or a small dedicated worker do it inline.

## Redis key layout (hot path)

```text
sip:agg:{source}:{window}:{window-start-iso-timestamp}  ->  JSON aggregate, TTL 3600s
```

Example: `sip:agg:web:1m:2026-01-15T12:00:00Z` →

```json
{ "count": 342, "avg_metric": 51.2, "p95_metric": 98.7, "p99_metric": 142.3, "error_rate": 0.03, "sum_metric": 17510.4 }
```

The `sip:` prefix (configurable via `REDIS_KEY_PREFIX`) namespaces this platform's keys if you ever share a Redis instance with other applications.

## TimescaleDB schema (cold path)

Bootstrapped by `infra/docker-compose/init-scripts/01-init-timescaledb.sql` on first container start.

### `events_raw` (hypertable, partitioned on `ts`)

| Column | Type | Notes |
|---|---|---|
| `ts` | `TIMESTAMPTZ` | Partition key |
| `event_id` | `UUID` | |
| `source` | `TEXT` | Indexed |
| `metric` | `DOUBLE PRECISION` | |
| `status` | `TEXT` | Indexed |
| `user_id` | `TEXT` | Indexed (already hashed at ingest) |
| `attributes` | `JSONB` | GIN-indexed for ad-hoc queries |

Retention: 30 days (`add_retention_policy`). Compression after 7 days, segmented by `source`.

### `metrics_1min` (hypertable)

1-minute rollups written by the aggregation job's cold-path sink. Retention: 90 days.

### `anomalies`

Confirmed anomaly alerts written by the notifier-service. Includes `anomaly_type` (`z-score` | `mad` | `ewma`), `severity`, `value`, `threshold`, `z_score`, `resolved`.

### `sessions` (new)

Written by the Spark session-analytics job via JDBC upsert (`ON CONFLICT (session_id) DO UPDATE`), since a session can span multiple Spark micro-batches while it stays open.

| Column | Type | Notes |
|---|---|---|
| `session_id` | `TEXT` primary key | |
| `source` | `TEXT` | |
| `started_at` / `ended_at` | `TIMESTAMPTZ` | `ended_at` grows monotonically across upserts |
| `event_count` | `BIGINT` | Cumulative across micro-batches |
| `furthest_step` | `TEXT` | Highest funnel step reached |
| `converted` | `BOOLEAN` | `true` once any batch reaches `purchase`; sticky (never flips back) |

### Convenience views

`events_last_hour`, `metrics_last_24h`, `active_anomalies`, `converted_sessions_last_24h`.

## Grafana's Postgres datasource

`infra/docker-compose/grafana/provisioning/datasources/datasources.yml` provisions a TimescaleDB datasource alongside Prometheus, so dashboard panels can run raw SQL against `anomalies`/`sessions` directly (see the anomaly-detection dashboard's "Recent anomalies" table panel) without going through query-api.
