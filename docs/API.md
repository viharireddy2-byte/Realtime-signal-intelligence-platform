# API Reference

Interactive OpenAPI docs are always available at `/docs` on a running query-api instance (`http://localhost:8000/docs` locally). This document is a stable reference for both HTTP services.

## Authentication

Disabled by default for local development. Set `API_KEY_REQUIRED=true` and `API_KEY=<secret>` on query-api to require an `X-API-Key` header on every data endpoint (health/readiness stay open for load balancer/Kubernetes probes). See [`SECURITY.md`](SECURITY.md) for how to layer real identity-aware auth in front of this for production.

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8000/kpi
```

---

## Query API (`services/query-api`, default port `8000`)

### `GET /health`

Liveness probe. Reports the status of Redis and TimescaleDB connectivity. No API key required.

```json
{
  "status": "healthy",
  "timestamp": "2026-01-15T12:00:00Z",
  "services": { "redis": "healthy", "timescaledb": "healthy" }
}
```

### `GET /ready`

Kubernetes readiness probe. Returns `200 {"status": "ready"}` or `503` if either store is unreachable.

### `GET /kpi`

Hot-path KPI reads from Redis, populated by the aggregation streaming job.

| Param | Type | Default | Description |
|---|---|---|---|
| `source` | string | none (all sources) | Filter by event source |
| `window` | string | `1m` | One of `1m`, `5m`, `15m`, `1h`, `1d` |

Returns up to 100 results, most recent first:

```json
[
  {
    "source": "web",
    "window": "1m",
    "timestamp": "2026-01-15T12:00:00Z",
    "count": 342,
    "avg_metric": 51.2,
    "p95_metric": 98.7,
    "p99_metric": 142.3,
    "error_rate": 0.03
  }
]
```

### `GET /series`

Historical time-series reads from TimescaleDB.

| Param | Type | Required | Description |
|---|---|---|---|
| `source` | string | no | Filter by event source |
| `from` | ISO datetime | yes | Range start |
| `to` | ISO datetime | yes | Range end |
| `aggregation` | string | no (`avg`) | One of `avg`, `sum`, `count`, `p95` |

```json
[
  {
    "source": "web",
    "metric": "avg",
    "data": [{ "timestamp": "2026-01-15T11:59:00Z", "value": 49.8 }]
  }
]
```

### `GET /alerts`

Anomaly alert history, written by the notifier-service on confirmed anomalies.

| Param | Type | Description |
|---|---|---|
| `since` | ISO datetime | Only alerts at/after this time |
| `resolved` | bool | Filter by resolved status |
| `severity` | string | `info` \| `warning` \| `critical` |

Returns up to 1000 results, most recent first. Each alert includes `anomaly_type` (`z-score`, `mad`, or `ewma` — whichever detector fired), `value`, `threshold`, and `z_score`.

### `GET /sessions`

Reconstructed user sessions, written by the Spark session-analytics job.

| Param | Type | Default | Description |
|---|---|---|---|
| `source` | string | none | Filter by event source |
| `converted_only` | bool | `false` | Only return sessions that reached the `purchase` funnel step |
| `limit` | int | `200` (max `1000`) | Max rows |

```json
[
  {
    "session_id": "6d1c...",
    "source": "web",
    "started_at": "2026-01-15T11:40:00Z",
    "ended_at": "2026-01-15T11:52:00Z",
    "event_count": 14,
    "furthest_step": "purchase",
    "converted": true
  }
]
```

### `WS /ws/live`

Pushes a JSON snapshot every `LIVE_FEED_INTERVAL_SECONDS` (default 2s) — the top ~50 most recent 1-minute KPI aggregates and the 10 most recent anomaly alerts. Powers `services/live-dashboard`; connect directly for any custom client.

```json
{
  "type": "snapshot",
  "generated_at": "2026-01-15T12:00:00Z",
  "kpis": [{ "source": "web", "window": "1m", "timestamp": "...", "count": 340, "avg_metric": 51.0 }],
  "recent_alerts": [{ "ts": "...", "source": "web", "severity": "warning", "description": "..." }]
}
```

### `GET /metrics`

Prometheus scrape endpoint (text exposition format).

---

## Notifier Service (`services/notifier-service`, default port `8001`)

### `GET /health`

Liveness probe.

### `POST /webhook/alerts`

Receives a standard Prometheus Alertmanager webhook payload and routes each alert through the same rule/cooldown/notification pipeline as anomaly alerts from Kafka.

### `POST /webhook/critical` / `POST /webhook/warning`

Convenience endpoints Alertmanager's routing tree targets directly by severity (see `infra/docker-compose/alertmanager.yml`).

### `GET /alerts/stats`

Rolling 24-hour alert counts by severity (`total`, `active`, `last_hour`).

### `GET /metrics`

Prometheus scrape endpoint.

---

## Live Dashboard (`services/live-dashboard`, default port `8003`)

### `GET /`

Serves the dashboard HTML page.

### `GET /config.js`

Runtime configuration (query-api WebSocket/HTTP URLs) injected as a small JS snippet, so the same static assets work across environments.

### `GET /health`

Liveness probe.
