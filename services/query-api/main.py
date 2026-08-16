"""
Query API
=========

Public read surface of the Real-Time Signal Intelligence Platform. Serves:

- hot, sub-second KPI reads from Redis (populated by the aggregation job)
- cold, historical time-series reads from TimescaleDB
- anomaly alert history
- reconstructed user sessions (populated by the session-analytics job)
- a WebSocket live feed used by the live-dashboard service

Follows the same single-file FastAPI service pattern used across this
platform (see ../notifier-service/main.py), split into a few small
supporting modules (config.py, security.py) for clarity.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import redis
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from config import settings
from security import require_api_key

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [query-api] %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Prometheus metrics
# --------------------------------------------------------------------------
REQUEST_COUNT = Counter("query_api_requests_total", "Total API requests", ["method", "endpoint", "status"])
REQUEST_DURATION = Histogram("query_api_request_duration_seconds", "API request duration", ["method", "endpoint"])
CACHE_HITS = Counter("query_api_cache_hits_total", "Cache hits", ["cache_type"])
CACHE_MISSES = Counter("query_api_cache_misses_total", "Cache misses", ["cache_type"])
WEBSOCKET_CLIENTS = Gauge("query_api_websocket_clients", "Currently connected live-feed WebSocket clients")

limiter = Limiter(key_func=get_remote_address, default_limits=[settings.rate_limit])


# --------------------------------------------------------------------------
# Pydantic response / request models
# --------------------------------------------------------------------------
class KPIResponse(BaseModel):
    source: str
    window: str
    timestamp: datetime
    count: int
    avg_metric: float
    p95_metric: float
    p99_metric: float = 0.0
    error_rate: float


class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    value: float


class SeriesResponse(BaseModel):
    source: str
    metric: str
    data: List[TimeSeriesPoint]


class Alert(BaseModel):
    id: int
    timestamp: datetime
    source: str
    anomaly_type: str
    severity: str
    value: float
    threshold: float
    z_score: float
    description: str
    resolved: bool


class SessionSummary(BaseModel):
    session_id: str
    source: str
    started_at: datetime
    ended_at: datetime
    event_count: int
    furthest_step: str
    converted: bool


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    services: Dict[str, str]


# --------------------------------------------------------------------------
# Database / cache connection management
# --------------------------------------------------------------------------
class DatabaseManager:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.pg_params: Dict[str, Any] = {}

    async def connect(self):
        self.redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )
        self.redis_client.ping()
        logger.info("Connected to Redis at %s:%s", settings.redis_host, settings.redis_port)

        self.pg_params = {
            "host": settings.postgres_host,
            "port": settings.postgres_port,
            "dbname": settings.postgres_db,
            "user": settings.postgres_user,
            "password": settings.postgres_password,
        }
        conn = psycopg2.connect(**self.pg_params)
        conn.close()
        logger.info("Connected to TimescaleDB at %s:%s/%s", settings.postgres_host, settings.postgres_port, settings.postgres_db)

    def get_redis(self) -> redis.Redis:
        return self.redis_client

    def get_pg_connection(self):
        return psycopg2.connect(**self.pg_params, cursor_factory=RealDictCursor)


db_manager = DatabaseManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db_manager.connect()
    yield
    if db_manager.redis_client:
        db_manager.redis_client.close()


app = FastAPI(
    title="Signal Intelligence Query API",
    description="Read API for real-time KPIs, historical series, anomaly alerts, and session analytics.",
    version="2.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_redis() -> redis.Redis:
    return db_manager.get_redis()


# --------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------
def parse_window(window: str) -> int:
    window_map = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
    return window_map.get(window, 60)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health_check():
    """Liveness check. Does not require an API key."""
    services: Dict[str, str] = {}

    try:
        db_manager.redis_client.ping()
        services["redis"] = "healthy"
    except Exception as e:
        services["redis"] = f"unhealthy: {e}"

    try:
        conn = db_manager.get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        services["timescaledb"] = "healthy"
    except Exception as e:
        services["timescaledb"] = f"unhealthy: {e}"

    status_str = "healthy" if all(v == "healthy" for v in services.values()) else "unhealthy"
    return HealthResponse(status=status_str, timestamp=datetime.utcnow(), services=services)


@app.get("/ready", tags=["ops"])
async def readiness_check():
    """Readiness probe for Kubernetes."""
    try:
        db_manager.redis_client.ping()
        conn = db_manager.get_pg_connection()
        conn.close()
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service not ready: {e}")


@app.get("/kpi", response_model=List[KPIResponse], dependencies=[Depends(require_api_key)])
@limiter.limit(settings.rate_limit)
async def get_kpi(
    request: Request,
    source: Optional[str] = Query(None, description="Event source filter"),
    window: str = Query("1m", description="Time window: 1m, 5m, 15m, 1h"),
    redis_client: redis.Redis = Depends(get_redis),
):
    """Real-time KPI reads from hot storage (Redis), populated by the
    aggregation streaming job on a sliding-window cadence."""

    with REQUEST_DURATION.labels(method="GET", endpoint="/kpi").time():
        try:
            prefix = settings.redis_key_prefix
            pattern = f"{prefix}:agg:{source or '*'}:{window}:*"
            keys = redis_client.keys(pattern)

            if not keys:
                CACHE_MISSES.labels(cache_type="kpi").inc()
                return []

            CACHE_HITS.labels(cache_type="kpi").inc()

            pipeline = redis_client.pipeline()
            for key in keys:
                pipeline.get(key)
            values = pipeline.execute()

            results: List[KPIResponse] = []
            for key, value in zip(keys, values):
                if not value:
                    continue
                try:
                    data = json.loads(value)
                    key_parts = key.split(":")
                    # sip:agg:{source}:{window}:{iso-timestamp}
                    results.append(
                        KPIResponse(
                            source=key_parts[2],
                            window=key_parts[3],
                            timestamp=datetime.fromisoformat(key_parts[4].replace("Z", "+00:00")),
                            count=data.get("count", 0),
                            avg_metric=data.get("avg_metric", 0.0),
                            p95_metric=data.get("p95_metric", 0.0),
                            p99_metric=data.get("p99_metric", 0.0),
                            error_rate=data.get("error_rate", 0.0),
                        )
                    )
                except (json.JSONDecodeError, IndexError, ValueError) as e:
                    logger.warning("Failed to parse KPI data for key %s: %s", key, e)

            results.sort(key=lambda x: x.timestamp, reverse=True)
            REQUEST_COUNT.labels(method="GET", endpoint="/kpi", status="success").inc()
            return results[:100]

        except Exception as e:
            REQUEST_COUNT.labels(method="GET", endpoint="/kpi", status="error").inc()
            logger.error("Error getting KPI data: %s", e)
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/series", response_model=List[SeriesResponse], dependencies=[Depends(require_api_key)])
@limiter.limit(settings.rate_limit)
async def get_series(
    request: Request,
    source: Optional[str] = Query(None, description="Event source filter"),
    start_time: datetime = Query(alias="from", description="Start time (ISO format)"),
    end_time: datetime = Query(alias="to", description="End time (ISO format)"),
    aggregation: str = Query("avg", description="Aggregation: avg, sum, count, p95"),
):
    """Historical time-series reads from cold storage (TimescaleDB)."""

    with REQUEST_DURATION.labels(method="GET", endpoint="/series").time():
        try:
            agg_map = {
                "avg": "AVG(metric)",
                "sum": "SUM(metric)",
                "count": "COUNT(*)",
                "p95": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric)",
            }
            agg_func = agg_map.get(aggregation, "AVG(metric)")

            query = f"""
                SELECT source, date_trunc('minute', ts) AS bucket, {agg_func} AS value
                FROM events_raw
                WHERE ts >= %s AND ts <= %s
            """
            params: List[Any] = [start_time, end_time]

            if source:
                query += " AND source = %s"
                params.append(source)

            query += " GROUP BY source, bucket ORDER BY source, bucket"

            conn = db_manager.get_pg_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            conn.close()

            series_data: Dict[str, List[TimeSeriesPoint]] = {}
            for row in rows:
                series_data.setdefault(row["source"], []).append(
                    TimeSeriesPoint(
                        timestamp=row["bucket"],
                        value=float(row["value"]) if row["value"] is not None else 0.0,
                    )
                )

            results = [
                SeriesResponse(source=name, metric=aggregation, data=points)
                for name, points in series_data.items()
            ]

            REQUEST_COUNT.labels(method="GET", endpoint="/series", status="success").inc()
            return results

        except Exception as e:
            REQUEST_COUNT.labels(method="GET", endpoint="/series", status="error").inc()
            logger.error("Error getting series data: %s", e)
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/alerts", response_model=List[Alert], dependencies=[Depends(require_api_key)])
@limiter.limit(settings.rate_limit)
async def get_alerts(
    request: Request,
    since: Optional[datetime] = Query(None, description="Get alerts since this time"),
    resolved: Optional[bool] = Query(None, description="Filter by resolved status"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
):
    """Anomaly alert history, written by the anomaly-detection streaming job."""

    with REQUEST_DURATION.labels(method="GET", endpoint="/alerts").time():
        try:
            query = "SELECT * FROM anomalies WHERE 1=1"
            params: List[Any] = []

            if since:
                query += " AND ts >= %s"
                params.append(since)
            if resolved is not None:
                query += " AND resolved = %s"
                params.append(resolved)
            if severity:
                query += " AND severity = %s"
                params.append(severity)

            query += " ORDER BY ts DESC LIMIT 1000"

            conn = db_manager.get_pg_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            conn.close()

            alerts = [
                Alert(
                    id=row["id"],
                    timestamp=row["ts"],
                    source=row["source"],
                    anomaly_type=row["anomaly_type"],
                    severity=row["severity"],
                    value=float(row["value"] or 0.0),
                    threshold=float(row["threshold"] or 0.0),
                    z_score=float(row["z_score"] or 0.0),
                    description=row["description"] or "",
                    resolved=row["resolved"],
                )
                for row in rows
            ]

            REQUEST_COUNT.labels(method="GET", endpoint="/alerts", status="success").inc()
            return alerts

        except Exception as e:
            REQUEST_COUNT.labels(method="GET", endpoint="/alerts", status="error").inc()
            logger.error("Error getting alerts: %s", e)
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions", response_model=List[SessionSummary], dependencies=[Depends(require_api_key)])
@limiter.limit(settings.rate_limit)
async def get_sessions(
    request: Request,
    source: Optional[str] = Query(None, description="Event source filter"),
    converted_only: bool = Query(False, description="Only return sessions that reached 'purchase'"),
    limit: int = Query(200, le=1000),
):
    """Reconstructed user sessions, written by the Spark session-analytics job
    (see streaming-jobs/session-analytics-job). Unlike the aggregation and
    anomaly-detection jobs, which each score individual events, this endpoint
    surfaces multi-event user journeys and funnel progress.
    """

    with REQUEST_DURATION.labels(method="GET", endpoint="/sessions").time():
        try:
            query = "SELECT * FROM sessions WHERE 1=1"
            params: List[Any] = []

            if source:
                query += " AND source = %s"
                params.append(source)
            if converted_only:
                query += " AND converted = TRUE"

            query += " ORDER BY ended_at DESC LIMIT %s"
            params.append(limit)

            conn = db_manager.get_pg_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            conn.close()

            sessions = [
                SessionSummary(
                    session_id=row["session_id"],
                    source=row["source"],
                    started_at=row["started_at"],
                    ended_at=row["ended_at"],
                    event_count=row["event_count"],
                    furthest_step=row["furthest_step"],
                    converted=row["converted"],
                )
                for row in rows
            ]

            REQUEST_COUNT.labels(method="GET", endpoint="/sessions", status="success").inc()
            return sessions

        except Exception as e:
            REQUEST_COUNT.labels(method="GET", endpoint="/sessions", status="error").inc()
            logger.error("Error getting sessions: %s", e)
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics", tags=["ops"])
async def get_metrics():
    """Prometheus scrape endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# --------------------------------------------------------------------------
# WebSocket live feed (new) — powers services/live-dashboard
# --------------------------------------------------------------------------
async def _snapshot_kpis(redis_client: redis.Redis) -> List[Dict[str, Any]]:
    prefix = settings.redis_key_prefix
    keys = redis_client.keys(f"{prefix}:agg:*:1m:*")
    if not keys:
        return []
    pipeline = redis_client.pipeline()
    for key in keys:
        pipeline.get(key)
    values = pipeline.execute()

    snapshot = []
    for key, value in zip(keys, values):
        if not value:
            continue
        try:
            data = json.loads(value)
            key_parts = key.split(":")
            snapshot.append(
                {
                    "source": key_parts[2],
                    "window": key_parts[3],
                    "timestamp": key_parts[4],
                    **data,
                }
            )
        except (json.JSONDecodeError, IndexError):
            continue
    snapshot.sort(key=lambda item: item["timestamp"], reverse=True)
    return snapshot[:50]


@app.websocket("/ws/live")
async def websocket_live_feed(websocket: WebSocket):
    """Pushes a fresh KPI + recent-alert snapshot every
    ``LIVE_FEED_INTERVAL_SECONDS`` seconds. Consumed by the live-dashboard
    service to drive the real-time charts without client-side polling."""

    await websocket.accept()
    WEBSOCKET_CLIENTS.inc()
    redis_client = db_manager.get_redis()

    try:
        while True:
            kpis = await _snapshot_kpis(redis_client)

            recent_alerts: List[Dict[str, Any]] = []
            try:
                conn = db_manager.get_pg_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT ts, source, severity, description FROM anomalies "
                        "ORDER BY ts DESC LIMIT 10"
                    )
                    recent_alerts = [dict(row) for row in cur.fetchall()]
                conn.close()
            except Exception as e:  # pragma: no cover - defensive
                logger.warning("Live feed could not read alerts: %s", e)

            payload = {
                "type": "snapshot",
                "generated_at": datetime.utcnow().isoformat(),
                "kpis": kpis,
                "recent_alerts": [
                    {**row, "ts": row["ts"].isoformat()} for row in recent_alerts
                ],
            }
            await websocket.send_json(payload)
            await asyncio.sleep(settings.live_feed_interval_seconds)

    except WebSocketDisconnect:
        logger.debug("Live feed client disconnected")
    finally:
        WEBSOCKET_CLIENTS.dec()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
