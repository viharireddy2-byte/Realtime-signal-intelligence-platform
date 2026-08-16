"""Centralized configuration for the query-api service.

All settings are sourced from environment variables so the same container
image works unmodified across docker-compose, Helm/Kubernetes, and local
development. Defaults match the local docker-compose stack.
"""

import os


class Settings:
    # Redis (hot path)
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    # TimescaleDB / PostgreSQL (cold path)
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "signalintel")
    postgres_user: str = os.getenv("POSTGRES_USER", "signalintel_admin")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "password")

    # Auth (disabled by default for local dev; enable for staging/production)
    api_key_required: bool = os.getenv("API_KEY_REQUIRED", "false").lower() == "true"
    api_key: str = os.getenv("API_KEY", "")

    # Rate limiting
    rate_limit: str = os.getenv("RATE_LIMIT", "600/minute")

    # WebSocket live-feed cadence
    live_feed_interval_seconds: float = float(os.getenv("LIVE_FEED_INTERVAL_SECONDS", "2.0"))

    # Redis key / cache namespace, kept short to minimize memory overhead
    redis_key_prefix: str = os.getenv("REDIS_KEY_PREFIX", "sip")


settings = Settings()
