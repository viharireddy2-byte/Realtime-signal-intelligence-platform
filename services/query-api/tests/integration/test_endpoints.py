"""Integration tests for query-api. Require a live Redis + TimescaleDB
(see infra/docker-compose or the `postgres`/`redis` service containers the
CI workflow spins up). Skipped automatically if those aren't reachable so
`pytest` still passes in an environment with no infra running.
"""

import os
import sys

import pytest
import redis as redis_lib
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _infra_available() -> bool:
    try:
        client = redis_lib.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            socket_connect_timeout=1,
        )
        client.ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _infra_available(),
    reason="Redis/TimescaleDB not reachable — start infra/docker-compose to run integration tests",
)


@pytest.fixture()
def client():
    from main import app

    with TestClient(app) as test_client:
        yield test_client


def test_health_endpoint_reports_service_status(client):
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert "status" in body
    assert "redis" in body["services"]
    assert "timescaledb" in body["services"]


def test_ready_endpoint(client):
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_kpi_endpoint_returns_list(client):
    response = client.get("/kpi", params={"window": "1m"})
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_alerts_endpoint_returns_list(client):
    response = client.get("/alerts")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_sessions_endpoint_returns_list(client):
    response = client.get("/sessions")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_metrics_endpoint_is_prometheus_text(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "query_api_requests_total" in response.text or response.text == ""
