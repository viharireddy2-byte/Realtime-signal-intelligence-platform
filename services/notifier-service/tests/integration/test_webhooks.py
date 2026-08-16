"""Integration tests for notifier-service HTTP surface. Require a reachable
TimescaleDB (used by /alerts/stats); skipped automatically otherwise.
Kafka consumption itself is exercised indirectly — these tests hit the
webhook endpoints directly rather than round-tripping through Kafka.
"""

import os
import sys

import psycopg2
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def _infra_available() -> bool:
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "signalintel"),
            user=os.getenv("POSTGRES_USER", "signalintel_admin"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            connect_timeout=1,
        )
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _infra_available(),
    reason="TimescaleDB not reachable — start infra/docker-compose to run integration tests",
)


@pytest.fixture()
def client():
    from main import app

    with TestClient(app) as test_client:
        yield test_client


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_alertmanager_webhook_is_accepted(client):
    payload = {
        "receiver": "web.hook",
        "status": "firing",
        "alerts": [
            {
                "fingerprint": "abc123",
                "labels": {"instance": "query-api:8000", "severity": "warning"},
                "annotations": {"summary": "Integration test alert"},
            }
        ],
        "groupLabels": {},
        "commonLabels": {},
        "commonAnnotations": {},
        "externalURL": "http://alertmanager:9093",
        "version": "4",
        "groupKey": "{}",
    }
    response = client.post("/webhook/alerts", json=payload)
    assert response.status_code == 200
    assert response.json()["processed"] == 1


def test_alert_stats_endpoint(client):
    response = client.get("/alerts/stats")
    assert response.status_code == 200
    body = response.json()
    assert "stats" in body
