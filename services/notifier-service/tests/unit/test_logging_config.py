"""Unit tests for CorrelationIdMiddleware. Built against a minimal standalone
FastAPI app (not the real `main.app`, which opens live Redis/TimescaleDB
connections on startup) so this runs without the docker-compose stack up,
consistent with the rest of tests/unit."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from logging_config import CorrelationIdMiddleware, correlation_id_var  # noqa: E402

app = FastAPI()
app.add_middleware(CorrelationIdMiddleware)


@app.get("/probe")
async def probe():
    return {"correlation_id": correlation_id_var.get()}


client = TestClient(app)


def test_generates_a_correlation_id_when_none_supplied():
    resp = client.get("/probe")
    assert resp.status_code == 200
    header_id = resp.headers["x-correlation-id"]
    assert header_id
    assert resp.json()["correlation_id"] == header_id


def test_reuses_an_incoming_correlation_id_header():
    resp = client.get("/probe", headers={"X-Correlation-ID": "test-trace-abc123"})
    assert resp.headers["x-correlation-id"] == "test-trace-abc123"
    assert resp.json()["correlation_id"] == "test-trace-abc123"


def test_falls_back_to_x_request_id_header():
    resp = client.get("/probe", headers={"X-Request-ID": "req-xyz"})
    assert resp.headers["x-correlation-id"] == "req-xyz"


def test_different_requests_get_different_ids():
    id_a = client.get("/probe").headers["x-correlation-id"]
    id_b = client.get("/probe").headers["x-correlation-id"]
    assert id_a != id_b
