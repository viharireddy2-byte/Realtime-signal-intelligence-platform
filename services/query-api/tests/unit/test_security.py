"""Unit tests for the optional API-key auth dependency."""

import os
import sys

import pytest
from fastapi import HTTPException

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import config  # noqa: E402
from security import require_api_key  # noqa: E402


@pytest.mark.asyncio
async def test_require_api_key_noop_when_disabled(monkeypatch):
    monkeypatch.setattr(config.settings, "api_key_required", False)
    # Should not raise even with no key supplied.
    await require_api_key(x_api_key=None)


@pytest.mark.asyncio
async def test_require_api_key_rejects_missing_key_when_enabled(monkeypatch):
    monkeypatch.setattr(config.settings, "api_key_required", True)
    monkeypatch.setattr(config.settings, "api_key", "secret123")

    with pytest.raises(HTTPException) as exc_info:
        await require_api_key(x_api_key=None)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_require_api_key_rejects_wrong_key_when_enabled(monkeypatch):
    monkeypatch.setattr(config.settings, "api_key_required", True)
    monkeypatch.setattr(config.settings, "api_key", "secret123")

    with pytest.raises(HTTPException):
        await require_api_key(x_api_key="wrong-key")


@pytest.mark.asyncio
async def test_require_api_key_accepts_correct_key_when_enabled(monkeypatch):
    monkeypatch.setattr(config.settings, "api_key_required", True)
    monkeypatch.setattr(config.settings, "api_key", "secret123")

    await require_api_key(x_api_key="secret123")
