"""Unit tests for query-api pure-function helpers. No network I/O — these
never touch Redis/TimescaleDB, so they run in any environment including CI
without the docker-compose stack up."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from main import parse_window  # noqa: E402


def test_parse_window_known_values():
    assert parse_window("1m") == 60
    assert parse_window("5m") == 300
    assert parse_window("15m") == 900
    assert parse_window("1h") == 3600
    assert parse_window("1d") == 86400


def test_parse_window_unknown_defaults_to_one_minute():
    assert parse_window("not-a-window") == 60
    assert parse_window("") == 60
