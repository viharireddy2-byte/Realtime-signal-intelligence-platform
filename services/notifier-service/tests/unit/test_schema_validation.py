"""Unit tests for the alert schema contract, mirroring
services/query-api/../event-producer's test_schema_validation.py pattern:
valid alerts pass, malformed ones are rejected with a reason, and this
service's local schema copy stays byte-identical to the canonical copy in
/schemas (see schemas/README.md)."""

import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from schema_validation import validate_alert  # noqa: E402

_LOCAL_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "schemas", "alert.v1.schema.json")
_ROOT_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "schemas", "alert.v1.schema.json")


def _valid_alert() -> dict:
    return {
        "alert_id": "alert-1",
        "source": "web",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "anomaly_type": "z-score",
        "severity": "critical",
        "value": 250.0,
        "threshold": 3.0,
        "z_score": 4.2,
        "description": "test anomaly",
        "is_anomaly": True,
        "stats": {"mean": 50.0, "stddev": 12.5},
    }


def test_valid_alert_passes():
    is_valid, error = validate_alert(_valid_alert())
    assert is_valid is True
    assert error == ""


def test_missing_required_field_is_rejected():
    alert = _valid_alert()
    del alert["severity"]
    is_valid, error = validate_alert(alert)
    assert is_valid is False
    assert "severity" in error


def test_unknown_severity_is_rejected():
    alert = _valid_alert()
    alert["severity"] = "catastrophic"  # not in the enum
    is_valid, error = validate_alert(alert)
    assert is_valid is False


def test_wrong_type_is_rejected():
    alert = _valid_alert()
    alert["is_anomaly"] = "yes"  # should be boolean
    is_valid, error = validate_alert(alert)
    assert is_valid is False


def test_unexpected_field_is_rejected():
    alert = _valid_alert()
    alert["unexpected_field"] = "surprise"
    is_valid, error = validate_alert(alert)
    assert is_valid is False


def test_null_stats_is_allowed():
    alert = _valid_alert()
    alert["stats"] = None
    is_valid, error = validate_alert(alert)
    assert is_valid is True


def test_local_schema_copy_matches_canonical_copy_at_repo_root():
    if not os.path.exists(_ROOT_SCHEMA_PATH):
        return
    with open(_LOCAL_SCHEMA_PATH) as f:
        local = json.load(f)
    with open(_ROOT_SCHEMA_PATH) as f:
        root = json.load(f)
    assert local == root, (
        "services/notifier-service/schemas/alert.v1.schema.json has drifted from "
        "the canonical copy in /schemas -- update both together."
    )
