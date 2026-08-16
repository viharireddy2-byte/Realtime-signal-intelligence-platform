"""JSON Schema validation for inbound alerts.

Runs *before* the Pydantic ``AlertPayload`` model is constructed. Pydantic
already enforces types on the fields it knows about, but it doesn't reject
the message the way a strict data contract should (e.g. it won't reject
unexpected extra fields, and a missing-vs-null distinction gets lost in
translation). This module is the strict contract check whose failures get
routed to the DLQ; Pydantic remains the second, more ergonomic layer used
by the rest of the service.

Loads `schemas/alert.v1.schema.json`, shipped alongside this module inside
notifier-service's own Docker build context (see `../../schemas/README.md`
at the repo root for why it's duplicated here).
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

from jsonschema import Draft7Validator

_SCHEMA_PATH = Path(__file__).parent / "schemas" / "alert.v1.schema.json"

with open(_SCHEMA_PATH, "r", encoding="utf-8") as f:
    _ALERT_SCHEMA = json.load(f)

_validator = Draft7Validator(_ALERT_SCHEMA)

VALIDATION_ENABLED = os.getenv("SCHEMA_VALIDATION_ENABLED", "true").lower() == "true"


def validate_alert(alert_dict: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate an alert dict against the alert.v1 schema.

    Returns (True, "") if valid (or if validation is disabled), otherwise
    (False, "<human-readable reason>"). Never raises.
    """
    if not VALIDATION_ENABLED:
        return True, ""

    errors = sorted(_validator.iter_errors(alert_dict), key=lambda e: e.path)
    if not errors:
        return True, ""

    first = errors[0]
    path = "/".join(str(p) for p in first.path) or "<root>"
    return False, f"{path}: {first.message}"
