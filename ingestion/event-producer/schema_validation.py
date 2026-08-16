"""JSON Schema validation for outbound events.

Loads `schemas/event.v1.schema.json` (shipped alongside this module inside
the event-producer's own Docker build context — see `../../schemas/README.md`
in the repo root for why it's duplicated here rather than imported from a
shared package) and validates every event dict before it is published to
Kafka.

Validation is on by default and controlled by the `SCHEMA_VALIDATION_ENABLED`
env var so it can be disabled for a quick local experiment without touching
code.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

from jsonschema import Draft7Validator

_SCHEMA_PATH = Path(__file__).parent / "schemas" / "event.v1.schema.json"

with open(_SCHEMA_PATH, "r", encoding="utf-8") as f:
    _EVENT_SCHEMA = json.load(f)

_validator = Draft7Validator(_EVENT_SCHEMA)

VALIDATION_ENABLED = os.getenv("SCHEMA_VALIDATION_ENABLED", "true").lower() == "true"


def validate_event(event_dict: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate an event dict against the event.v1 schema.

    Returns (True, "") if valid (or if validation is disabled), otherwise
    (False, "<human-readable reason>"). Never raises — a validation bug
    should never be able to take down the producer.
    """
    if not VALIDATION_ENABLED:
        return True, ""

    errors = sorted(_validator.iter_errors(event_dict), key=lambda e: e.path)
    if not errors:
        return True, ""

    first = errors[0]
    path = "/".join(str(p) for p in first.path) or "<root>"
    return False, f"{path}: {first.message}"
