"""Unit tests for the event schema contract: valid events pass, malformed
ones are rejected with a useful reason, and this service's local copy of
the schema stays byte-identical to the canonical copy in /schemas (see
schemas/README.md for why it's duplicated instead of imported)."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from event_producer import EventGenerator  # noqa: E402
from schema_validation import validate_event  # noqa: E402

_LOCAL_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "schemas", "event.v1.schema.json")
_ROOT_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "schemas", "event.v1.schema.json")


def test_generated_events_are_always_schema_valid():
    generator = EventGenerator()
    for source in EventGenerator.SOURCES:
        event = generator.generate_event(source=source)
        is_valid, error = validate_event(event.to_dict())
        assert is_valid, f"generator produced an invalid event for source={source}: {error}"


def test_missing_required_field_is_rejected():
    event = EventGenerator().generate_event(source="web").to_dict()
    del event["schema_version"]
    is_valid, error = validate_event(event)
    assert is_valid is False
    assert "schema_version" in error


def test_wrong_type_is_rejected():
    event = EventGenerator().generate_event(source="web").to_dict()
    event["source"] = 12345  # should be a string
    is_valid, error = validate_event(event)
    assert is_valid is False


def test_unknown_schema_version_is_rejected():
    event = EventGenerator().generate_event(source="web").to_dict()
    event["schema_version"] = "99.0"
    is_valid, error = validate_event(event)
    assert is_valid is False


def test_unexpected_top_level_field_is_rejected():
    """additionalProperties: false at the top level -- catches a field being
    added to the envelope without a schema bump."""
    event = EventGenerator().generate_event(source="web").to_dict()
    event["extra_untracked_field"] = "surprise"
    is_valid, error = validate_event(event)
    assert is_valid is False


def test_local_schema_copy_matches_canonical_copy_at_repo_root():
    if not os.path.exists(_ROOT_SCHEMA_PATH):
        # Running from an environment where only this service's directory
        # was checked out (e.g. a Docker build context) -- nothing to
        # compare against, so there's nothing to drift.
        return
    with open(_LOCAL_SCHEMA_PATH) as f:
        local = json.load(f)
    with open(_ROOT_SCHEMA_PATH) as f:
        root = json.load(f)
    assert local == root, (
        "ingestion/event-producer/schemas/event.v1.schema.json has drifted from "
        "the canonical copy in /schemas -- update both together."
    )
