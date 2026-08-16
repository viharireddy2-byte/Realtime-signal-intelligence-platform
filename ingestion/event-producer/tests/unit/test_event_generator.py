"""Unit tests for EventGenerator. Pure in-memory logic — no Kafka connection,
so these run in any environment including CI without the docker-compose
stack up (mirrors the convention used in services/query-api/tests/unit and
services/notifier-service/tests/unit)."""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from event_producer import Event, EventGenerator, SCHEMA_VERSION  # noqa: E402

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def test_generate_event_has_required_envelope_fields():
    event = EventGenerator().generate_event(source="web")
    assert event.source == "web"
    assert event.schema_version == SCHEMA_VERSION
    assert UUID_RE.match(event.event_id)
    assert "T" in event.timestamp  # ISO 8601


def test_generate_event_hashes_user_id_not_raw():
    event = EventGenerator().generate_event(source="web")
    user_id = event.attributes["user_id"]
    assert not user_id.startswith("user_")  # never the raw "user_1234" form
    assert len(user_id) == 16  # sha256 hexdigest, truncated


def test_generate_event_source_specific_attributes():
    web = EventGenerator().generate_event(source="web")
    assert "browser" in web.attributes

    iot = EventGenerator().generate_event(source="iot-device")
    assert "temperature_c" in iot.attributes
    assert 15 <= iot.attributes["temperature_c"] <= 35

    svc = EventGenerator().generate_event(source="service-checkout")
    assert "downstream_latency_ms" in svc.attributes


def test_generate_event_funnel_step_is_a_known_step():
    event = EventGenerator().generate_event(source="mobile")
    assert event.attributes["funnel_step"] in EventGenerator.FUNNEL_STEPS


def test_event_to_json_round_trips_through_to_dict():
    event = EventGenerator().generate_event(source="api")
    assert event.to_dict()["event_id"] == event.event_id
    assert "attributes" in event.to_json()


def test_session_ids_repeat_across_events_from_the_same_generator():
    """The session pool is bounded and reused so downstream session
    reconstruction has something to reconstruct — with a small pool size,
    a handful of events should collide on session_id."""
    generator = EventGenerator(session_pool_size=2)
    sessions = {generator.generate_event().attributes["session_id"] for _ in range(50)}
    assert len(sessions) <= 2
