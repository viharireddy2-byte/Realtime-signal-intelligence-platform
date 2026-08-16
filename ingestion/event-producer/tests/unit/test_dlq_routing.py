"""Unit tests for plan_publish() -- the pure routing decision behind
produce_event() (valid event -> main topic, invalid event -> DLQ topic).
Factored out specifically so this is testable without a live Kafka broker;
SignalEventProducer itself opens a real KafkaProducer connection in
__init__ and is exercised by loadtests/integration instead."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from event_producer import EventGenerator, plan_publish  # noqa: E402

TOPIC = "signal.events.v1"
DLQ_TOPIC = "signal.events.dlq"


def test_valid_event_is_routed_to_the_main_topic():
    event = EventGenerator().generate_event(source="web")
    plan = plan_publish(event, topic=TOPIC, dlq_topic=DLQ_TOPIC)

    assert plan.is_valid is True
    assert plan.topic == TOPIC
    assert json.loads(plan.payload)["event_id"] == event.event_id


def test_invalid_event_is_routed_to_the_dlq_topic_with_reason_attached():
    event = EventGenerator().generate_event(source="web")
    event.schema_version = "not-a-real-version"  # force a schema failure

    plan = plan_publish(event, topic=TOPIC, dlq_topic=DLQ_TOPIC)

    assert plan.is_valid is False
    assert plan.topic == DLQ_TOPIC
    assert plan.validation_error  # non-empty reason

    dlq_body = json.loads(plan.payload)
    assert dlq_body["error"] == plan.validation_error
    assert dlq_body["original_topic"] == TOPIC
    assert dlq_body["event"]["event_id"] == event.event_id


def test_headers_include_a_correlation_id_matching_the_event():
    event = EventGenerator().generate_event(source="web")
    plan = plan_publish(event, topic=TOPIC, dlq_topic=DLQ_TOPIC)

    header_dict = {k: v.decode("utf-8") for k, v in plan.headers}
    assert header_dict.get("correlation_id") == event.event_id
