"""Unit tests for NotifierService._send_to_dlq(). The real DLQ path opens a
live KafkaProducer (see _get_dlq_producer()), so these tests monkeypatch it
with an in-memory fake -- consistent with test_notifier_rules.py's approach
of exercising NotifierService's logic without any real Kafka/Postgres/network
connection."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from main import ALERTS_DLQ, NotifierService  # noqa: E402


class _FakeDlqProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})

    def flush(self, timeout=None):
        pass


def test_send_to_dlq_publishes_reason_and_original_payload(monkeypatch):
    notifier = NotifierService()
    fake_producer = _FakeDlqProducer()
    monkeypatch.setattr(notifier, "_get_dlq_producer", lambda: fake_producer)

    raw_value = b'{"source": "web", "not": "a valid alert"}'
    notifier._send_to_dlq(b"web", "schema validation failed: severity is required", raw_value, source="web")

    assert len(fake_producer.sent) == 1
    sent = fake_producer.sent[0]
    assert sent["topic"] == "signal.alerts.dlq"
    assert sent["key"] == "web"

    body = json.loads(sent["value"])
    assert body["error"] == "schema validation failed: severity is required"
    assert "web" in body["raw_value"]
    assert body["original_topic"]


def test_send_to_dlq_increments_metric():
    notifier = NotifierService()
    before = ALERTS_DLQ.labels(source="test-metric-source")._value.get()

    class _NoopProducer(_FakeDlqProducer):
        pass

    notifier._dlq_producer = _NoopProducer()
    notifier._send_to_dlq(None, "boom", b"{}", source="test-metric-source")

    after = ALERTS_DLQ.labels(source="test-metric-source")._value.get()
    assert after == before + 1


def test_send_to_dlq_does_not_raise_if_producer_is_broken():
    """A DLQ publish failure should be logged, not propagated -- otherwise a
    broken DLQ topic could take down the primary consumer loop, which is
    exactly the failure mode the DLQ exists to avoid."""
    notifier = NotifierService()

    class _BrokenProducer:
        def send(self, *args, **kwargs):
            raise RuntimeError("kafka is down")

    notifier._dlq_producer = _BrokenProducer()
    notifier._send_to_dlq(b"web", "some error", b"{}", source="web")  # must not raise
