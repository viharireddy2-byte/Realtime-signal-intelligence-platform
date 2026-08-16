"""Unit tests for NotifierService's rule/cooldown logic. Pure in-memory
logic — no Kafka, Postgres, or outbound network calls."""

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from main import AlertPayload, NotifierService  # noqa: E402


def make_alert(**overrides) -> AlertPayload:
    defaults = dict(
        alert_id="alert-1",
        source="web",
        timestamp=datetime.utcnow(),
        anomaly_type="z-score",
        severity="critical",
        value=250.0,
        threshold=3.0,
        z_score=4.2,
        description="test anomaly",
        is_anomaly=True,
    )
    defaults.update(overrides)
    return AlertPayload(**defaults)


@pytest.fixture()
def notifier():
    return NotifierService()


@pytest.mark.asyncio
async def test_non_anomaly_alerts_never_notify(notifier):
    alert = make_alert(is_anomaly=False)
    assert await notifier._should_notify(alert) is False


@pytest.mark.asyncio
async def test_matching_rule_triggers_notification(notifier):
    alert = make_alert(severity="critical")
    assert await notifier._should_notify(alert) is True


@pytest.mark.asyncio
async def test_severity_with_no_matching_rule_does_not_notify(notifier):
    alert = make_alert(severity="unknown-severity")
    assert await notifier._should_notify(alert) is False


@pytest.mark.asyncio
async def test_cooldown_suppresses_repeat_notifications(notifier):
    alert = make_alert(source="web", severity="critical")
    assert await notifier._should_notify(alert) is True
    # Immediately repeating the same source+severity should be suppressed
    # by the 1-minute cooldown on the "Critical Anomalies" rule.
    assert await notifier._should_notify(alert) is False


@pytest.mark.asyncio
async def test_cooldown_expires_after_window(notifier):
    alert = make_alert(source="web", severity="critical")
    assert await notifier._should_notify(alert) is True

    # Simulate the cooldown having already elapsed.
    notifier.recent_alerts["web:critical"] = datetime.utcnow() - timedelta(minutes=5)
    assert await notifier._should_notify(alert) is True


@pytest.mark.asyncio
async def test_source_scoped_rule_is_a_stricter_subset_of_the_generic_rule(notifier):
    # The default rule set has a source-scoped "High-Throughput Sources" rule
    # (web/api only) *and* a source-agnostic "Warning Anomalies" rule that
    # already matches every source at warning severity. So every source
    # notifies at warning severity — the source-scoped rule only matters if
    # the generic one is disabled/removed. This test documents that overlap
    # rather than assuming the source filter excludes anything by itself.
    alert = make_alert(source="iot-device", severity="warning")
    assert await notifier._should_notify(alert) is True

    notifier2 = NotifierService()
    alert2 = make_alert(source="api", severity="warning")
    assert await notifier2._should_notify(alert2) is True


@pytest.mark.asyncio
async def test_source_scoped_rule_excludes_unlisted_sources_when_generic_rule_disabled(notifier):
    # Disable the source-agnostic rule to isolate the source-scoped one.
    for rule in notifier.alert_rules:
        if rule.name == "Warning Anomalies":
            rule.enabled = False

    excluded = make_alert(source="iot-device", severity="warning")
    assert await notifier._should_notify(excluded) is False

    included = make_alert(source="api", severity="warning")
    assert await notifier._should_notify(included) is True
