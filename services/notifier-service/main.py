"""
Notifier Service
=================

Consumes confirmed anomalies from ``signal.alerts.v1`` (written by the
anomaly-detection streaming job), applies notification rules with cooldowns,
persists them to TimescaleDB, and dispatches real notifications over email
(SMTP), Slack (incoming webhook), and arbitrary outbound webhooks. Also
accepts Alertmanager webhooks for infrastructure-level alerts so both
data-plane and infra-plane alerts land in one place.
"""

import asyncio
import json
import logging
import signal
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
import psycopg2
import smtplib
from email.mime.text import MIMEText
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

from config import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [notifier-service] %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Prometheus metrics
# --------------------------------------------------------------------------
ALERTS_PROCESSED = Counter("notifier_alerts_processed_total", "Total alerts processed", ["severity", "source"])
NOTIFICATIONS_SENT = Counter("notifier_notifications_sent_total", "Total notifications sent", ["channel", "status"])
ALERT_PROCESSING_TIME = Histogram("notifier_alert_processing_seconds", "Time spent processing an alert")
ACTIVE_ALERTS = Gauge("notifier_active_alerts", "Number of unresolved alerts in the last hour", ["severity"])


# --------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------
class AlertPayload(BaseModel):
    alert_id: str
    source: str
    timestamp: datetime
    anomaly_type: str
    severity: str
    value: float
    threshold: float
    z_score: float
    description: str
    is_anomaly: bool
    stats: Optional[Dict[str, float]] = None


class AlertRule(BaseModel):
    name: str
    severity_threshold: str
    sources: List[str] = []  # empty = all sources
    cooldown_minutes: int = 5
    enabled: bool = True


# --------------------------------------------------------------------------
# Notifier
# --------------------------------------------------------------------------
class NotifierService:
    def __init__(self):
        self.alert_rules = self._load_alert_rules()
        self.recent_alerts: Dict[str, datetime] = {}  # cooldown tracking
        self.running = False

        self.db_config = {
            "host": settings.postgres_host,
            "port": settings.postgres_port,
            "dbname": settings.postgres_db,
            "user": settings.postgres_user,
            "password": settings.postgres_password,
        }

        self.kafka_config = {
            "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
            "group_id": settings.consumer_group_id,
            "auto_offset_reset": "latest",
            "enable_auto_commit": True,
            "value_deserializer": lambda x: json.loads(x.decode("utf-8")),
        }

        self._http_session: Optional[aiohttp.ClientSession] = None

    @staticmethod
    def _load_alert_rules() -> List[AlertRule]:
        return [
            AlertRule(name="Critical Anomalies", severity_threshold="critical", cooldown_minutes=1),
            AlertRule(name="Warning Anomalies", severity_threshold="warning", cooldown_minutes=5),
            AlertRule(
                name="High-Throughput Sources",
                severity_threshold="warning",
                sources=["web", "api"],
                cooldown_minutes=10,
            ),
        ]

    async def start(self):
        self.running = True
        self._http_session = aiohttp.ClientSession()
        self._start_kafka_consumer_thread()
        logger.info("Notifier service started")

    async def stop(self):
        self.running = False
        if self._http_session:
            await self._http_session.close()
        logger.info("Notifier service stopped")

    def _start_kafka_consumer_thread(self):
        def consume_alerts():
            consumer = KafkaConsumer(settings.alerts_topic, **self.kafka_config)
            logger.info("Started Kafka consumer for topic '%s'", settings.alerts_topic)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                for message in consumer:
                    if not self.running:
                        break
                    try:
                        alert = AlertPayload(**message.value)
                        loop.run_until_complete(self.process_alert(alert))
                    except Exception as e:
                        logger.error("Error processing alert message: %s", e)
            except Exception as e:
                logger.error("Kafka consumer error: %s", e)
            finally:
                consumer.close()
                loop.close()

        thread = threading.Thread(target=consume_alerts, daemon=True)
        thread.start()

    async def process_alert(self, alert: AlertPayload):
        with ALERT_PROCESSING_TIME.time():
            try:
                ALERTS_PROCESSED.labels(severity=alert.severity, source=alert.source).inc()

                if await self._should_notify(alert):
                    self._store_alert(alert)
                    await self._send_notifications(alert)
                    self._update_active_alerts_metric()

                logger.info("Processed alert %s from %s (%s)", alert.alert_id, alert.source, alert.severity)
            except Exception as e:
                logger.error("Error processing alert %s: %s", alert.alert_id, e)

    async def _should_notify(self, alert: AlertPayload) -> bool:
        if not alert.is_anomaly:
            return False

        applicable_rules = [
            rule
            for rule in self.alert_rules
            if rule.enabled
            and (not rule.sources or alert.source in rule.sources)
            and rule.severity_threshold == alert.severity
        ]
        if not applicable_rules:
            return False

        cooldown_key = f"{alert.source}:{alert.severity}"
        now = datetime.utcnow()

        if cooldown_key in self.recent_alerts:
            elapsed = now - self.recent_alerts[cooldown_key]
            min_cooldown = min(rule.cooldown_minutes for rule in applicable_rules)
            if elapsed < timedelta(minutes=min_cooldown):
                return False

        self.recent_alerts[cooldown_key] = now
        return True

    def _store_alert(self, alert: AlertPayload):
        try:
            conn = psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO anomalies (ts, source, anomaly_type, severity, value, threshold, z_score, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        alert.timestamp,
                        alert.source,
                        alert.anomaly_type,
                        alert.severity,
                        alert.value,
                        alert.threshold,
                        alert.z_score,
                        alert.description,
                    ),
                )
                conn.commit()
            conn.close()
        except Exception as e:
            logger.error("Error storing alert in database: %s", e)

    async def _send_notifications(self, alert: AlertPayload):
        tasks = []

        if settings.email_enabled and settings.email_recipients:
            tasks.append(self._send_email_notification(alert))
        if settings.slack_enabled and settings.slack_webhook_url:
            tasks.append(self._send_slack_notification(alert))
        if settings.webhook_enabled and settings.custom_webhooks:
            for url in settings.custom_webhooks:
                tasks.append(self._send_webhook_notification(alert, url))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_email_notification(self, alert: AlertPayload):
        """Send an email via SMTP. Runs the blocking smtplib call in a thread
        so it doesn't stall the event loop."""
        subject = f"[{alert.severity.upper()}] Signal anomaly in {alert.source}"
        body = (
            f"Source: {alert.source}\n"
            f"Severity: {alert.severity}\n"
            f"Type: {alert.anomaly_type}\n"
            f"Value: {alert.value:.2f}\n"
            f"Threshold: {alert.threshold:.2f}\n"
            f"Z-Score: {alert.z_score:.2f}\n"
            f"Time: {alert.timestamp}\n"
            f"Description: {alert.description}\n\n"
            f"Stats: {json.dumps(alert.stats, indent=2) if alert.stats else 'N/A'}"
        )

        if not settings.smtp_host:
            logger.info("SMTP not configured (SMTP_HOST unset); skipping email for %s", alert.alert_id)
            NOTIFICATIONS_SENT.labels(channel="email", status="skipped").inc()
            return

        def _send_sync():
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = settings.smtp_from
            msg["To"] = ", ".join(settings.email_recipients)

            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
                server.starttls()
                if settings.smtp_user:
                    server.login(settings.smtp_user, settings.smtp_password)
                server.sendmail(settings.smtp_from, settings.email_recipients, msg.as_string())

        try:
            await asyncio.to_thread(_send_sync)
            NOTIFICATIONS_SENT.labels(channel="email", status="success").inc()
        except Exception as e:
            logger.error("Error sending email notification: %s", e)
            NOTIFICATIONS_SENT.labels(channel="email", status="error").inc()

    async def _send_slack_notification(self, alert: AlertPayload):
        color = {"critical": "#E01E5A", "warning": "#ECB22E", "info": "#2EB67D"}.get(alert.severity, "#808080")
        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": f"{alert.severity.upper()} anomaly: {alert.source}",
                    "text": alert.description,
                    "fields": [
                        {"title": "Value", "value": f"{alert.value:.2f}", "short": True},
                        {"title": "Z-Score", "value": f"{alert.z_score:.2f}", "short": True},
                        {"title": "Time", "value": alert.timestamp.isoformat(), "short": False},
                    ],
                    "ts": int(alert.timestamp.timestamp()),
                }
            ]
        }
        try:
            async with self._http_session.post(
                settings.slack_webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
            NOTIFICATIONS_SENT.labels(channel="slack", status="success").inc()
        except Exception as e:
            logger.error("Error sending Slack notification: %s", e)
            NOTIFICATIONS_SENT.labels(channel="slack", status="error").inc()

    async def _send_webhook_notification(self, alert: AlertPayload, webhook_url: str):
        payload = {
            "alert_id": alert.alert_id,
            "source": alert.source,
            "severity": alert.severity,
            "description": alert.description,
            "timestamp": alert.timestamp.isoformat(),
            "value": alert.value,
            "z_score": alert.z_score,
            "stats": alert.stats,
        }
        try:
            async with self._http_session.post(
                webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
            NOTIFICATIONS_SENT.labels(channel="webhook", status="success").inc()
        except Exception as e:
            logger.error("Error sending webhook notification to %s: %s", webhook_url, e)
            NOTIFICATIONS_SENT.labels(channel="webhook", status="error").inc()

    def _update_active_alerts_metric(self):
        try:
            conn = psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT severity, COUNT(*) AS count
                    FROM anomalies
                    WHERE resolved = false AND ts > NOW() - INTERVAL '1 hour'
                    GROUP BY severity
                    """
                )
                results = cur.fetchall()

            for severity in ["critical", "warning", "info"]:
                ACTIVE_ALERTS.labels(severity=severity).set(0)
            for row in results:
                ACTIVE_ALERTS.labels(severity=row["severity"]).set(row["count"])

            conn.close()
        except Exception as e:
            logger.error("Error updating active alerts metric: %s", e)


notifier = NotifierService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await notifier.start()
    yield
    await notifier.stop()


app = FastAPI(
    title="Signal Intelligence Notifier Service",
    description="Real-time alert processing, cooldown rules, and multi-channel notification dispatch.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "notifier-service",
        "timestamp": datetime.utcnow(),
        "version": "2.0.0",
    }


@app.post("/webhook/alerts")
async def receive_alertmanager_webhook(payload: Dict[str, Any]):
    """Receive a standard Prometheus Alertmanager webhook payload."""
    try:
        logger.info("Received Alertmanager webhook: %s", payload.get("status"))
        for alert_data in payload.get("alerts", []):
            alert = AlertPayload(
                alert_id=alert_data.get("fingerprint", "unknown"),
                source=alert_data.get("labels", {}).get("instance", "unknown"),
                timestamp=datetime.utcnow(),
                anomaly_type="infrastructure",
                severity=alert_data.get("labels", {}).get("severity", "warning"),
                value=0.0,
                threshold=0.0,
                z_score=0.0,
                description=alert_data.get("annotations", {}).get("summary", "Infrastructure alert"),
                is_anomaly=True,
            )
            await notifier.process_alert(alert)
        return {"status": "success", "processed": len(payload.get("alerts", []))}
    except Exception as e:
        logger.error("Error processing Alertmanager webhook: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/webhook/critical")
async def receive_critical_alerts(payload: Dict[str, Any]):
    logger.info("Received critical alert webhook: %s", payload)
    return {"status": "received", "severity": "critical"}


@app.post("/webhook/warning")
async def receive_warning_alerts(payload: Dict[str, Any]):
    logger.info("Received warning alert webhook: %s", payload)
    return {"status": "received", "severity": "warning"}


@app.get("/metrics")
async def get_metrics():
    from fastapi.responses import Response

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/alerts/stats")
async def get_alert_stats():
    try:
        conn = psycopg2.connect(**notifier.db_config, cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    severity,
                    COUNT(*) AS total,
                    COUNT(CASE WHEN resolved = false THEN 1 END) AS active,
                    COUNT(CASE WHEN ts > NOW() - INTERVAL '1 hour' THEN 1 END) AS last_hour
                FROM anomalies
                WHERE ts > NOW() - INTERVAL '24 hours'
                GROUP BY severity
                """
            )
            stats = cur.fetchall()
        conn.close()
        return {"timestamp": datetime.utcnow(), "stats": [dict(row) for row in stats]}
    except Exception as e:
        logger.error("Error getting alert stats: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _signal_handler(signum, frame):
    logger.info("Received signal %s, shutting down...", signum)
    notifier.running = False
    sys.exit(0)


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
