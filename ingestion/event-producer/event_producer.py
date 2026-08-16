"""
Signal Event Producer
======================

Synthetic high-volume event generator for the Real-Time Signal Intelligence
Platform. Publishes JSON-encoded telemetry events to Kafka at a configurable
throughput, with realistic value distributions, correlated status codes, and
source-specific attribute enrichment.

This is the "front door" of the platform: every downstream job (aggregation,
anomaly detection, session analytics) consumes the stream this module writes
to the `signal.events.v1` topic.
"""

import asyncio
import json
import uuid
import random
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import logging
from dataclasses import dataclass, field, asdict
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import argparse
from prometheus_client import Counter, Histogram, Gauge, start_http_server

SCHEMA_VERSION = "2.0"

# --------------------------------------------------------------------------
# Prometheus metrics
# --------------------------------------------------------------------------
EVENTS_PRODUCED = Counter(
    "signal_events_produced_total", "Total number of events produced", ["source", "status"]
)
PRODUCE_DURATION = Histogram(
    "signal_event_produce_duration_seconds", "Time spent producing a single event"
)
KAFKA_ERRORS = Counter(
    "signal_kafka_errors_total", "Total number of Kafka producer errors", ["error_type"]
)
EVENTS_PER_SECOND = Gauge(
    "signal_events_per_second", "Current observed event production rate"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [event-producer] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Canonical event envelope written to Kafka."""

    event_id: str
    schema_version: str
    source: str
    timestamp: str
    attributes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))


class EventGenerator:
    """Generates synthetic telemetry events with realistic distributions.

    Enhancements over a naive generator:
    - 5% outlier injection so the anomaly-detection job has real signal to find
    - status correlated with metric magnitude (higher metric -> more errors)
    - source-specific attribute enrichment (web/mobile/iot/service)
    - a `funnel_step` + `session_id` pairing so the session-analytics job can
      reconstruct user journeys
    - `user_id` is hashed before it ever leaves the producer, following the
      data-anonymization guidance in docs/SECURITY.md
    """

    SOURCES = ["web", "mobile", "api", "iot-device", "service-checkout", "service-search"]
    STATUSES = ["ok", "warning", "error"]
    FUNNEL_STEPS = ["landing", "product_view", "add_to_cart", "checkout", "purchase"]
    REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-northeast"]

    def __init__(self, session_pool_size: int = 4000):
        self._user_ids = [f"user_{i}" for i in range(1000, 10000)]
        # A bounded pool of session ids lets the same session appear across
        # multiple events, which is what makes session reconstruction possible.
        self._session_pool = [str(uuid.uuid4()) for _ in range(session_pool_size)]

    @staticmethod
    def _hash_user_id(raw_user_id: str) -> str:
        """Anonymize the user identifier (see docs/SECURITY.md)."""
        return hashlib.sha256(raw_user_id.encode("utf-8")).hexdigest()[:16]

    def generate_event(self, source: Optional[str] = None) -> Event:
        if source is None:
            source = random.choice(self.SOURCES)

        # 5% chance of an outlier value so anomaly detection has real signal
        if random.random() < 0.05:
            metric = random.uniform(100, 500)
        else:
            metric = max(0.0, random.normalvariate(50, 15))

        if metric > 100:
            status = random.choices(self.STATUSES, weights=[0.3, 0.4, 0.3])[0]
        else:
            status = random.choices(self.STATUSES, weights=[0.8, 0.15, 0.05])[0]

        raw_user_id = random.choice(self._user_ids)
        session_id = random.choice(self._session_pool)

        attributes: Dict[str, Any] = {
            "user_id": self._hash_user_id(raw_user_id),
            "metric": round(metric, 2),
            "status": status,
            "session_id": session_id,
            "funnel_step": random.choice(self.FUNNEL_STEPS),
            "region": random.choice(self.REGIONS),
            "version": random.choice(["1.0.0", "1.1.0", "1.2.0", "2.0.0"]),
        }

        if source == "web":
            attributes.update(
                {
                    "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
                    "page_load_time_s": round(random.uniform(0.5, 5.0), 2),
                }
            )
        elif source == "mobile":
            attributes.update(
                {
                    "platform": random.choice(["ios", "android"]),
                    "app_version": random.choice(["2.1.0", "2.2.0", "2.3.0"]),
                }
            )
        elif source == "iot-device":
            attributes.update(
                {
                    "device_type": random.choice(["sensor", "gateway", "controller"]),
                    "temperature_c": round(random.uniform(15, 35), 1),
                    "battery_pct": random.randint(0, 100),
                }
            )
        elif source.startswith("service-"):
            attributes.update(
                {
                    "downstream_latency_ms": round(random.uniform(5, 250), 1),
                    "retry_count": random.choices([0, 1, 2, 3], weights=[0.85, 0.1, 0.04, 0.01])[0],
                }
            )

        return Event(
            event_id=str(uuid.uuid4()),
            schema_version=SCHEMA_VERSION,
            source=source,
            timestamp=datetime.now(timezone.utc).isoformat(),
            attributes=attributes,
        )


class SignalEventProducer:
    """Kafka producer wrapper with throughput control and delivery metrics."""

    def __init__(
        self,
        bootstrap_servers: List[str] = None,
        topic: str = "signal.events.v1",
        **kafka_config,
    ):
        self.topic = topic
        self.event_generator = EventGenerator()

        producer_config = {
            "bootstrap_servers": bootstrap_servers or ["localhost:9092"],
            "value_serializer": lambda v: v.encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "acks": 1,
            "retries": 3,
            "batch_size": 16384,
            "linger_ms": 10,
            "buffer_memory": 33554432,
            "compression_type": "snappy",
            **kafka_config,
        }

        self.producer = KafkaProducer(**producer_config)
        logger.info("Kafka producer initialized (topic=%s)", self.topic)

    def _delivery_callback(self, source: str):
        def callback(record_metadata=None, exception=None):
            if exception:
                logger.error("Failed to deliver event: %s", exception)
                KAFKA_ERRORS.labels(error_type=type(exception).__name__).inc()
                EVENTS_PRODUCED.labels(source=source, status="failed").inc()
            else:
                EVENTS_PRODUCED.labels(source=source, status="success").inc()

        return callback

    @PRODUCE_DURATION.time()
    def produce_event(self, event: Event):
        try:
            future = self.producer.send(self.topic, key=event.source, value=event.to_json())
            future.add_callback(self._delivery_callback(event.source))
            future.add_errback(lambda e: self._delivery_callback(event.source)(exception=e))
            return future
        except KafkaError as e:
            logger.error("Error producing event: %s", e)
            KAFKA_ERRORS.labels(error_type=type(e).__name__).inc()
            raise

    def produce_batch(self, events: List[Event]):
        futures = [self.produce_event(event) for event in events]
        self.producer.flush()
        return futures

    async def run_continuous(
        self,
        events_per_second: int = 100,
        duration_seconds: Optional[int] = None,
        sources: Optional[List[str]] = None,
    ):
        logger.info("Starting continuous event generation at %s events/sec", events_per_second)

        sources = sources or EventGenerator.SOURCES
        start_time = time.time()
        event_count = 0
        last_rate_update = start_time

        try:
            while True:
                batch_start = time.time()
                batch_size = min(events_per_second, 500)
                events = [
                    self.event_generator.generate_event(random.choice(sources))
                    for _ in range(batch_size)
                ]

                self.produce_batch(events)
                event_count += len(events)

                current_time = time.time()
                if current_time - last_rate_update >= 1.0:
                    rate = event_count / (current_time - start_time)
                    EVENTS_PER_SECOND.set(rate)
                    last_rate_update = current_time
                    logger.info("Produced %s events, rate=%.2f/s", event_count, rate)

                if duration_seconds and (current_time - start_time) >= duration_seconds:
                    logger.info("Reached duration limit of %s seconds", duration_seconds)
                    break

                batch_duration = time.time() - batch_start
                target_duration = len(events) / events_per_second
                sleep_time = max(0.0, target_duration - batch_duration)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping...")
        finally:
            self.close()

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    parser = argparse.ArgumentParser(description="Signal Event Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="signal.events.v1", help="Kafka topic")
    parser.add_argument("--rate", type=int, default=100, help="Events per second")
    parser.add_argument("--duration", type=int, help="Duration in seconds (infinite if not set)")
    parser.add_argument("--sources", nargs="+", help="List of sources to use")
    parser.add_argument("--metrics-port", type=int, default=8002, help="Prometheus metrics port")

    args = parser.parse_args()

    start_http_server(args.metrics_port)
    logger.info("Prometheus metrics available at http://localhost:%s/metrics", args.metrics_port)

    producer = SignalEventProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        topic=args.topic,
    )

    asyncio.run(
        producer.run_continuous(
            events_per_second=args.rate,
            duration_seconds=args.duration,
            sources=args.sources,
        )
    )


if __name__ == "__main__":
    main()
