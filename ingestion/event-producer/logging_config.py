"""Structured logging with a per-event correlation ID.

Same JSON/text formatter convention as the FastAPI services (see
services/query-api/logging_config.py for the full rationale) but without a
request middleware to drive it — instead, `bind_correlation_id()` is called
once per event/batch in event_producer.py so log lines emitted while
producing that event carry its correlation id, and the same id is written
into the Kafka message headers by tracing.kafka_trace_headers() so a
consumer can pick the thread back up.
"""

import contextvars
import json
import logging
import os

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


class _CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get()
        return True


class _JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "correlation_id": getattr(record, "correlation_id", "-"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(service_name: str = "event-producer") -> None:
    log_format = os.getenv("LOG_FORMAT", "text").lower()
    handler = logging.StreamHandler()
    handler.addFilter(_CorrelationIdFilter())

    if log_format == "json":
        handler.setFormatter(_JsonFormatter(service_name))
    else:
        handler.setFormatter(
            logging.Formatter(
                f"%(asctime)s %(levelname)s [{service_name}] [%(correlation_id)s] %(message)s"
            )
        )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


def bind_correlation_id(correlation_id: str) -> None:
    correlation_id_var.set(correlation_id)
