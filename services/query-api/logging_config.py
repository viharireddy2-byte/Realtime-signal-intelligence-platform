"""Structured logging with request correlation IDs.

Every log line — and every response — gets an `X-Correlation-ID`. If the
caller sent one (e.g. a value threaded through from event-producer via a
Kafka `correlation_id` header and then into a client's API call), it's
reused; otherwise one is generated. This is what makes it possible to grep
one request's full story out of `docker compose logs` across services
without a tracing backend, and it's included as a span attribute when
OpenTelemetry tracing is enabled (see tracing.py) so the two systems agree
on the same identifier.

LOG_FORMAT=json emits one JSON object per line (what you want feeding into
Loki/CloudWatch/ELK in a real deployment). LOG_FORMAT=text (the default)
keeps the human-readable single-line format this service always used, just
with the correlation id appended, so local `docker compose logs -f` output
doesn't get harder to read.
"""

import contextvars
import json
import logging
import os
import uuid
from typing import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

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


def configure_logging(service_name: str) -> None:
    """Replaces the root logger's handlers. Call once at import/startup time,
    before any `logging.getLogger(...)` calls emit output."""
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


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Reads X-Correlation-ID (or X-Request-ID) from the incoming request if
    present, otherwise generates one. Binds it to a contextvar for the
    duration of the request so every log line emitted while handling it
    picks it up automatically, and echoes it back in the response header."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get("x-correlation-id") or request.headers.get("x-request-id")
        correlation_id = incoming or str(uuid.uuid4())
        token = correlation_id_var.set(correlation_id)
        try:
            response = await call_next(request)
        finally:
            correlation_id_var.reset(token)
        response.headers["X-Correlation-ID"] = correlation_id
        return response
