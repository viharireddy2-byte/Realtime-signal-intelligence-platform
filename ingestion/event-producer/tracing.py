"""OpenTelemetry distributed tracing setup for the event-producer.

Unlike the FastAPI services, the producer isn't request-driven, so there's
no framework auto-instrumentation to hook into. Instead:

- `init_tracing()` sets up the same OTLP/HTTP exporter as the other
  services (see services/query-api/tracing.py for the full rationale and
  the note on why context propagation currently stops at the JVM streaming
  jobs).
- `kafka_trace_headers()` injects the current trace context into a list of
  Kafka message headers (`kafka-python`'s expected `[(key, bytes), ...]`
  format) alongside a `correlation_id` header, so any consumer that also
  uses OpenTelemetry can continue the same trace.

Env vars: see services/query-api/tracing.py docstring (OTEL_TRACES_ENABLED,
OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME).
"""

import logging
import os
from typing import List, Tuple

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

_initialized = False


def tracing_enabled() -> bool:
    return os.getenv("OTEL_TRACES_ENABLED", "true").lower() == "true"


def init_tracing(service_name: str = "event-producer") -> trace.Tracer:
    global _initialized

    if not tracing_enabled():
        logger.info("OpenTelemetry tracing disabled (OTEL_TRACES_ENABLED=false)")
        return trace.get_tracer(service_name)

    if not _initialized:
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")
        resource = Resource.create({SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", service_name)})
        provider = TracerProvider(resource=resource)
        try:
            exporter = OTLPSpanExporter(endpoint=endpoint)
            provider.add_span_processor(BatchSpanProcessor(exporter))
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Could not initialize OTLP span exporter (%s); spans will be created but not exported.", e)
        trace.set_tracer_provider(provider)
        logger.info("OpenTelemetry tracing initialized (service=%s, endpoint=%s)", service_name, endpoint)
        _initialized = True

    return trace.get_tracer(service_name)


def kafka_trace_headers(correlation_id: str) -> List[Tuple[str, bytes]]:
    """Build the Kafka message headers for the currently-active span context,
    plus a plain-text correlation_id header that doesn't require an
    OpenTelemetry-aware consumer to be useful (e.g. it shows up as-is in
    kafka-ui and in DLQ dumps)."""
    carrier: dict = {}
    propagate.inject(carrier)
    headers = [(k, v.encode("utf-8")) for k, v in carrier.items()]
    headers.append(("correlation_id", correlation_id.encode("utf-8")))
    return headers
