"""OpenTelemetry distributed tracing setup.

Exports spans to an OTLP/HTTP collector (Jaeger's built-in OTLP receiver in
docker-compose and Helm — see infra/docker-compose/docker-compose.yml and
infra/helm/signal-intel-platform/templates/jaeger-deployment.yaml) so a
request can be followed through query-api's own handlers and its Redis /
TimescaleDB calls in the Jaeger UI (http://localhost:16686 locally).

Deliberately safe to leave enabled with no collector reachable: the OTLP
exporter batches spans on a background thread and drops them on export
failure rather than blocking or raising into request handling.

Env vars:
- OTEL_TRACES_ENABLED (default: "true")
- OTEL_EXPORTER_OTLP_ENDPOINT (default: "http://localhost:4318/v1/traces")
- OTEL_SERVICE_NAME (defaults to the service_name argument below)

Known limitation: trace context (the `traceparent` header) is propagated
Python-service-to-Python-service and producer-to-Kafka-header, but the
Flink/Spark (JVM) streaming jobs in streaming-jobs/ do not yet read or
forward it, so a trace started in event-producer currently ends at the
Kafka write, and a new trace starts wherever a Python service reads that
data back out. Propagating context through the JVM jobs is tracked as
follow-up work in docs/ARCHITECTURE.md.
"""

import logging
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

_initialized = False


def tracing_enabled() -> bool:
    return os.getenv("OTEL_TRACES_ENABLED", "true").lower() == "true"


def init_tracing(service_name: str) -> trace.Tracer:
    """Idempotent. Safe to call more than once (e.g. in tests)."""
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
        except Exception as e:  # pragma: no cover - defensive, exporter init rarely fails
            logger.warning("Could not initialize OTLP span exporter (%s); tracing spans will be created but not exported.", e)
        trace.set_tracer_provider(provider)
        logger.info("OpenTelemetry tracing initialized (service=%s, endpoint=%s)", service_name, endpoint)
        _initialized = True

    return trace.get_tracer(service_name)


def instrument_fastapi_app(app, service_name: str):
    """Wire up automatic spans for every FastAPI route (including DB/Redis
    call durations captured as nested spans by the relevant auto-instrumentors,
    where installed)."""
    init_tracing(service_name)
    if not tracing_enabled():
        return
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)
