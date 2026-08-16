# Observability

Three pillars, wired into every Python service (event-producer, query-api,
notifier-service) the same way so the mental model is the same everywhere:
metrics tell you *something changed*, traces tell you *where*, logs tell
you *why*.

## Metrics (Prometheus)

Unchanged from before this pass — every service exposes `/metrics`
(or a dedicated metrics port for event-producer), scraped by the
`prometheus` container/pod on a 15s interval. See
`infra/docker-compose/prometheus/prometheus.yml` and the two provisioned
Grafana dashboards under `infra/docker-compose/grafana/dashboards/`.

New counters added alongside the data-contract work:

| Metric | Service | Meaning |
|---|---|---|
| `signal_events_dlq_total{source}` | event-producer | Events that failed schema validation and were routed to `signal.events.dlq` |
| `notifier_alerts_dlq_total{source}` | notifier-service | Alerts that failed JSON parsing / schema validation / Pydantic validation and were routed to `signal.alerts.dlq` |

## Traces (OpenTelemetry -> Jaeger)

Each service's `tracing.py` (`ingestion/event-producer/tracing.py`,
`services/query-api/tracing.py`, `services/notifier-service/tracing.py`)
sets up an OTLP/HTTP exporter pointed at Jaeger's built-in OTLP receiver.

**To look at a trace locally:**

```bash
docker compose -f infra/docker-compose/docker-compose.yml up -d
curl http://localhost:8000/kpi          # generates a query-api trace
open http://localhost:16686              # Jaeger UI -- search by service "query-api"
```

**Env vars** (same three across every service, see each `.env.example`):

| Var | Default | Meaning |
|---|---|---|
| `OTEL_TRACES_ENABLED` | `true` | Set `false` to fully disable (no exporter created, near-zero overhead) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | Where spans are sent. `http://jaeger:4318/v1/traces` in docker-compose |
| `OTEL_SERVICE_NAME` | per-service default | The name a trace's spans are grouped under in Jaeger |

**It's safe to run with no collector.** The OTLP exporter batches spans on
a background thread and drops a batch on export failure rather than
blocking request handling or raising — starting a service with
`OTEL_EXPORTER_OTLP_ENDPOINT` pointed at nothing just means spans are
created and thrown away, which is the same failure mode as a Prometheus
scrape target that's down.

**Sampling.** At 100+ events/sec, tracing every single `produce_event`
span is more volume than you want in a real deployment. The OpenTelemetry
SDK reads the standard `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`
env vars automatically (nothing in this repo overrides the sampler), so
e.g. `OTEL_TRACES_SAMPLER=traceidratio` + `OTEL_TRACES_SAMPLER_ARG=0.05`
samples 5% of traces without touching code.

**Known limitation:** the Flink/Spark (JVM) streaming jobs don't
propagate the `traceparent` header yet, so a trace currently ends at the
Kafka write from event-producer and a new one starts wherever a Python
service reads the resulting data back out. See `docs/ARCHITECTURE.md`
("Observability: metrics, traces, and logs") for what closing that gap
would take.

## Logs (structured, correlation-ID-aware)

Every service's `logging_config.py` implements the same pattern:

- A `correlation_id` contextvar, bound for the lifetime of one HTTP
  request (`CorrelationIdMiddleware`, FastAPI services) or one Kafka
  message being processed (notifier-service's consumer loop) or one event
  being generated (event-producer's `bind_correlation_id`).
- Every log line emitted during that window automatically includes it,
  via a `logging.Filter`.
- `X-Correlation-ID` is read from an inbound HTTP request if the caller
  sent one, otherwise generated, and always echoed back in the response
  header — so a request can be grepped end-to-end across services that
  call each other.
- The same ID travels in Kafka message headers (`correlation_id`) from
  event-producer, so `docker compose logs | grep <id>` finds the
  producer's log line and (once JVM propagation lands, see above) will
  find the consumer's too.

**Env vars:**

| Var | Default | Meaning |
|---|---|---|
| `LOG_FORMAT` | `text` | `text` for human-readable local dev logs; `json` for one-JSON-object-per-line output suited to Loki/CloudWatch/ELK |
| `LOG_LEVEL` | `INFO` | Standard Python logging level name |

Example `LOG_FORMAT=json` output:

```json
{"timestamp": "2026-08-16T17:32:10+0000", "level": "INFO", "service": "query-api", "logger": "main", "correlation_id": "3f1c9e2a-...", "message": "Connected to Redis at redis:6379"}
```
