# Data contracts

This directory holds the canonical JSON Schema definition for every message
shape that crosses a Kafka topic in this platform. They exist so a breaking
change to an event or alert shape is caught by a unit test in the producing
service, not discovered at 2am by whichever consumer chokes on the first
malformed message.

| Schema | Topic | Written by | Read by |
|---|---|---|---|
| [`event.v1.schema.json`](event.v1.schema.json) | `signal.events.v1` | event-producer | aggregation-job, anomaly-detection-job, session-analytics-job |
| [`alert.v1.schema.json`](alert.v1.schema.json) | `signal.alerts.v1` | anomaly-detection-job | notifier-service |

## How validation is wired in

- **event-producer** validates every generated event against
  `event.v1.schema.json` before publishing. A schema failure is a bug in the
  generator itself (it should never happen), so it's treated the same way a
  real producer would treat a serialization bug: the event is **not**
  dropped silently and it does **not** crash the process — it's routed to
  `signal.events.dlq` with the validation error attached, and
  `signal_events_dlq_total` increments so it's visible in Prometheus/Grafana.
- **notifier-service** validates every inbound alert against
  `alert.v1.schema.json` before constructing the Pydantic model. A message
  that fails validation (e.g. the anomaly-detection job gets redeployed with
  a field rename) is routed to `signal.alerts.dlq` instead of being dropped,
  and `notifier_alerts_dlq_total` increments.
- See [`../scripts/dlq_inspect.py`](../scripts/dlq_inspect.py) to inspect or
  replay messages sitting in a DLQ topic.

## Why the schema files are duplicated per service

Each service (`ingestion/event-producer`, `services/notifier-service`, ...)
builds from its own directory as an independent Docker build context — that's
what lets CI build and push each image without checking out the whole repo
into one layer. A single shared Python package importable across those
contexts would need either a private package index, a Bazel/pants-style
monorepo build, or a build context rooted at the repo top (which drags every
service's dependencies into every other service's image layer cache).

For a project this size, the simplest correct thing is to treat this
directory as the **source of truth for review and documentation**, and keep
a byte-identical copy next to each service that consumes it
(`services/notifier-service/schemas/alert.v1.schema.json`, etc.). A test in
`tests/unit/test_schema_validation.py` in each service asserts its local copy
is identical to the one here, so drift is caught in CI instead of silently
diverging. If this were a larger monorepo, the next step would be a proper
shared package (or a schema registry, e.g. Confluent Schema Registry with
Avro/Protobuf) — noted as follow-up work in `docs/ARCHITECTURE.md`.
