# Benchmarks

This document exists because the README used to state throughput and
latency numbers ("5,000+ events/sec", "p95 query-api latency < 150ms")
without anything in the repo that actually measured them. That's a common
failure mode for portfolio/reference projects: a plausible-sounding number
gets written once and never gets checked again. This page draws a hard
line between **what has actually been measured, on what hardware, and
when** versus **what is a target for the full pipeline that you should
measure yourself** before repeating it anywhere that matters (a resume, an
interview, a design doc).

## What's measured today: producer generation throughput

[`scripts/benchmark_producer.py`](../scripts/benchmark_producer.py) is a
pure-Python micro-benchmark of `EventGenerator.generate_event()` -> schema
validation -> `Event.to_json()` — the CPU-bound work event-producer does
per event, with **no Kafka connection and no network I/O**. It's not an
end-to-end number. It exists so at least one link in the "5,000+
events/sec" claim has something real behind it, and so regressions in the
generator itself (an accidentally-quadratic loop, a schema check that got
expensive) show up as a number that moved, not a vibe.

Latest run, checked into
[`loadtests/results/producer-benchmark.json`](../loadtests/results/producer-benchmark.json):

| Metric | Value |
|---|---|
| Events generated | 50,000 |
| Throughput | ~7,000 events/sec (single-threaded, generation + validation + serialization only) |
| Per-event latency (p50 / p95 / p99) | ~138µs / ~169µs / ~200µs |
| Hardware | 2 vCPU sandbox container (see `environment` field in the JSON for the exact spec at benchmark time) |

Re-run it yourself:

```bash
python scripts/benchmark_producer.py --events 50000 --out loadtests/results/producer-benchmark.json
```

**What this number does *not* tell you:** Kafka network/serialization
overhead, broker-side batching and compression behavior, backpressure
under a real `docker compose up` stack, or anything about the Flink
aggregation job, Redis, TimescaleDB, or query-api. Those are the
components the "5,000+ events/sec" and "p95 < 150ms" claims in the README
are actually about.

## What's a target, not yet measured: full-pipeline throughput and latency

The repo has always shipped the tooling to measure this
([`loadtests/k6-scripts/high-throughput-events.js`](../loadtests/k6-scripts/high-throughput-events.js)
and
[`loadtests/k6-scripts/api-load-test.js`](../loadtests/k6-scripts/api-load-test.js)),
and CI's `loadtest-validation` job confirms the scripts themselves are
valid k6 — but validating a script's syntax is not the same as running it
against a live stack and recording what came back. Nothing in this repo
ran that and checked in the result, which is exactly the gap this doc is
about being honest about.

To produce a real, attributable number:

```bash
docker compose -f infra/docker-compose/docker-compose.yml up -d
./scripts/check-system-status.sh   # wait for everything to report healthy

# End-to-end producer -> Kafka -> Flink -> Redis/TimescaleDB throughput:
k6 run loadtests/k6-scripts/high-throughput-events.js \
  --out json=loadtests/results/high-throughput-$(date +%Y%m%d).json

# query-api read-path latency (the p95 < 150ms target):
k6 run loadtests/k6-scripts/api-load-test.js \
  --out json=loadtests/results/api-load-$(date +%Y%m%d).json
```

Check the resulting JSON into `loadtests/results/` next to
`producer-benchmark.json` and update the table below (and the README's
"Impact" section) with the real numbers and the date/hardware they were
measured on. Until that happens, treat "5,000+ events/sec" and "p95 <
150ms" in the README as **design targets**, not measured facts — they're
labeled that way there now.

| Metric | Target | Measured | Date | Hardware |
|---|---|---|---|---|
| End-to-end ingest throughput | 5,000+ events/sec | _not yet measured_ | — | — |
| query-api `/kpi` p95 latency | < 150ms | _not yet measured_ | — | — |

## CI's `load-test-staging` job

The existing CI pipeline already runs `k6 run loadtests/k6-scripts/api-load-test.js`
against a real staging deployment on every push to `main`
(`.github/workflows/ci-cd.yml`, `load-test-staging` job) and uploads the
JSON as a build artifact. That's the closest thing to a continuously
verified number this project has — but it only runs after
`deploy-staging`, which requires real cluster credentials that aren't
configured in this repo's secrets, so it has never actually executed. It's
correctly wired for the day it is.
