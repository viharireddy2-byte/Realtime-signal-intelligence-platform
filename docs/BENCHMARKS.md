# Benchmarks

## Controlled thirty-minute execution

The project owner's execution results are recorded below and in
[`loadtests/results/controlled-run-summary.json`](../loadtests/results/controlled-run-summary.json).

| Metric | Result |
|---|---|
| Duration | 30 minutes (1,800 seconds) |
| Events processed | 12,600,000 |
| Average throughput | 7,000 events/second |
| Flagged-event latency to Kafka alert topic (`signal.alerts.v1`), p95 | 450 ms |
| Snappy batch payload size reduction | 50% compared with the same uncompressed workload |

### Measurement scope

- Throughput is the average over the thirty-minute run: `12,600,000 / 1,800 = 7,000 events/second`.
- The 450 ms p95 result applies to flagged events reaching the Kafka alert topic. It does not measure query API responses, notifier delivery, or storage persistence.
- The compression result compares batch payload sizes for the same workload with and without Snappy. It does not imply a 50% reduction in total network traffic, storage usage, or cost.
- These results describe this controlled run, rather than a maximum capacity guarantee.

The supplied run summary does not include hardware specifications, a run timestamp, the exact invocation, raw latency samples, byte counts, or the latency start-point definition. Preserve those details with future run artifacts so comparisons use the same conditions. The summary records the supplied results without assigning the producer micro-benchmark's hardware to this run.

## What's measured today: producer generation throughput

[`scripts/benchmark_producer.py`](../scripts/benchmark_producer.py) is a
pure-Python micro-benchmark of `EventGenerator.generate_event()` -> schema
validation -> `Event.to_json()` — the CPU-bound work event-producer does
per event, with **no Kafka connection and no network I/O**. It's not an
end-to-end number. It provides an isolated baseline for detecting regressions in event generation.

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
aggregation job, Redis, TimescaleDB, or query-api. Measure these components separately from the producer baseline.

## Capturing future runs

Store run artifacts under `loadtests/results/`, including the exact command, environment, duration, processed-event counter deltas, latency samples and timestamp boundaries, and compressed/uncompressed byte counts for identical batches. Calculate throughput from processed events divided by elapsed seconds, and payload reduction as `100 * (1 - compressed_bytes / uncompressed_bytes)`.

For the separate query API read path, the existing k6 script can capture results against a running stack:

```bash
k6 run loadtests/k6-scripts/api-load-test.js --out json=loadtests/results/api-load.json
```

The query API p95 target is under 150 ms; the controlled-run alert-topic latency does not establish this metric.

