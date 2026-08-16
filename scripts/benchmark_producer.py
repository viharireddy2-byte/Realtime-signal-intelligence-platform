#!/usr/bin/env python3
"""
Producer micro-benchmark
=========================

Measures how fast this machine can run the actual code path that matters
for event-producer's advertised throughput: EventGenerator.generate_event()
-> schema validation -> Event.to_json(). Deliberately does **not** open a
Kafka connection -- this is a CPU-bound micro-benchmark of the producer's
own logic, not an end-to-end throughput number.

Why this exists: README.md used to assert "5,000+ events/sec" and "p95
query-api latency < 150ms" without anything in the repo that measured
either number. Those are still reasonable *targets* for the full pipeline,
but they were previously undistinguishable from a made-up number. This
script gives one piece of that claim something real and reproducible
behind it; see docs/BENCHMARKS.md for what this does and doesn't cover,
and for how to produce the full end-to-end numbers (event-producer -> Kafka
-> Flink -> Redis -> query-api) with the existing k6 scripts against a
running docker-compose or Helm deployment.

Usage:
    python scripts/benchmark_producer.py --events 50000
    python scripts/benchmark_producer.py --events 50000 --out loadtests/results/producer-benchmark.json
"""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion", "event-producer"))

# Schema validation is on by default in the real producer; keep it on here
# so the benchmark reflects what actually ships, not a stripped-down path.
os.environ.setdefault("SCHEMA_VALIDATION_ENABLED", "true")
os.environ.setdefault("OTEL_TRACES_ENABLED", "false")  # no collector in this benchmark run

from event_producer import EventGenerator, plan_publish  # noqa: E402


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=os.path.dirname(__file__), stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def percentile(sorted_values, pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * pct))
    return sorted_values[idx]


def main():
    parser = argparse.ArgumentParser(description="Micro-benchmark EventGenerator + schema validation + serialization.")
    parser.add_argument("--events", type=int, default=50000)
    parser.add_argument("--out", default=None, help="Write JSON results here in addition to stdout")
    args = parser.parse_args()

    generator = EventGenerator()
    sources = EventGenerator.SOURCES

    per_event_seconds = []
    start = time.perf_counter()
    for i in range(args.events):
        event_start = time.perf_counter()
        event = generator.generate_event(source=sources[i % len(sources)])
        plan = plan_publish(event, topic="signal.events.v1", dlq_topic="signal.events.dlq")
        assert plan.is_valid, "benchmark generated an invalid event -- this indicates a real bug, not a benchmark artifact"
        per_event_seconds.append(time.perf_counter() - event_start)
    total_seconds = time.perf_counter() - start

    per_event_seconds.sort()
    events_per_second = args.events / total_seconds

    results = {
        "benchmark": "producer-generation-and-serialization",
        "scope": "EventGenerator.generate_event() + schema validation + Event.to_json(). "
                 "No Kafka connection, no network I/O. NOT an end-to-end throughput number -- "
                 "see docs/BENCHMARKS.md for how to measure the full pipeline with loadtests/k6-scripts/.",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_commit(),
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
        "events_generated": args.events,
        "total_seconds": round(total_seconds, 4),
        "events_per_second": round(events_per_second, 1),
        "per_event_latency_microseconds": {
            "p50": round(percentile(per_event_seconds, 0.50) * 1_000_000, 2),
            "p95": round(percentile(per_event_seconds, 0.95) * 1_000_000, 2),
            "p99": round(percentile(per_event_seconds, 0.99) * 1_000_000, 2),
            "mean": round(statistics.mean(per_event_seconds) * 1_000_000, 2),
        },
    }

    print(json.dumps(results, indent=2))

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2)
            f.write("\n")
        print(f"\nWrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
