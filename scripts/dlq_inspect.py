#!/usr/bin/env python3
"""
DLQ inspection / replay CLI
============================

Inspects (and optionally replays) messages sitting in one of this
platform's dead-letter-queue topics (`signal.events.dlq`,
`signal.alerts.dlq`) -- see schemas/README.md for how messages end up
there. Messages are never dropped silently: this is the tool an on-call
engineer reaches for to see *why* something landed in the DLQ and, once
the root cause is fixed (a bad deploy, a schema drift), replay it back to
its original topic.

Examples:

    # Dump everything currently in the events DLQ, most recent first.
    python scripts/dlq_inspect.py --topic signal.events.dlq --from-beginning

    # Follow the alerts DLQ live, like `tail -f`.
    python scripts/dlq_inspect.py --topic signal.alerts.dlq --follow

    # Replay every message in the events DLQ back to signal.events.v1,
    # stripping the DLQ wrapper (error/failed_at/original_topic) so only
    # the original event body goes back out.
    python scripts/dlq_inspect.py --topic signal.events.dlq --from-beginning --replay
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


def build_consumer(bootstrap_servers: str, topic: str, from_beginning: bool) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        auto_offset_reset="earliest" if from_beginning else "latest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,  # stop iterating once caught up, unless --follow
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k,
    )


def print_dlq_message(index: int, key: bytes, value: bytes) -> dict:
    try:
        body = json.loads(value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"--- [{index}] key={key!r} (not JSON, {len(value)} bytes) ---")
        print(value[:500])
        return {}

    print(f"--- [{index}] key={key.decode('utf-8', errors='replace') if key else None} ---")
    print(f"  error:        {body.get('error')}")
    print(f"  failed_at:    {body.get('failed_at')}")
    print(f"  orig. topic:  {body.get('original_topic')}")
    inner = body.get("event") or body.get("raw_value")
    print(f"  payload:      {json.dumps(inner)[:300]}")
    return body


def main():
    parser = argparse.ArgumentParser(description="Inspect and optionally replay DLQ messages.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", required=True, choices=["signal.events.dlq", "signal.alerts.dlq"])
    parser.add_argument("--from-beginning", action="store_true", help="Read from the start of the topic")
    parser.add_argument("--follow", action="store_true", help="Keep consuming instead of stopping when caught up")
    parser.add_argument("--limit", type=int, default=100, help="Max messages to print/replay")
    parser.add_argument("--replay", action="store_true", help="Republish the original payload to its source topic")
    args = parser.parse_args()

    consumer = build_consumer(args.bootstrap_servers, args.topic, args.from_beginning)
    if args.follow:
        consumer.config["consumer_timeout_ms"] = float("inf")

    producer = None
    if args.replay:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","),
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    count = 0
    replayed = 0
    try:
        for message in consumer:
            if count >= args.limit:
                break
            body = print_dlq_message(count, message.key, message.value)
            count += 1

            if args.replay and body:
                target_topic = body.get("original_topic")
                inner = body.get("event")
                raw_value = body.get("raw_value")
                if target_topic and inner is not None:
                    producer.send(target_topic, key=message.key, value=json.dumps(inner))
                    replayed += 1
                elif target_topic and raw_value is not None:
                    producer.send(target_topic, key=message.key, value=raw_value)
                    replayed += 1
                else:
                    print("  (skipped replay: no original_topic/event/raw_value in DLQ envelope)")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if producer:
            producer.flush()
            producer.close()

    print(f"\n{count} message(s) inspected" + (f", {replayed} replayed at {datetime.now(timezone.utc).isoformat()}" if args.replay else "") + ".")
    return 0


if __name__ == "__main__":
    sys.exit(main())
