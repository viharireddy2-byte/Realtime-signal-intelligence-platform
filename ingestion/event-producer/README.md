# Event Producer

Synthetic telemetry generator that publishes JSON events to the
`signal.events.v1` Kafka topic. See [`event_producer.py`](./event_producer.py).

## Run locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python event_producer.py --bootstrap-servers localhost:9092 --rate 200
```

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap-servers` | `localhost:9092` | Comma-separated Kafka brokers |
| `--topic` | `signal.events.v1` | Destination topic |
| `--rate` | `100` | Target events/sec |
| `--duration` | unset (infinite) | Stop after N seconds |
| `--sources` | all sources | Restrict to specific event sources |
| `--metrics-port` | `8002` | Prometheus `/metrics` port |

See [`../../docs/DATABASE.md`](../../docs/DATABASE.md) for the event schema.
