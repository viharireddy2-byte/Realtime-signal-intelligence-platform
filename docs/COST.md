# Cost & Capacity Planning

## Summary

Every component in this platform is open source with no licensing cost. Running expenses are entirely infrastructure (compute/storage/network) and the operational time to run it. Numbers below are rough AWS-list-price estimates for planning purposes — get real quotes before budgeting against them, and remember reserved/spot pricing can cut compute costs 30-60%.

## Local development

Free, using your own hardware. Recommended: 4+ cores, 16+ GB RAM, 20+ GB free disk (the full docker-compose stack — Kafka, Redis, TimescaleDB, Elasticsearch, Flink job+task manager, Spark master+worker, Prometheus, Grafana, and four application services — is comfortably runnable on a modern laptop, though tight on 8 GB machines).

## Cloud deployment (illustrative, AWS-flavored)

### Small (≤10,000 events/sec)

| Component | Sizing | Est. monthly |
|---|---|---|
| Kubernetes cluster | 3× t3.medium | $120 |
| Kafka brokers | 3× t3.large | $240 |
| Flink cluster | 2× t3.large | $160 |
| Spark cluster | 2× t3.large | $160 |
| TimescaleDB | db.t3.medium | $80 |
| Redis | cache.t3.micro | $20 |
| Load balancer | ALB | $25 |
| Storage (EBS) | 1TB gp3 | $80 |
| Data transfer | 1TB | $90 |
| Monitoring | CloudWatch or self-hosted Prometheus/Grafana | $50 |
| **Total** | | **~$1,025/month** |

### Medium (≤50,000 events/sec)

| Component | Sizing | Est. monthly |
|---|---|---|
| Kubernetes cluster | 3× t3.large | $240 |
| Kafka brokers | 3× t3.xlarge | $480 |
| Flink cluster | 4× t3.xlarge | $640 |
| Spark cluster | 3× t3.xlarge | $480 |
| TimescaleDB | db.r5.large | $180 |
| Redis cluster | 3× cache.t3.small | $90 |
| Load balancer | ALB | $25 |
| Storage (EBS) | 2TB gp3 | $160 |
| Data transfer | 5TB | $450 |
| Monitoring | CloudWatch or self-hosted | $100 |
| **Total** | | **~$2,845/month** |

### Large (≤200,000 events/sec)

Scale each tier proportionally — expect $6,500-$7,500/month before reserved-instance discounts. At this volume, prioritize the optimizations below over raw instance count.

## Cost optimization

### Right-size Kubernetes requests/limits

```yaml
resources:
  requests: { cpu: "500m", memory: "1Gi" }
  limits: { cpu: "2000m", memory: "4Gi" }
```

### Autoscale query-api (already wired in the Helm chart)

`infra/helm/signal-intel-platform/templates/query-api-deployment.yaml` includes a `HorizontalPodAutoscaler` (2-10 replicas, 70% CPU / 80% memory targets) whenever `autoscaling.enabled: true` — the default.

### Data lifecycle (already applied)

`infra/docker-compose/init-scripts/01-init-timescaledb.sql` enables retention (30 days on `events_raw`, 90 days on `metrics_1min`) and compression (7-day-old `events_raw` rows, segmented by `source`) out of the box — this is not a "consider adding" item, it's live in the schema this repo ships. Tune the intervals in that file for your actual retention requirements.

### Kafka topic configuration

```properties
retention.ms=604800000   # 7 days
compression.type=snappy
```

Already set via `compressionType: snappy` in the Helm `kafka.config` values and the producer's `compression_type='snappy'`.

### Reserved / spot instances

- Reserved instances (1-year): 30-40% compute savings, 40-60% database savings
- Spot instances for Flink/Spark task managers and workers (stateless-ish, checkpointed workloads tolerate interruption reasonably well): significant additional savings, at the cost of occasional task manager churn

## Operational cost (people, not infra)

Running this well — not just standing it up — is a part-time-to-full-time job depending on scale: expect 0.3-0.5 FTE of platform/DevOps time for a small-to-medium deployment (on-call rotation, capacity planning, upgrades, incident response). Budget for that alongside infrastructure; it's typically the larger line item at real scale.

## Capacity planning rule of thumb

Kafka partition count, Flink parallelism, and TimescaleDB chunk sizing all need to scale together — adding compute without adding Kafka partitions (and matching consumer parallelism) just means idle consumers. Start with `numPartitions: 6` on `signal.events.v1` (the Helm default) and only increase it alongside a real measured throughput ceiling, not preemptively.
