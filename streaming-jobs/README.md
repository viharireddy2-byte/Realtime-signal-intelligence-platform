# Streaming Jobs

| Job | Engine | Reads | Writes | Purpose |
|-----|--------|-------|--------|---------|
| [`aggregation-job`](./aggregation-job) | Apache Flink (DataStream API) | `signal.events.v1` | `signal.aggregates.hot`, `signal.aggregates.cold` | 1-minute sliding-window KPIs (count, avg, p95, p99, error rate) per source |
| [`anomaly-detection-job`](./anomaly-detection-job) | Apache Flink (DataStream API, keyed state) | `signal.events.v1` | `signal.alerts.v1`, `signal.anomalies.cold` | Per-source anomaly scoring using rolling z-score, MAD, and EWMA detectors |
| [`session-analytics-job`](./session-analytics-job) | Apache Spark (Structured Streaming) | `signal.events.v1` | `sessions` table (direct JDBC upsert) | Session-windows raw events into user sessions and funnel progress |

## Building

Each job is an independent Maven module producing a fat/shaded JAR:

```bash
cd streaming-jobs/aggregation-job && mvn clean package -DskipTests
cd streaming-jobs/anomaly-detection-job && mvn clean package -DskipTests
cd streaming-jobs/session-analytics-job && mvn clean package -DskipTests
```

## Submitting

Against the local docker-compose stack:

```bash
# Flink jobs
docker compose -f infra/docker-compose/docker-compose.yml exec flink-jobmanager \
  flink run -d /opt/flink/usrlib/signal-aggregation-job-2.0.0.jar

docker compose -f infra/docker-compose/docker-compose.yml exec flink-jobmanager \
  flink run -d /opt/flink/usrlib/signal-anomaly-detection-job-2.0.0.jar

# Spark job
docker compose -f infra/docker-compose/docker-compose.yml exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  --class com.signalintel.platform.sessions.SessionAnalyticsJob \
  /opt/spark-apps/signal-session-analytics-job-2.0.0.jar
```

`scripts/submit-jobs.sh` automates all three submissions after
`scripts/setup-local-dev.sh` has brought the stack up.

## Why three engines' worth of jobs live in one directory

The aggregation and anomaly jobs are both low-latency, per-event stream
processors, which is exactly what Flink's DataStream API and keyed state are
built for. Session reconstruction is a fundamentally different shape of
problem — grouping many events into few, longer-lived sessions with a gap
based close condition — which maps directly onto Spark Structured
Streaming's built-in `session_window`. Using the right engine for each
workload (rather than forcing everything through one) is intentional, not
incidental — see [`../docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) for the
full reasoning.
