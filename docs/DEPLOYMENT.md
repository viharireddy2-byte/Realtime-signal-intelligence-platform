# Deployment

## 1. Local development (Docker Compose)

The fastest path:

```bash
./scripts/setup-local-dev.sh
```

Or step by step:

```bash
# 1. Build the streaming-job JARs
cd streaming-jobs/aggregation-job && mvn clean package -DskipTests && cd ../..
cd streaming-jobs/anomaly-detection-job && mvn clean package -DskipTests && cd ../..
cd streaming-jobs/session-analytics-job && mvn clean package -DskipTests && cd ../..

# 2. Bring up infrastructure
cd infra/docker-compose
docker compose up -d zookeeper kafka kafka-exporter redis redis-exporter timescaledb postgres-exporter elasticsearch node-exporter

# 3. Create Kafka topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic signal.events.v1 --partitions 6 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic signal.alerts.v1 --partitions 3 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic signal.aggregates.hot --partitions 3 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic signal.aggregates.cold --partitions 3 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic signal.anomalies.cold --partitions 3 --replication-factor 1

# 4. Bring up processing clusters, monitoring, and applications
docker compose up -d flink-jobmanager flink-taskmanager spark-master spark-worker
docker compose up -d prometheus grafana alertmanager
docker compose up -d --build query-api notifier-service live-dashboard event-producer kafka-ui

# 5. Submit the streaming jobs
cd ../..
./scripts/submit-jobs.sh
```

Verify everything is healthy: `./scripts/check-system-status.sh`

Tear down: `docker compose -f infra/docker-compose/docker-compose.yml down` (add `-v` to also drop volumes/data).

## 2. Kubernetes (Helm)

```bash
helm dependency update infra/helm/signal-intel-platform

helm upgrade --install signal-intel infra/helm/signal-intel-platform \
  --namespace signal-intel --create-namespace \
  --set services.queryApi.env.API_KEY_REQUIRED=true \
  --set secrets.apiKey.value="$(openssl rand -hex 32)" \
  --set postgresql.auth.password="$(openssl rand -hex 24)" \
  --set secrets.postgresql.password="$(openssl rand -hex 24)"
```

**Never deploy with the default `values.yaml` credentials outside a local/dev cluster.** Pull real secrets from your secret manager (Vault, AWS/GCP/Azure Secrets Manager, Sealed Secrets, External Secrets Operator) into a `values-production.yaml` or `--set-file`/`--set` at install time — see [`SECURITY.md`](SECURITY.md).

The chart provisions:

- Kafka, Redis, PostgreSQL/TimescaleDB via the Bitnami subcharts (toggle with `kafka.enabled` / `redis.enabled` / `postgresql.enabled`; point at externally managed instances instead by setting `enabled: false` and updating `config.*`)
- `query-api` (Deployment + Service + HorizontalPodAutoscaler + Ingress), `notifier-service`, `live-dashboard`, `event-producer` (Deployment + Service each)
- Prometheus + Grafana via the community subcharts

The chart does **not** auto-submit the Flink/Spark job JARs — job JARs are built and versioned independently of the Helm release (see `streaming-jobs/README.md`). Submit them after the Flink/Spark clusters are up:

```bash
kubectl cp streaming-jobs/aggregation-job/target/signal-aggregation-job-2.0.0.jar \
  <flink-jobmanager-pod>:/tmp/ -n signal-intel
kubectl exec <flink-jobmanager-pod> -n signal-intel -- flink run -d /tmp/signal-aggregation-job-2.0.0.jar
# repeat for anomaly-detection-job, and spark-submit for session-analytics-job
```

## 3. CI/CD

`.github/workflows/ci-cd.yml` runs on every push/PR:

1. **python-quality** — lint (flake8), format check (black), unit tests (pytest) for each Python service
2. **jvm-quality** — `mvn test` + `mvn package` for each streaming job, JARs uploaded as artifacts
3. **loadtest-validation** — sanity-runs the k6 scripts with 1 VU / 1 iteration
4. **build-images** — builds and pushes Docker images to GHCR (push events only)
5. **integration-tests** — spins up Postgres + Redis service containers, initializes the schema, runs `tests/integration` for query-api and notifier-service
6. **security-scan** — Trivy filesystem scan, results uploaded to the GitHub Security tab
7. **deploy-staging → load-test-staging → deploy-production** — gated on `main`, with a manual-approval `production` GitHub environment. The actual `helm upgrade` commands are commented placeholders — wire in your cluster credentials (`kubectl` context / `KUBECONFIG` secret) before enabling them for real.
8. **cleanup** — prunes old GHCR image versions

## Environment variables reference

| Service | Variable | Default | Notes |
|---|---|---|---|
| event-producer | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| query-api | `REDIS_HOST`, `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | see `config.py` | |
| query-api | `API_KEY_REQUIRED`, `API_KEY` | `false`, empty | see `SECURITY.md` |
| query-api | `RATE_LIMIT` | `600/minute` | slowapi default limit |
| query-api | `LIVE_FEED_INTERVAL_SECONDS` | `2.0` | WebSocket push cadence |
| notifier-service | `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_*` | see `config.py` | |
| notifier-service | `EMAIL_ENABLED`, `SMTP_*`, `EMAIL_RECIPIENTS` | disabled | |
| notifier-service | `SLACK_ENABLED`, `SLACK_WEBHOOK_URL` | disabled | |
| notifier-service | `WEBHOOK_ENABLED`, `CUSTOM_WEBHOOKS` | disabled | |
| live-dashboard | `QUERY_API_WS_URL`, `QUERY_API_HTTP_URL` | `ws://localhost:8000/ws/live`, `http://localhost:8000` | |
| aggregation-job / anomaly-detection-job | `KAFKA_BOOTSTRAP_SERVERS`, `EVENTS_TOPIC`, `JOB_PARALLELISM` | `kafka:29092`, `signal.events.v1`, `2` | |
| session-analytics-job | `KAFKA_BOOTSTRAP_SERVERS`, `SESSION_GAP`, `WATERMARK_DELAY`, `POSTGRES_*` | see job source | |
