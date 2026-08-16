#!/bin/bash
#
# Submits the built streaming-job JARs to the running Flink and Spark
# clusters. Run after setup-local-dev.sh (or after rebuilding a job) with
# the docker-compose stack already up.

set -e

COMPOSE_FILE="infra/docker-compose/docker-compose.yml"

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" "$@"
    else
        docker-compose -f "$COMPOSE_FILE" "$@"
    fi
}

echo "Submitting signal-aggregation-job to Flink..."
compose exec -T flink-jobmanager flink run -d \
    /opt/flink/usrlib/aggregation-job/signal-aggregation-job-2.0.0.jar

echo "Submitting signal-anomaly-detection-job to Flink..."
compose exec -T flink-jobmanager flink run -d \
    /opt/flink/usrlib/anomaly-detection-job/signal-anomaly-detection-job-2.0.0.jar

echo "Submitting signal-session-analytics-job to Spark..."
compose exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --class com.signalintel.platform.sessions.SessionAnalyticsJob \
    /opt/spark-apps/signal-session-analytics-job-2.0.0.jar

echo "All jobs submitted. Check http://localhost:8081 (Flink) and http://localhost:8082 (Spark) for status."
