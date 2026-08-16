#!/bin/bash
#
# Real-Time Signal Intelligence Platform — Local Development Setup
# Brings up the full docker-compose stack, builds the streaming-job JARs,
# creates Kafka topics, and starts event generation.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

COMPOSE_FILE="infra/docker-compose/docker-compose.yml"

print_status()  { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

command_exists() { command -v "$1" >/dev/null 2>&1; }

check_prerequisites() {
    print_status "Checking prerequisites..."
    local missing=()

    command_exists docker || missing+=("docker")
    command_exists python3 || missing+=("python3")
    command_exists pip || missing+=("pip")

    if ! docker compose version >/dev/null 2>&1 && ! command_exists docker-compose; then
        missing+=("docker compose")
    fi

    if ! command_exists mvn; then
        print_warning "Maven not found — required to build the Flink/Spark job JARs."
        missing+=("maven")
    fi

    if ! command_exists java; then
        print_warning "Java not found — required to build the Flink/Spark job JARs."
        missing+=("java")
    fi

    if [ ${#missing[@]} -ne 0 ]; then
        print_error "Missing dependencies: ${missing[*]}"
        exit 1
    fi
    print_success "All prerequisites are satisfied!"
}

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" "$@"
    else
        docker-compose -f "$COMPOSE_FILE" "$@"
    fi
}

check_docker() {
    print_status "Checking Docker daemon..."
    docker info >/dev/null 2>&1 || { print_error "Docker daemon is not running."; exit 1; }
    print_success "Docker daemon is running!"
}

build_streaming_jobs() {
    print_status "Building streaming-job JARs..."
    for job in aggregation-job anomaly-detection-job session-analytics-job; do
        if [ -d "streaming-jobs/$job" ]; then
            print_status "Building $job..."
            (cd "streaming-jobs/$job" && mvn -q clean package -DskipTests)
            print_success "$job built."
        fi
    done
}

start_infrastructure() {
    print_status "Starting infrastructure services..."
    compose up -d zookeeper kafka kafka-exporter redis redis-exporter timescaledb postgres-exporter elasticsearch node-exporter

    print_status "Waiting for Kafka to be ready..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    [ $timeout -le 0 ] && { print_error "Kafka failed to start within timeout."; exit 1; }
    print_success "Core infrastructure is ready!"
}

create_kafka_topics() {
    print_status "Creating Kafka topics..."
    topics=(
        "signal.events.v1:6:1"
        "signal.alerts.v1:3:1"
        "signal.aggregates.hot:3:1"
        "signal.aggregates.cold:3:1"
        "signal.anomalies.cold:3:1"
    )
    for topic_config in "${topics[@]}"; do
        IFS=':' read -r topic partitions replication <<< "$topic_config"
        print_status "Creating topic: $topic (partitions: $partitions, replication: $replication)"
        compose exec -T kafka kafka-topics \
            --bootstrap-server localhost:9092 --create \
            --topic "$topic" --partitions "$partitions" --replication-factor "$replication" \
            --if-not-exists
    done
    print_success "Kafka topics created!"
}

start_processing_services() {
    print_status "Starting Flink and Spark clusters..."
    compose up -d flink-jobmanager flink-taskmanager spark-master spark-worker
    print_status "Waiting for processing clusters to be ready..."
    sleep 20
    print_success "Processing services started!"
}

start_monitoring() {
    print_status "Starting monitoring stack..."
    compose up -d prometheus grafana alertmanager
    sleep 10
    print_success "Monitoring services started!"
}

start_application_services() {
    print_status "Starting application services..."
    compose up -d --build query-api notifier-service live-dashboard event-producer
    sleep 10
    print_success "Application services started!"
}

start_ui_services() {
    print_status "Starting UI services..."
    compose up -d kafka-ui
    print_success "UI services started!"
}

display_urls() {
    print_success "Setup complete! Services are available at:"
    echo ""
    echo -e "${BLUE}Core Services:${NC}"
    echo "  - Kafka UI:            http://localhost:8080"
    echo "  - Flink Dashboard:     http://localhost:8081"
    echo "  - Spark Master UI:     http://localhost:8082"
    echo ""
    echo -e "${BLUE}Applications:${NC}"
    echo "  - Query API:           http://localhost:8000"
    echo "  - Query API docs:      http://localhost:8000/docs"
    echo "  - Notifier Service:    http://localhost:8001"
    echo "  - Live Dashboard:      http://localhost:8003"
    echo ""
    echo -e "${BLUE}Monitoring:${NC}"
    echo "  - Grafana:             http://localhost:3000 (admin/admin)"
    echo "  - Prometheus:          http://localhost:9090"
    echo "  - Alertmanager:        http://localhost:9093"
    echo ""
    echo -e "${BLUE}Databases:${NC}"
    echo "  - TimescaleDB:         localhost:5432 (signalintel_admin/password)"
    echo "  - Redis:               localhost:6379"
    echo "  - Elasticsearch:       http://localhost:9200"
    echo ""
}

main() {
    print_status "Starting Real-Time Signal Intelligence Platform setup..."
    echo ""

    check_prerequisites
    check_docker
    build_streaming_jobs
    start_infrastructure
    create_kafka_topics
    start_processing_services
    start_monitoring
    start_application_services
    start_ui_services

    print_status "Submitting streaming jobs..."
    ./scripts/submit-jobs.sh || print_warning "Job submission failed — see streaming-jobs/README.md to submit manually."

    display_urls

    echo ""
    print_status "To check system health, run: ./scripts/check-system-status.sh"
    print_status "To run load tests, run:      ./scripts/run-load-tests.sh"
    print_status "To stop everything, run:     docker compose -f $COMPOSE_FILE down"
    echo ""
    print_success "Setup completed successfully!"
}

main "$@"
