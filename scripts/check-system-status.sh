#!/bin/bash
#
# Real-Time Signal Intelligence Platform — System Status Check
# Verifies that every component of the docker-compose stack is healthy.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

COMPOSE_FILE="infra/docker-compose/docker-compose.yml"
BASE_URL="http://localhost:8000"

print_status()  { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[OK]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error()   { echo -e "${RED}[FAIL]${NC} $1"; }

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" "$@"
    else
        docker-compose -f "$COMPOSE_FILE" "$@"
    fi
}

check_service() {
    local service_name=$1
    local status
    status=$(compose ps -q "$service_name" 2>/dev/null | xargs -r docker inspect -f '{{.State.Status}}' 2>/dev/null || echo "not found")
    if [ "$status" = "running" ]; then
        print_success "$service_name is running"
    else
        print_error "$service_name is not running (status: $status)"
        return 1
    fi
}

check_http_endpoint() {
    local name=$1 url=$2 expected=${3:-200}
    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    if [ "$response" = "$expected" ]; then
        print_success "$name is accessible ($url)"
    else
        print_error "$name is not accessible ($url) - HTTP $response"
        return 1
    fi
}

check_database() {
    local result
    result=$(compose exec -T timescaledb psql -U signalintel_admin -d signalintel -c "SELECT 1;" 2>/dev/null || echo "failed")
    [[ $result == *"1"* ]] && print_success "TimescaleDB is accessible" || print_error "TimescaleDB is not accessible"
}

check_redis() {
    local result
    result=$(compose exec -T redis redis-cli ping 2>/dev/null || echo "failed")
    [ "$result" = "PONG" ] && print_success "Redis is accessible" || print_error "Redis is not accessible"
}

check_kafka() {
    local result
    result=$(compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l)
    [ "$result" -gt 0 ] && print_success "Kafka is accessible and has topics" || print_error "Kafka is not accessible or has no topics"
}

check_flink() {
    local response
    response=$(curl -s "http://localhost:8081/overview" 2>/dev/null || echo "failed")
    [[ $response == *"slots"* ]] && print_success "Flink is accessible" || print_error "Flink is not accessible"
}

check_spark() {
    local response
    response=$(curl -s "http://localhost:8082/json" 2>/dev/null || echo "failed")
    [[ $response == *"status"* ]] && print_success "Spark master is accessible" || print_error "Spark master is not accessible"
}

test_api_functionality() {
    print_status "Testing query-api functionality..."
    check_http_endpoint "Health endpoint" "$BASE_URL/health"
    check_http_endpoint "KPI endpoint" "$BASE_URL/kpi"
    check_http_endpoint "Alerts endpoint" "$BASE_URL/alerts"
    check_http_endpoint "Sessions endpoint" "$BASE_URL/sessions"
    check_http_endpoint "Metrics endpoint" "$BASE_URL/metrics"
}

check_event_flow() {
    print_status "Checking event flow..."

    local kafka_consumer_output
    kafka_consumer_output=$(timeout 10s compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 --topic signal.events.v1 --from-beginning --max-messages 1 2>/dev/null || echo "no events")
    if [[ $kafka_consumer_output == *"event_id"* ]]; then
        print_success "Events are flowing through Kafka"
    else
        print_warning "No events detected in signal.events.v1 yet."
    fi

    local db_count
    db_count=$(compose exec -T timescaledb psql -U signalintel_admin -d signalintel -t -c "SELECT COUNT(*) FROM events_raw;" 2>/dev/null | tr -d ' \n' || echo "0")
    if [ "$db_count" -gt 0 ] 2>/dev/null; then
        print_success "Events are being stored in TimescaleDB ($db_count rows)"
    else
        print_warning "No events found in events_raw yet."
    fi

    local redis_keys
    redis_keys=$(compose exec -T redis redis-cli keys "sip:agg:*" 2>/dev/null | wc -l || echo "0")
    if [ "$redis_keys" -gt 0 ]; then
        print_success "Hot aggregates are present in Redis ($redis_keys keys)"
    else
        print_warning "No hot aggregates found in Redis yet."
    fi
}

run_all_checks() {
    print_status "Real-Time Signal Intelligence Platform — System Status Check"
    echo "=================================================="
    echo ""

    print_status "Infrastructure..."
    check_service "signal-zookeeper" || true
    check_service "signal-kafka" || true
    check_service "signal-redis" || true
    check_service "signal-timescaledb" || true
    check_service "signal-elasticsearch" || true
    echo ""

    print_status "Processing clusters..."
    check_service "signal-flink-jobmanager" || true
    check_service "signal-spark-master" || true
    echo ""

    print_status "Application services..."
    check_service "signal-query-api" || true
    check_service "signal-notifier-service" || true
    check_service "signal-live-dashboard" || true
    check_service "signal-event-producer" || true
    echo ""

    print_status "Monitoring..."
    check_service "signal-prometheus" || true
    check_service "signal-grafana" || true
    check_service "signal-alertmanager" || true
    check_service "signal-jaeger" || true
    echo ""

    print_status "Connectivity..."
    check_database || true
    check_redis || true
    check_kafka || true
    check_flink || true
    check_spark || true
    echo ""

    print_status "HTTP endpoints..."
    check_http_endpoint "Kafka UI" "http://localhost:8080" || true
    check_http_endpoint "Flink Dashboard" "http://localhost:8081" || true
    check_http_endpoint "Grafana" "http://localhost:3000" || true
    check_http_endpoint "Prometheus" "http://localhost:9090" || true
    check_http_endpoint "Jaeger" "http://localhost:16686" || true
    echo ""

    test_api_functionality
    echo ""
    check_event_flow
    echo ""

    print_success "System status check complete."
    print_status "Quick start: Grafana http://localhost:3000 (admin/admin), API docs http://localhost:8000/docs"
}

run_all_checks
