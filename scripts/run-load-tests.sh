#!/bin/bash
#
# Load Testing Script for the Real-Time Signal Intelligence Platform
# Runs the k6 suites under loadtests/k6-scripts against a running stack.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

BASE_URL=${BASE_URL:-"http://localhost:8000"}
OUTPUT_DIR="loadtest-results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

print_status()  { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

check_k6() {
    if ! command -v k6 >/dev/null 2>&1; then
        print_error "k6 is not installed."
        print_status "  macOS:  brew install k6"
        print_status "  Ubuntu: sudo apt install k6"
        print_status "  Windows: choco install k6"
        exit 1
    fi
    print_success "k6 is installed!"
}

check_services() {
    print_status "Checking service health..."
    if curl -s "$BASE_URL/health" >/dev/null; then
        print_success "Query API is healthy"
    else
        print_error "Query API is not accessible at $BASE_URL"
        exit 1
    fi

    print_status "Checking whether the event pipeline has data..."
    sleep 5
    if curl -s "$BASE_URL/kpi" | grep -q '\[\]' || curl -s "$BASE_URL/kpi" | grep -q 'source'; then
        print_success "Event pipeline is responding!"
    else
        print_warning "No KPI data detected yet — start the event producer first if this is a cold stack."
    fi
}

setup_output_dir() {
    mkdir -p "$OUTPUT_DIR"
    print_status "Results will be saved to: $OUTPUT_DIR"
}

run_api_load_test() {
    print_status "Running API load test..."
    local output_file="$OUTPUT_DIR/api-load-test_${TIMESTAMP}"
    k6 run --env BASE_URL="$BASE_URL" \
        --out json="$output_file.json" \
        loadtests/k6-scripts/api-load-test.js | tee "$output_file.log"
    print_success "API load test completed! Results: $output_file.*"
}

run_event_throughput_test() {
    print_status "Running high-throughput event generation simulation..."
    local output_file="$OUTPUT_DIR/high-throughput-events_${TIMESTAMP}"
    k6 run --out json="$output_file.json" \
        loadtests/k6-scripts/high-throughput-events.js | tee "$output_file.log"
    print_success "High-throughput event test completed! Results: $output_file.*"
}

run_spike_test() {
    print_status "Running spike test..."
    local output_file="$OUTPUT_DIR/spike-test_${TIMESTAMP}"

    cat > "/tmp/signal-spike-test.js" << 'EOF'
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '1m', target: 10 },
    { duration: '30s', target: 100 },
    { duration: '1m', target: 100 },
    { duration: '30s', target: 10 },
    { duration: '1m', target: 10 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.1'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export default function () {
  const responses = http.batch([
    ['GET', `${BASE_URL}/health`],
    ['GET', `${BASE_URL}/kpi?window=1m`],
    ['GET', `${BASE_URL}/alerts`],
  ]);

  check(responses[0], { 'health status is 200': (r) => r.status === 200 });
  check(responses[1], { 'kpi status is 200': (r) => r.status === 200 });
  check(responses[2], { 'alerts status is 200': (r) => r.status === 200 });

  sleep(Math.random() * 2 + 1);
}
EOF

    k6 run --env BASE_URL="$BASE_URL" --out json="$output_file.json" /tmp/signal-spike-test.js | tee "$output_file.log"
    rm /tmp/signal-spike-test.js
    print_success "Spike test completed! Results: $output_file.*"
}

run_soak_test() {
    print_status "Running soak test (30 minutes)..."
    print_warning "This test takes 30 minutes. Ctrl+C to cancel."
    sleep 5

    local output_file="$OUTPUT_DIR/soak-test_${TIMESTAMP}"
    cat > "/tmp/signal-soak-test.js" << 'EOF'
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '5m', target: 20 },
    { duration: '20m', target: 20 },
    { duration: '5m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'],
    http_req_failed: ['rate<0.02'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export default function () {
  const endpoint = Math.random() < 0.5 ? '/kpi?window=1m' : '/alerts';
  const response = http.get(`${BASE_URL}${endpoint}`);
  check(response, {
    'status is 200': (r) => r.status === 200,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });
  sleep(2);
}
EOF

    k6 run --env BASE_URL="$BASE_URL" --out json="$output_file.json" /tmp/signal-soak-test.js | tee "$output_file.log"
    rm /tmp/signal-soak-test.js
    print_success "Soak test completed! Results: $output_file.*"
}

show_usage() {
    echo "Usage: $0 [OPTIONS] [TEST_TYPE]"
    echo ""
    echo "Options:"
    echo "  -u, --url URL   Base URL for API tests (default: http://localhost:8000)"
    echo "  -h, --help      Show this help message"
    echo ""
    echo "Test Types: api | events | spike | soak | all (default)"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--url) BASE_URL="$2"; shift 2 ;;
        -h|--help) show_usage; exit 0 ;;
        *) TEST_TYPE="$1"; shift ;;
    esac
done

TEST_TYPE=${TEST_TYPE:-"all"}

main() {
    print_status "Starting load testing for Real-Time Signal Intelligence Platform"
    print_status "Target URL: $BASE_URL"
    print_status "Test Type: $TEST_TYPE"
    echo ""

    check_k6
    check_services
    setup_output_dir

    case $TEST_TYPE in
        api) run_api_load_test ;;
        events) run_event_throughput_test ;;
        spike) run_spike_test ;;
        soak) run_soak_test ;;
        all)
            run_api_load_test
            echo ""
            run_event_throughput_test
            echo ""
            run_spike_test
            echo ""
            print_status "Run the 30-minute soak test too? (y/N)"
            read -r response
            [[ $response =~ ^[Yy]$ ]] && run_soak_test || print_status "Skipping soak test"
            ;;
        *) print_error "Unknown test type: $TEST_TYPE"; show_usage; exit 1 ;;
    esac

    print_success "Load testing completed! Results in: $OUTPUT_DIR"
}

main "$@"
