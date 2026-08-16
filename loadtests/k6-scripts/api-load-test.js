import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Custom metrics
export let errorRate = new Rate('errors');
export let latency = new Trend('latency');
export let requestCount = new Counter('requests');

// Test configuration
export let options = {
  stages: [
    { duration: '2m', target: 10 },
    { duration: '5m', target: 50 },
    { duration: '10m', target: 100 },
    { duration: '10m', target: 100 },
    { duration: '5m', target: 200 },
    { duration: '5m', target: 200 },
    { duration: '5m', target: 50 },
    { duration: '2m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<150'],
    http_req_failed: ['rate<0.05'],
    errors: ['rate<0.05'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const API_ENDPOINTS = {
  health: `${BASE_URL}/health`,
  kpi: `${BASE_URL}/kpi`,
  series: `${BASE_URL}/series`,
  alerts: `${BASE_URL}/alerts`,
  sessions: `${BASE_URL}/sessions`,
};

const SOURCES = ['web', 'mobile', 'api', 'iot-device', 'service-checkout', 'service-search'];
const WINDOWS = ['1m', '5m', '15m', '1h'];
const AGGREGATIONS = ['avg', 'sum', 'count', 'p95'];

function randomChoice(array) {
  return array[Math.floor(Math.random() * array.length)];
}

function getRandomTimestamp(hoursBack = 24) {
  const now = new Date();
  const past = new Date(now.getTime() - hoursBack * 60 * 60 * 1000);
  const randomTime = new Date(past.getTime() + Math.random() * (now.getTime() - past.getTime()));
  return randomTime.toISOString();
}

export default function () {
  const choice = Math.random();

  if (choice < 0.35) {
    testKPIEndpoint();
  } else if (choice < 0.6) {
    testSeriesEndpoint();
  } else if (choice < 0.8) {
    testAlertsEndpoint();
  } else if (choice < 0.92) {
    testSessionsEndpoint();
  } else {
    testHealthEndpoint();
  }

  sleep(Math.random() * 2 + 1);
}

function testKPIEndpoint() {
  const source = Math.random() < 0.7 ? randomChoice(SOURCES) : null;
  const window = randomChoice(WINDOWS);

  const params = {};
  if (source) params.source = source;
  params.window = window;

  const url = `${API_ENDPOINTS.kpi}?${new URLSearchParams(params).toString()}`;
  const response = http.get(url, { headers: { Accept: 'application/json' }, tags: { endpoint: 'kpi' } });

  const success = check(response, {
    'KPI status is 200': (r) => r.status === 200,
    'KPI response time < 150ms': (r) => r.timings.duration < 150,
    'KPI response is JSON array': (r) => {
      try {
        return Array.isArray(JSON.parse(r.body));
      } catch (e) {
        return false;
      }
    },
  });

  errorRate.add(!success);
  latency.add(response.timings.duration);
  requestCount.add(1);
}

function testSeriesEndpoint() {
  const source = Math.random() < 0.8 ? randomChoice(SOURCES) : null;
  const aggregation = randomChoice(AGGREGATIONS);

  const hoursBack = Math.random() * 23 + 1;
  const endTime = new Date();
  const startTime = new Date(endTime.getTime() - hoursBack * 60 * 60 * 1000);

  const params = { from: startTime.toISOString(), to: endTime.toISOString(), aggregation };
  if (source) params.source = source;

  const url = `${API_ENDPOINTS.series}?${new URLSearchParams(params).toString()}`;
  const response = http.get(url, { headers: { Accept: 'application/json' }, tags: { endpoint: 'series' } });

  const success = check(response, {
    'Series status is 200': (r) => r.status === 200,
    'Series response time < 500ms': (r) => r.timings.duration < 500,
    'Series response has expected shape': (r) => {
      try {
        const data = JSON.parse(r.body);
        return Array.isArray(data) && (data.length === 0 || (data[0].source && Array.isArray(data[0].data)));
      } catch (e) {
        return false;
      }
    },
  });

  errorRate.add(!success);
  latency.add(response.timings.duration);
  requestCount.add(1);
}

function testAlertsEndpoint() {
  const params = {};
  if (Math.random() < 0.5) params.since = getRandomTimestamp(48);
  if (Math.random() < 0.3) params.resolved = Math.random() < 0.5 ? 'true' : 'false';
  if (Math.random() < 0.2) params.severity = randomChoice(['info', 'warning', 'critical']);

  const url = Object.keys(params).length
    ? `${API_ENDPOINTS.alerts}?${new URLSearchParams(params).toString()}`
    : API_ENDPOINTS.alerts;

  const response = http.get(url, { headers: { Accept: 'application/json' }, tags: { endpoint: 'alerts' } });

  const success = check(response, {
    'Alerts status is 200': (r) => r.status === 200,
    'Alerts response time < 200ms': (r) => r.timings.duration < 200,
    'Alerts response is JSON array': (r) => {
      try {
        return Array.isArray(JSON.parse(r.body));
      } catch (e) {
        return false;
      }
    },
  });

  errorRate.add(!success);
  latency.add(response.timings.duration);
  requestCount.add(1);
}

function testSessionsEndpoint() {
  const params = {};
  if (Math.random() < 0.6) params.source = randomChoice(SOURCES);
  if (Math.random() < 0.3) params.converted_only = 'true';

  const url = Object.keys(params).length
    ? `${API_ENDPOINTS.sessions}?${new URLSearchParams(params).toString()}`
    : API_ENDPOINTS.sessions;

  const response = http.get(url, { headers: { Accept: 'application/json' }, tags: { endpoint: 'sessions' } });

  const success = check(response, {
    'Sessions status is 200': (r) => r.status === 200,
    'Sessions response time < 300ms': (r) => r.timings.duration < 300,
    'Sessions response is JSON array': (r) => {
      try {
        return Array.isArray(JSON.parse(r.body));
      } catch (e) {
        return false;
      }
    },
  });

  errorRate.add(!success);
  latency.add(response.timings.duration);
  requestCount.add(1);
}

function testHealthEndpoint() {
  const response = http.get(API_ENDPOINTS.health, { headers: { Accept: 'application/json' }, tags: { endpoint: 'health' } });

  const success = check(response, {
    'Health status is 200': (r) => r.status === 200,
    'Health response time < 50ms': (r) => r.timings.duration < 50,
    'Health response has status + services': (r) => {
      try {
        const data = JSON.parse(r.body);
        return Boolean(data.status && data.services);
      } catch (e) {
        return false;
      }
    },
  });

  errorRate.add(!success);
  latency.add(response.timings.duration);
  requestCount.add(1);
}

export function setup() {
  console.log('Starting load test for Signal Intelligence Query API');
  console.log(`Base URL: ${BASE_URL}`);

  const healthResponse = http.get(API_ENDPOINTS.health);
  if (healthResponse.status !== 200) {
    throw new Error(`Health check failed: ${healthResponse.status}`);
  }

  console.log('Health check passed, starting load test...');
  return { timestamp: new Date().toISOString() };
}

export function teardown(data) {
  console.log(`Load test completed at ${new Date().toISOString()}`);
  console.log(`Test started at: ${data.timestamp}`);
}
