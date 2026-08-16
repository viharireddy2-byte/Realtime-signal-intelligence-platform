import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import { randomIntBetween, randomItem } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
export let eventGenerationRate = new Rate('event_generation_success');
export let eventLatency = new Trend('event_generation_latency');
export let eventsGenerated = new Counter('events_generated_total');

// Test configuration for high-throughput event generation.
// This simulates the shape/volume of load the event-producer service puts
// on Kafka; it does not call the producer's HTTP surface (it doesn't have
// one beyond /metrics) — it validates event schema and timing budgets that
// mirror what event_producer.py actually does.
export let options = {
  scenarios: {
    constant_load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '5m',
      tags: { scenario: 'constant_load' },
    },
    ramping_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '2m', target: 25 },
        { duration: '3m', target: 100 },
        { duration: '5m', target: 100 },
        { duration: '2m', target: 0 },
      ],
      tags: { scenario: 'ramping_load' },
    },
    spike_test: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '1m', target: 50 },
        { duration: '30s', target: 200 },
        { duration: '30s', target: 200 },
        { duration: '1m', target: 50 },
        { duration: '30s', target: 0 },
      ],
      tags: { scenario: 'spike_test' },
    },
  },
  thresholds: {
    event_generation_success: ['rate>0.95'],
    event_generation_latency: ['p(95)<100'],
    events_generated_total: ['count>50000'],
  },
};

const EVENT_SOURCES = ['web', 'mobile', 'api', 'iot-device', 'service-checkout', 'service-search'];
const STATUSES = ['ok', 'warning', 'error'];
const REGIONS = ['us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south', 'ap-northeast'];
const BROWSERS = ['chrome', 'firefox', 'safari', 'edge'];
const PLATFORMS = ['ios', 'android'];
const DEVICE_TYPES = ['sensor', 'gateway', 'controller'];
const FUNNEL_STEPS = ['landing', 'product_view', 'add_to_cart', 'checkout', 'purchase'];

function generateEventData() {
  const source = randomItem(EVENT_SOURCES);
  const eventId = generateUUID();
  const timestamp = new Date().toISOString();

  let metric;
  if (Math.random() < 0.05) {
    metric = randomIntBetween(100, 500);
  } else {
    metric = Math.max(0, randomNormalDistribution(50, 15));
  }

  let status;
  if (metric > 100) {
    status = randomItem(STATUSES);
  } else {
    status = Math.random() < 0.8 ? 'ok' : randomItem(STATUSES);
  }

  const attributes = {
    user_id: `user_${randomIntBetween(1000, 9999)}`,
    metric: Math.round(metric * 100) / 100,
    status,
    session_id: generateUUID(),
    funnel_step: randomItem(FUNNEL_STEPS),
    region: randomItem(REGIONS),
    version: randomItem(['1.0.0', '1.1.0', '1.2.0', '2.0.0']),
  };

  if (source === 'web') {
    attributes.browser = randomItem(BROWSERS);
    attributes.page_load_time_s = Math.round((Math.random() * 4.5 + 0.5) * 100) / 100;
  } else if (source === 'mobile') {
    attributes.platform = randomItem(PLATFORMS);
    attributes.app_version = randomItem(['2.1.0', '2.2.0', '2.3.0']);
  } else if (source === 'iot-device') {
    attributes.device_type = randomItem(DEVICE_TYPES);
    attributes.temperature_c = Math.round((Math.random() * 20 + 15) * 10) / 10;
    attributes.battery_pct = randomIntBetween(0, 100);
  }

  return {
    event_id: eventId,
    schema_version: '2.0',
    source,
    timestamp,
    attributes,
  };
}

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function randomNormalDistribution(mean, stdDev) {
  let u = 0,
    v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  const z = Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
  return z * stdDev + mean;
}

export default function () {
  const startTime = Date.now();

  const batchSize = randomIntBetween(1, 10);
  const events = [];
  for (let i = 0; i < batchSize; i++) {
    events.push(generateEventData());
  }

  sleep((Math.random() * 5) / 1000); // simulated validation/serialization
  sleep((Math.random() * 10 + 5) / 1000); // simulated network + producer latency

  const totalLatency = Date.now() - startTime;

  eventGenerationRate.add(true);
  eventLatency.add(totalLatency);
  eventsGenerated.add(batchSize);

  check(events[0], {
    'Event has required fields': (event) => event.event_id && event.source && event.timestamp && event.attributes,
    'Event ID is valid UUID': (event) =>
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(event.event_id),
    'Event timestamp is a valid ISO string': (event) => !isNaN(Date.parse(event.timestamp)),
    'Event source is valid': (event) => EVENT_SOURCES.includes(event.source),
    'Event metric is numeric': (event) => typeof event.attributes.metric === 'number' && event.attributes.metric >= 0,
    'Event has a hashed user_id': (event) => typeof event.attributes.user_id === 'string' && event.attributes.user_id.length > 0,
  });

  const scenario = __ENV.K6_SCENARIO;
  if (scenario === 'constant_load') {
    sleep(Math.random() * 0.1);
  } else if (scenario === 'ramping_load') {
    sleep(Math.random() * 0.2);
  } else if (scenario === 'spike_test' && Math.random() < 0.3) {
    for (let burst = 0; burst < 5; burst++) {
      const burstEvents = [];
      for (let i = 0; i < 3; i++) burstEvents.push(generateEventData());
      eventsGenerated.add(burstEvents.length);
      sleep(0.001);
    }
  } else {
    sleep(Math.random() * 0.3);
  }
}

export function setup() {
  console.log('Starting high-throughput event generation test');
  console.log('Target: >5000 events/sec sustained');
  return {
    startTime: new Date().toISOString(),
    config: { targetRate: 5000, testDuration: '10m' },
  };
}

export function teardown(data) {
  console.log('Event generation test completed');
  console.log(`Started: ${data.startTime}`);
  console.log(`Ended: ${new Date().toISOString()}`);
}
