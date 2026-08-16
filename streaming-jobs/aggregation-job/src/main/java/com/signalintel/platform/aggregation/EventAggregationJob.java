package com.signalintel.platform.aggregation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Flink job for real-time event aggregation.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Consumes events from Kafka topic {@code signal.events.v1}</li>
 *   <li>Performs sliding-window aggregation (1 minute window, sliding every 10s) keyed by source</li>
 *   <li>Computes count, avg, p95, p99 and error-rate per source</li>
 *   <li>Emits aggregates to {@code signal.aggregates.hot} for the query-api's Redis hot path</li>
 *   <li>Emits aggregates to {@code signal.aggregates.cold} for TimescaleDB persistence</li>
 * </ol>
 *
 * <p>Configuration is read from environment variables so the same fat JAR runs
 * unmodified in docker-compose, Flink Session clusters, and Kubernetes/Helm.
 */
public class EventAggregationJob {

    private static final String KAFKA_BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
    private static final String INPUT_TOPIC =
            System.getenv().getOrDefault("EVENTS_TOPIC", "signal.events.v1");
    private static final String HOT_SINK_TOPIC =
            System.getenv().getOrDefault("AGGREGATES_HOT_TOPIC", "signal.aggregates.hot");
    private static final String COLD_SINK_TOPIC =
            System.getenv().getOrDefault("AGGREGATES_COLD_TOPIC", "signal.aggregates.cold");
    private static final int PARALLELISM =
            Integer.parseInt(System.getenv().getOrDefault("JOB_PARALLELISM", "2"));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(PARALLELISM);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId("signal-aggregation-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> extractEventTimestamp(event)),
                "kafka-source");

        DataStream<EventData> events = rawEvents
                .map(new EventParser())
                .filter(event -> event != null);

        DataStream<AggregateResult> aggregates = events
                .keyBy(event -> event.source)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .aggregate(new EventAggregator());

        DataStream<String> hotMessages = aggregates.map(new HotSinkMessageFormatter());
        KafkaSink<String> hotSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(HOT_SINK_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        hotMessages.sinkTo(hotSink).name("hot-aggregate-sink");

        DataStream<String> coldMessages = aggregates.map(new ColdSinkMessageFormatter());
        KafkaSink<String> coldSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(COLD_SINK_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        coldMessages.sinkTo(coldSink).name("cold-aggregate-sink");

        env.execute("signal-event-aggregation-job");
    }

    private static long extractEventTimestamp(String eventJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(eventJson);
            return Instant.parse(node.get("timestamp").asText()).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    /** Flattened event representation used inside the job. */
    public static class EventData {
        public String eventId;
        public String source;
        public long timestamp;
        public double metric;
        public String status;
        public String userId;

        public EventData() {}

        public EventData(String eventId, String source, long timestamp,
                          double metric, String status, String userId) {
            this.eventId = eventId;
            this.source = source;
            this.timestamp = timestamp;
            this.metric = metric;
            this.status = status;
            this.userId = userId;
        }

        public boolean isError() {
            return "error".equals(status);
        }
    }

    /** Result of a single windowed aggregation for one source. */
    public static class AggregateResult {
        public String source;
        public long windowStart;
        public long windowEnd;
        public long count;
        public double sum;
        public double avg;
        public double p95;
        public double p99;
        public long errorCount;
        public double errorRate;
    }

    public static class EventParser implements MapFunction<String, EventData> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public EventData map(String value) {
            try {
                JsonNode node = mapper.readTree(value);
                JsonNode attributes = node.get("attributes");
                return new EventData(
                        node.get("event_id").asText(),
                        node.get("source").asText(),
                        Instant.parse(node.get("timestamp").asText()).toEpochMilli(),
                        attributes.get("metric").asDouble(),
                        attributes.get("status").asText(),
                        attributes.get("user_id").asText()
                );
            } catch (Exception e) {
                System.err.println("Failed to parse event: " + value + ", error: " + e.getMessage());
                return null;
            }
        }
    }

    public static class EventAggregator
            implements AggregateFunction<EventData, EventAggregator.Accumulator, AggregateResult> {

        public static class Accumulator {
            public String source;
            public long windowStart;
            public long windowEnd;
            public long count;
            public double sum;
            public List<Double> metrics = new ArrayList<>();
            public long errorCount;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(EventData event, Accumulator accumulator) {
            if (accumulator.source == null) {
                accumulator.source = event.source;
            }
            accumulator.count++;
            accumulator.sum += event.metric;
            accumulator.metrics.add(event.metric);
            if (event.isError()) {
                accumulator.errorCount++;
            }
            return accumulator;
        }

        @Override
        public AggregateResult getResult(Accumulator accumulator) {
            AggregateResult result = new AggregateResult();
            result.source = accumulator.source;
            result.windowStart = accumulator.windowStart;
            result.windowEnd = accumulator.windowEnd;
            result.count = accumulator.count;
            result.sum = accumulator.sum;
            result.avg = accumulator.count > 0 ? accumulator.sum / accumulator.count : 0.0;
            result.errorCount = accumulator.errorCount;
            result.errorRate = accumulator.count > 0 ? (double) accumulator.errorCount / accumulator.count : 0.0;

            if (!accumulator.metrics.isEmpty()) {
                Collections.sort(accumulator.metrics);
                int size = accumulator.metrics.size();
                result.p95 = accumulator.metrics.get(Math.min(size - 1, (int) (size * 0.95)));
                result.p99 = accumulator.metrics.get(Math.min(size - 1, (int) (size * 0.99)));
            }

            return result;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            a.metrics.addAll(b.metrics);
            a.errorCount += b.errorCount;
            return a;
        }
    }

    /** Formats aggregates for the query-api hot path (Redis). */
    public static class HotSinkMessageFormatter implements MapFunction<AggregateResult, String> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public String map(AggregateResult aggregate) throws Exception {
            ObjectNode message = mapper.createObjectNode();
            message.put("type", "hot_aggregate");

            // Redis key format: sip:agg:{source}:1m:{window-start-iso}
            String redisKey = String.format("sip:agg:%s:1m:%s",
                    aggregate.source,
                    Instant.ofEpochMilli(aggregate.windowStart).toString());

            ObjectNode payload = mapper.createObjectNode();
            payload.put("count", aggregate.count);
            payload.put("avg_metric", aggregate.avg);
            payload.put("p95_metric", aggregate.p95);
            payload.put("p99_metric", aggregate.p99);
            payload.put("error_rate", aggregate.errorRate);
            payload.put("sum_metric", aggregate.sum);

            message.put("key", redisKey);
            message.set("value", payload);
            message.put("ttl", 3600);

            return mapper.writeValueAsString(message);
        }
    }

    /** Formats aggregates for TimescaleDB persistence. */
    public static class ColdSinkMessageFormatter implements MapFunction<AggregateResult, String> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public String map(AggregateResult aggregate) throws Exception {
            ObjectNode message = mapper.createObjectNode();
            message.put("type", "cold_aggregate");

            ObjectNode payload = mapper.createObjectNode();
            payload.put("ts", Instant.ofEpochMilli(aggregate.windowStart).toString());
            payload.put("source", aggregate.source);
            payload.put("count_events", aggregate.count);
            payload.put("avg_metric", aggregate.avg);
            payload.put("p95_metric", aggregate.p95);
            payload.put("p99_metric", aggregate.p99);
            payload.put("error_rate", aggregate.errorRate);
            payload.put("sum_metric", aggregate.sum);

            message.set("data", payload);
            return mapper.writeValueAsString(message);
        }
    }
}
