package com.signalintel.platform.anomaly;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Flink job for real-time anomaly detection.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Consumes events from Kafka topic {@code signal.events.v1}</li>
 *   <li>Maintains rolling statistics per source using Flink keyed state</li>
 *   <li>Scores every event with three complementary detectors:
 *     <ul>
 *       <li><b>Z-score</b> against a rolling window mean/stddev (sensitive to sudden spikes)</li>
 *       <li><b>MAD</b> (median absolute deviation) — more robust to outliers than z-score</li>
 *       <li><b>EWMA</b> (exponentially weighted moving average/variance) — reacts faster to
 *           slow drift than a fixed-size rolling window, since it weights recent points more
 *           heavily without needing to store them.</li>
 *     </ul>
 *   </li>
 *   <li>Emits confirmed anomalies to {@code signal.alerts.v1} for the notifier-service</li>
 *   <li>Emits confirmed anomalies to {@code signal.anomalies.cold} for TimescaleDB persistence</li>
 * </ol>
 */
public class AnomalyDetectionJob {

    private static final String KAFKA_BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
    private static final String INPUT_TOPIC =
            System.getenv().getOrDefault("EVENTS_TOPIC", "signal.events.v1");
    private static final String ALERTS_TOPIC =
            System.getenv().getOrDefault("ALERTS_TOPIC", "signal.alerts.v1");
    private static final String COLD_SINK_TOPIC =
            System.getenv().getOrDefault("ANOMALIES_COLD_TOPIC", "signal.anomalies.cold");
    private static final int PARALLELISM =
            Integer.parseInt(System.getenv().getOrDefault("JOB_PARALLELISM", "2"));

    // Detection parameters
    private static final int ROLLING_WINDOW_SIZE = 100;
    private static final double Z_SCORE_THRESHOLD = 3.0;
    private static final double MAD_THRESHOLD = 3.0;
    private static final double EWMA_THRESHOLD = 3.0;
    private static final double EWMA_ALPHA = 0.1; // smoothing factor: higher = reacts faster
    private static final long MIN_EVENTS_FOR_DETECTION = 10;

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
                .setGroupId("signal-anomaly-detection-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> extractEventTimestamp(event)),
                "kafka-source");

        DataStream<EventData> events = rawEvents
                .map(new EventParser())
                .filter(event -> event != null && event.metric >= 0);

        DataStream<AnomalyAlert> anomalies = events
                .keyBy(event -> event.source)
                .flatMap(new AnomalyDetector());

        DataStream<AnomalyAlert> confirmedAnomalies = anomalies
                .filter(alert -> alert != null && alert.isAnomaly);

        DataStream<String> alertMessages = confirmedAnomalies.map(new AlertMessageFormatter());
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ALERTS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        alertMessages.sinkTo(alertSink).name("alert-sink");

        DataStream<String> coldMessages = confirmedAnomalies.map(new ColdSinkMessageFormatter());
        KafkaSink<String> coldSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(COLD_SINK_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        coldMessages.sinkTo(coldSink).name("cold-sink");

        env.execute("signal-anomaly-detection-job");
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
    }

    public static class AnomalyAlert {
        public String alertId;
        public String source;
        public long timestamp;
        public String anomalyType;
        public String severity;
        public double value;
        public double threshold;
        public double zScore;
        public double madScore;
        public double ewmaScore;
        public String description;
        public boolean isAnomaly;
        public RollingStats stats;
    }

    /** Rolling window statistics (mean/stddev/median/MAD) plus an EWMA tracker. */
    public static class RollingStats {
        public Queue<Double> values = new ArrayDeque<>();
        public double sum;
        public double sumSquares;
        public int maxSize;

        // EWMA state
        public double ewmaMean;
        public double ewmaVariance;
        public boolean ewmaInitialized;

        /** No-arg constructor required for Flink POJO state serialization. */
        public RollingStats() {
        }

        public RollingStats(int maxSize) {
            this.maxSize = maxSize;
        }

        public void addValue(double value) {
            if (values.size() >= maxSize) {
                double removed = values.poll();
                sum -= removed;
                sumSquares -= removed * removed;
            }
            values.offer(value);
            sum += value;
            sumSquares += value * value;

            updateEwma(value);
        }

        private void updateEwma(double value) {
            if (!ewmaInitialized) {
                ewmaMean = value;
                ewmaVariance = 0.0;
                ewmaInitialized = true;
                return;
            }
            double delta = value - ewmaMean;
            ewmaMean += EWMA_ALPHA * delta;
            // EWMA of squared deviation approximates a decaying variance estimate
            ewmaVariance = (1 - EWMA_ALPHA) * (ewmaVariance + EWMA_ALPHA * delta * delta);
        }

        public double getEwmaStdDev() {
            return Math.sqrt(Math.max(0, ewmaVariance));
        }

        public double getMean() {
            return values.isEmpty() ? 0.0 : sum / values.size();
        }

        public double getStandardDeviation() {
            if (values.size() < 2) return 0.0;
            double mean = getMean();
            double variance = (sumSquares / values.size()) - (mean * mean);
            return Math.sqrt(Math.max(0, variance));
        }

        public double getMedian() {
            if (values.isEmpty()) return 0.0;
            Double[] sorted = values.toArray(new Double[0]);
            java.util.Arrays.sort(sorted);
            int size = sorted.length;
            return size % 2 == 0
                    ? (sorted[size / 2 - 1] + sorted[size / 2]) / 2.0
                    : sorted[size / 2];
        }

        public double getMAD() {
            if (values.isEmpty()) return 0.0;
            double median = getMedian();
            Double[] deviations = new Double[values.size()];
            int i = 0;
            for (Double value : values) {
                deviations[i++] = Math.abs(value - median);
            }
            java.util.Arrays.sort(deviations);
            int size = deviations.length;
            return size % 2 == 0
                    ? (deviations[size / 2 - 1] + deviations[size / 2]) / 2.0
                    : deviations[size / 2];
        }

        public int size() {
            return values.size();
        }
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

    public static class AnomalyDetector extends RichFlatMapFunction<EventData, AnomalyAlert> {

        private ValueState<RollingStats> statsState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<RollingStats> descriptor = new ValueStateDescriptor<>(
                    "rolling-stats",
                    TypeInformation.of(new TypeHint<RollingStats>() {})
            );
            statsState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(EventData event, Collector<AnomalyAlert> out) throws Exception {
            RollingStats stats = statsState.value();
            if (stats == null) {
                stats = new RollingStats(ROLLING_WINDOW_SIZE);
            }

            stats.addValue(event.metric);
            statsState.update(stats);

            if (stats.size() < MIN_EVENTS_FOR_DETECTION) {
                return;
            }

            double mean = stats.getMean();
            double stdDev = stats.getStandardDeviation();
            double zScore = stdDev > 0 ? (event.metric - mean) / stdDev : 0.0;

            double median = stats.getMedian();
            double mad = stats.getMAD();
            double madScore = mad > 0 ? Math.abs(event.metric - median) / mad : 0.0;

            double ewmaStdDev = stats.getEwmaStdDev();
            double ewmaScore = ewmaStdDev > 0 ? Math.abs(event.metric - stats.ewmaMean) / ewmaStdDev : 0.0;

            boolean isZScoreAnomaly = Math.abs(zScore) > Z_SCORE_THRESHOLD;
            boolean isMadAnomaly = madScore > MAD_THRESHOLD;
            boolean isEwmaAnomaly = ewmaScore > EWMA_THRESHOLD;
            boolean isAnomaly = isZScoreAnomaly || isMadAnomaly || isEwmaAnomaly;

            double maxScore = Math.max(Math.abs(zScore), Math.max(madScore, ewmaScore));
            String severity = "info";
            if (isAnomaly) {
                if (maxScore > 4.5) {
                    severity = "critical";
                } else if (maxScore > 3.5) {
                    severity = "warning";
                } else {
                    severity = "info";
                }
            }

            String anomalyType;
            if (isZScoreAnomaly) {
                anomalyType = "z-score";
            } else if (isMadAnomaly) {
                anomalyType = "mad";
            } else if (isEwmaAnomaly) {
                anomalyType = "ewma";
            } else {
                anomalyType = "normal";
            }

            AnomalyAlert alert = new AnomalyAlert();
            alert.alertId = java.util.UUID.randomUUID().toString();
            alert.source = event.source;
            alert.timestamp = event.timestamp;
            alert.anomalyType = anomalyType;
            alert.severity = severity;
            alert.value = event.metric;
            alert.threshold = isZScoreAnomaly ? Z_SCORE_THRESHOLD : (isMadAnomaly ? MAD_THRESHOLD : EWMA_THRESHOLD);
            alert.zScore = zScore;
            alert.madScore = madScore;
            alert.ewmaScore = ewmaScore;
            alert.isAnomaly = isAnomaly;
            alert.stats = stats;

            if (isAnomaly) {
                alert.description = String.format(
                        "Anomaly in %s: value=%.2f mean=%.2f z=%.2f mad=%.2f ewma=%.2f (detector=%s)",
                        event.source, event.metric, mean, zScore, madScore, ewmaScore, anomalyType
                );
            } else {
                alert.description = String.format(
                        "Normal value in %s: value=%.2f mean=%.2f z=%.2f",
                        event.source, event.metric, mean, zScore
                );
            }

            out.collect(alert);
        }
    }

    public static class AlertMessageFormatter implements MapFunction<AnomalyAlert, String> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public String map(AnomalyAlert alert) throws Exception {
            ObjectNode message = mapper.createObjectNode();
            message.put("alert_id", alert.alertId);
            message.put("source", alert.source);
            message.put("timestamp", Instant.ofEpochMilli(alert.timestamp).toString());
            message.put("anomaly_type", alert.anomalyType);
            message.put("severity", alert.severity);
            message.put("value", alert.value);
            message.put("threshold", alert.threshold);
            message.put("z_score", alert.zScore);
            message.put("mad_score", alert.madScore);
            message.put("ewma_score", alert.ewmaScore);
            message.put("description", alert.description);
            message.put("is_anomaly", alert.isAnomaly);

            if (alert.stats != null) {
                ObjectNode stats = mapper.createObjectNode();
                stats.put("mean", alert.stats.getMean());
                stats.put("std_dev", alert.stats.getStandardDeviation());
                stats.put("median", alert.stats.getMedian());
                stats.put("mad", alert.stats.getMAD());
                stats.put("ewma_mean", alert.stats.ewmaMean);
                stats.put("ewma_std_dev", alert.stats.getEwmaStdDev());
                stats.put("sample_size", alert.stats.size());
                message.set("stats", stats);
            }

            return mapper.writeValueAsString(message);
        }
    }

    public static class ColdSinkMessageFormatter implements MapFunction<AnomalyAlert, String> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public String map(AnomalyAlert alert) throws Exception {
            ObjectNode message = mapper.createObjectNode();
            message.put("type", "anomaly_insert");

            ObjectNode data = mapper.createObjectNode();
            data.put("ts", Instant.ofEpochMilli(alert.timestamp).toString());
            data.put("source", alert.source);
            data.put("anomaly_type", alert.anomalyType);
            data.put("severity", alert.severity);
            data.put("value", alert.value);
            data.put("threshold", alert.threshold);
            data.put("z_score", alert.zScore);
            data.put("description", alert.description);
            data.put("resolved", false);

            message.set("data", data);
            return mapper.writeValueAsString(message);
        }
    }
}
