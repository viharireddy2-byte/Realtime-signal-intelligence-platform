package com.signalintel.platform.sessions;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * Spark Structured Streaming job that reconstructs user sessions from the
 * raw event stream and computes a simple purchase-funnel rollup per session.
 *
 * <p>This is new relative to the two Flink jobs (aggregation, anomaly
 * detection): those only ever look at individual events. This job groups
 * events into <b>sessions</b> using Spark's native session-window support
 * (a 30 minute inactivity gap closes a session) and asks, per session:
 * how many events, how long did it last, how far into the funnel
 * ({@code landing -> product_view -> add_to_cart -> checkout -> purchase})
 * did the user get, and did they convert?
 *
 * <p>Results are upserted into the {@code sessions} table in TimescaleDB via
 * {@code foreachBatch}, which the query-api's {@code /sessions} endpoint
 * reads from.
 *
 * <p>Session reconstruction is a batch-over-microbatch, many-events-to-one-
 * session workload, which is a natural fit for Spark Structured Streaming's
 * built-in session windows — a deliberately different processing model from
 * the low-latency, per-event Flink jobs above.
 */
public class SessionAnalyticsJob {

    private static final String KAFKA_BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
    private static final String EVENTS_TOPIC =
            System.getenv().getOrDefault("EVENTS_TOPIC", "signal.events.v1");
    private static final String SESSION_GAP =
            System.getenv().getOrDefault("SESSION_GAP", "30 minutes");
    private static final String WATERMARK_DELAY =
            System.getenv().getOrDefault("WATERMARK_DELAY", "10 minutes");
    private static final String CHECKPOINT_LOCATION =
            System.getenv().getOrDefault("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/session-analytics");

    private static final String PG_HOST = System.getenv().getOrDefault("POSTGRES_HOST", "timescaledb");
    private static final String PG_PORT = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
    private static final String PG_DB = System.getenv().getOrDefault("POSTGRES_DB", "signalintel");
    private static final String PG_USER = System.getenv().getOrDefault("POSTGRES_USER", "signalintel_admin");
    private static final String PG_PASSWORD = System.getenv().getOrDefault("POSTGRES_PASSWORD", "password");

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("signal-session-analytics-job")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType attributesSchema = new StructType()
                .add("user_id", DataTypes.StringType)
                .add("session_id", DataTypes.StringType)
                .add("funnel_step", DataTypes.StringType)
                .add("metric", DataTypes.DoubleType)
                .add("status", DataTypes.StringType);

        StructType eventSchema = new StructType()
                .add("event_id", DataTypes.StringType)
                .add("schema_version", DataTypes.StringType)
                .add("source", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType)
                .add("attributes", attributesSchema);

        Dataset<Row> rawEvents = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", EVENTS_TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> parsedEvents = rawEvents
                .selectExpr("CAST(value AS STRING) AS json")
                .select(from_json(col("json"), eventSchema).alias("event"))
                .select(
                        col("event.source").alias("source"),
                        to_timestamp(col("event.timestamp")).alias("event_time"),
                        col("event.attributes.session_id").alias("session_id"),
                        col("event.attributes.funnel_step").alias("funnel_step")
                )
                .filter(col("session_id").isNotNull());

        // Map each funnel step to an ordinal so we can take the max reached per session.
        Dataset<Row> withStepOrder = parsedEvents.withColumn(
                "step_order",
                when(col("funnel_step").equalTo("landing"), lit(1))
                        .when(col("funnel_step").equalTo("product_view"), lit(2))
                        .when(col("funnel_step").equalTo("add_to_cart"), lit(3))
                        .when(col("funnel_step").equalTo("checkout"), lit(4))
                        .when(col("funnel_step").equalTo("purchase"), lit(5))
                        .otherwise(lit(0))
        );

        Dataset<Row> sessionized = withStepOrder
                .withWatermark("event_time", WATERMARK_DELAY)
                .groupBy(
                        session_window(col("event_time"), SESSION_GAP).alias("session"),
                        col("session_id"),
                        col("source")
                )
                .agg(
                        count(lit(1)).alias("event_count"),
                        min(col("event_time")).alias("started_at"),
                        max(col("event_time")).alias("ended_at"),
                        max(col("step_order")).alias("furthest_step_order")
                )
                .withColumn(
                        "furthest_step",
                        when(col("furthest_step_order").equalTo(5), lit("purchase"))
                                .when(col("furthest_step_order").equalTo(4), lit("checkout"))
                                .when(col("furthest_step_order").equalTo(3), lit("add_to_cart"))
                                .when(col("furthest_step_order").equalTo(2), lit("product_view"))
                                .otherwise(lit("landing"))
                )
                .withColumn("converted", col("furthest_step_order").equalTo(5));

        StreamingQuery query = sessionized.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) SessionAnalyticsJob::upsertBatch)
                .outputMode("update")
                .option("checkpointLocation", CHECKPOINT_LOCATION)
                .start();

        query.awaitTermination();
    }

    /**
     * Upserts one micro-batch of session rollups into TimescaleDB.
     * Uses a plain JDBC batch + {@code ON CONFLICT} rather than Spark's JDBC
     * writer because session rows need to be updated in place as a session
     * stays open across multiple micro-batches.
     */
    private static void upsertBatch(Dataset<Row> batch, Long batchId) {
        if (batch.isEmpty()) {
            return;
        }

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", PG_HOST, PG_PORT, PG_DB);
        String upsertSql =
                "INSERT INTO sessions (session_id, source, started_at, ended_at, event_count, furthest_step, converted) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (session_id) DO UPDATE SET " +
                "  ended_at = GREATEST(sessions.ended_at, EXCLUDED.ended_at), " +
                "  event_count = sessions.event_count + EXCLUDED.event_count, " +
                "  furthest_step = EXCLUDED.furthest_step, " +
                "  converted = sessions.converted OR EXCLUDED.converted";

        Properties props = new Properties();
        props.setProperty("user", PG_USER);
        props.setProperty("password", PG_PASSWORD);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             PreparedStatement stmt = conn.prepareStatement(upsertSql)) {

            conn.setAutoCommit(false);
            for (Row row : batch.collectAsList()) {
                stmt.setString(1, row.getAs("session_id"));
                stmt.setString(2, row.getAs("source"));
                stmt.setTimestamp(3, Timestamp.valueOf(row.<Timestamp>getAs("started_at").toLocalDateTime()));
                stmt.setTimestamp(4, Timestamp.valueOf(row.<Timestamp>getAs("ended_at").toLocalDateTime()));
                stmt.setLong(5, row.getAs("event_count"));
                stmt.setString(6, row.getAs("furthest_step"));
                stmt.setBoolean(7, row.getAs("converted"));
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to upsert session batch " + batchId, e);
        }
    }
}
