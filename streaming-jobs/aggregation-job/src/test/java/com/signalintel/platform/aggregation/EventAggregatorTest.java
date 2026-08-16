package com.signalintel.platform.aggregation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link EventAggregationJob.EventAggregator}. Exercises the
 * AggregateFunction directly (createAccumulator/add/merge/getResult) without
 * spinning up a Flink MiniCluster, so these run fast and need no cluster
 * infrastructure.
 */
public class EventAggregatorTest {

    private EventAggregationJob.EventData event(String source, double metric, String status) {
        return new EventAggregationJob.EventData(
                "evt-" + Math.random(), source, System.currentTimeMillis(), metric, status, "user_1"
        );
    }

    @Test
    public void computesCountSumAndAverage() {
        EventAggregationJob.EventAggregator aggregator = new EventAggregationJob.EventAggregator();
        EventAggregationJob.EventAggregator.Accumulator acc = aggregator.createAccumulator();

        acc = aggregator.add(event("web", 10.0, "ok"), acc);
        acc = aggregator.add(event("web", 20.0, "ok"), acc);
        acc = aggregator.add(event("web", 30.0, "ok"), acc);

        EventAggregationJob.AggregateResult result = aggregator.getResult(acc);

        assertEquals(3, result.count);
        assertEquals(60.0, result.sum, 0.0001);
        assertEquals(20.0, result.avg, 0.0001);
        assertEquals("web", result.source);
    }

    @Test
    public void computesErrorRate() {
        EventAggregationJob.EventAggregator aggregator = new EventAggregationJob.EventAggregator();
        EventAggregationJob.EventAggregator.Accumulator acc = aggregator.createAccumulator();

        acc = aggregator.add(event("api", 5.0, "ok"), acc);
        acc = aggregator.add(event("api", 5.0, "error"), acc);
        acc = aggregator.add(event("api", 5.0, "error"), acc);
        acc = aggregator.add(event("api", 5.0, "ok"), acc);

        EventAggregationJob.AggregateResult result = aggregator.getResult(acc);

        assertEquals(2, result.errorCount);
        assertEquals(0.5, result.errorRate, 0.0001);
    }

    @Test
    public void computesP95AndP99WithinRange() {
        EventAggregationJob.EventAggregator aggregator = new EventAggregationJob.EventAggregator();
        EventAggregationJob.EventAggregator.Accumulator acc = aggregator.createAccumulator();

        for (int i = 1; i <= 100; i++) {
            acc = aggregator.add(event("web", i, "ok"), acc);
        }

        EventAggregationJob.AggregateResult result = aggregator.getResult(acc);

        // p95 of 1..100 should land near the top of the distribution.
        assertTrue("p95 should be >= 90, was " + result.p95, result.p95 >= 90);
        assertTrue("p99 should be >= p95, was p99=" + result.p99 + " p95=" + result.p95, result.p99 >= result.p95);
    }

    @Test
    public void mergeCombinesTwoAccumulators() {
        EventAggregationJob.EventAggregator aggregator = new EventAggregationJob.EventAggregator();

        EventAggregationJob.EventAggregator.Accumulator a = aggregator.createAccumulator();
        a = aggregator.add(event("web", 10.0, "ok"), a);

        EventAggregationJob.EventAggregator.Accumulator b = aggregator.createAccumulator();
        b = aggregator.add(event("web", 20.0, "error"), b);

        EventAggregationJob.EventAggregator.Accumulator merged = aggregator.merge(a, b);
        EventAggregationJob.AggregateResult result = aggregator.getResult(merged);

        assertEquals(2, result.count);
        assertEquals(30.0, result.sum, 0.0001);
        assertEquals(1, result.errorCount);
    }

    @Test
    public void emptyAccumulatorProducesZeroedResult() {
        EventAggregationJob.EventAggregator aggregator = new EventAggregationJob.EventAggregator();
        EventAggregationJob.EventAggregator.Accumulator acc = aggregator.createAccumulator();

        EventAggregationJob.AggregateResult result = aggregator.getResult(acc);

        assertEquals(0, result.count);
        assertEquals(0.0, result.avg, 0.0001);
        assertEquals(0.0, result.errorRate, 0.0001);
    }
}
