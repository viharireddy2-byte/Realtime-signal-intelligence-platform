package com.signalintel.platform.anomaly;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AnomalyDetectionJob.RollingStats}: the rolling
 * mean/stddev/median/MAD/EWMA tracker that backs all three anomaly
 * detectors. Pure in-memory logic, no Flink runtime required.
 */
public class RollingStatsTest {

    @Test
    public void meanAndStdDevMatchKnownValues() {
        AnomalyDetectionJob.RollingStats stats = new AnomalyDetectionJob.RollingStats(100);
        for (double v : new double[]{2, 4, 4, 4, 5, 5, 7, 9}) {
            stats.addValue(v);
        }

        assertEquals(5.0, stats.getMean(), 0.0001);
        assertEquals(2.0, stats.getStandardDeviation(), 0.0001); // population stddev of this set is exactly 2
    }

    @Test
    public void medianHandlesEvenAndOddCounts() {
        AnomalyDetectionJob.RollingStats odd = new AnomalyDetectionJob.RollingStats(100);
        for (double v : new double[]{1, 3, 2}) odd.addValue(v);
        assertEquals(2.0, odd.getMedian(), 0.0001);

        AnomalyDetectionJob.RollingStats even = new AnomalyDetectionJob.RollingStats(100);
        for (double v : new double[]{1, 2, 3, 4}) even.addValue(v);
        assertEquals(2.5, even.getMedian(), 0.0001);
    }

    @Test
    public void windowEvictsOldestValueWhenFull() {
        AnomalyDetectionJob.RollingStats stats = new AnomalyDetectionJob.RollingStats(3);
        stats.addValue(10);
        stats.addValue(10);
        stats.addValue(10);
        assertEquals(3, stats.size());
        assertEquals(10.0, stats.getMean(), 0.0001);

        // Pushes the window to [10, 10, 100] — the first 10 should have been evicted.
        stats.addValue(100);
        assertEquals(3, stats.size());
        assertEquals(40.0, stats.getMean(), 0.0001);
    }

    @Test
    public void madIsRobustToASingleOutlier() {
        AnomalyDetectionJob.RollingStats stats = new AnomalyDetectionJob.RollingStats(100);
        for (double v : new double[]{50, 51, 49, 50, 52, 48, 500}) {
            stats.addValue(v);
        }

        // The MAD-based median should sit near the cluster of normal values,
        // not be dragged toward the outlier the way a mean would be.
        assertTrue("median should stay close to ~50, was " + stats.getMedian(), stats.getMedian() < 55);
    }

    @Test
    public void ewmaTracksRecentValuesMoreThanOldOnes() {
        AnomalyDetectionJob.RollingStats stats = new AnomalyDetectionJob.RollingStats(200);
        for (int i = 0; i < 50; i++) {
            stats.addValue(50.0);
        }
        double ewmaAfterStableRun = stats.ewmaMean;
        assertEquals(50.0, ewmaAfterStableRun, 0.5);

        // A sustained shift should pull the EWMA mean toward the new level
        // faster than the plain rolling mean (which is still anchored by
        // the first 50 stable values).
        for (int i = 0; i < 20; i++) {
            stats.addValue(80.0);
        }

        assertTrue("EWMA mean should have moved toward 80, was " + stats.ewmaMean, stats.ewmaMean > 60);
        assertTrue(
                "EWMA should react faster than the plain rolling mean here",
                stats.ewmaMean > stats.getMean()
        );
    }

    @Test
    public void emptyStatsReturnZeroesNotExceptions() {
        AnomalyDetectionJob.RollingStats stats = new AnomalyDetectionJob.RollingStats(10);
        assertEquals(0.0, stats.getMean(), 0.0001);
        assertEquals(0.0, stats.getStandardDeviation(), 0.0001);
        assertEquals(0.0, stats.getMedian(), 0.0001);
        assertEquals(0.0, stats.getMAD(), 0.0001);
        assertEquals(0.0, stats.getEwmaStdDev(), 0.0001);
    }
}
