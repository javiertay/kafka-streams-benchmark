package benchmark;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StatisticsTest {
    @Test void calculatesNearestRankPercentiles() {
        Latency latency = Statistics.latency(List.of(1_000_000L, 2_000_000L, 3_000_000L, 4_000_000L, 100_000_000L));
        assertEquals(3, latency.p50Ms());
        assertEquals(100, latency.p95Ms());
        assertEquals(100, latency.p99Ms());
        assertEquals(100, latency.maxMs());
    }

    @Test void calculatesMedianForOddAndEvenSamples() {
        assertEquals(2, Statistics.median(List.of(1d, 3d, 2d)));
        assertEquals(2.5, Statistics.median(List.of(1d, 2d, 3d, 4d)));
    }

    @Test void boundsLatencySamplesForLargeDurationRuns() {
        StageMetrics metrics = new StageMetrics(200_000);
        for (int i = 0; i < 200_000; i++) metrics.processed(i);
        assertEquals(100_000, metrics.processing.size());
    }

    @Test void throughputCountsIntervalsBetweenEvents() {
        StageMetrics metrics = new StageMetrics(3);
        metrics.firstProcessNanos.set(1_000_000_000L);
        metrics.lastProcessNanos.set(2_000_000_000L);
        assertEquals(2, metrics.throughput(metrics.firstProcessNanos, metrics.lastProcessNanos, 3));
    }

    @Test void reportsStageElapsedWallTime() {
        StageMetrics metrics = new StageMetrics(10);
        metrics.firstProcessNanos.set(1_000_000_000L);
        metrics.lastProcessNanos.set(3_500_000_000L);
        assertEquals(2.5, metrics.elapsedSeconds(metrics.firstProcessNanos, metrics.lastProcessNanos));
    }
}
